import { createHash } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { ConsolidationPending, ConsolidationRequestBlocked, ConsolidationRequests, requestInputBytes } from '../src/functions/consolidation-partitions.js'
import { newRoot, pageKey } from '../src/functions/consolidation-manifest.js'
import { reductionCut } from '../src/functions/consolidation-reduction.js'
import { buildSemanticCandidatePrompt, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM,
  SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY } from '../src/prompts/consolidation.js'
import { KV, fingerprintId } from '../src/state/schema.js'

type Candidate = {
  id: string
  key: string
  fact: string
  confidence: number
  sourceSessionIds: string[]
  children: string[]
  legacyCandidateIds?: string[]
}

function clone<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T }

function createKV(initial: Array<{ scope: string; key: string; value: unknown }> = []) {
  const values = new Map(initial.map(item => [`${item.scope}\0${item.key}`, clone(item.value)]))
  const key = (scope: string, id: string) => `${scope}\0${id}`
  const get = vi.fn(async (scope: string, id: string) => values.has(key(scope, id)) ? clone(values.get(key(scope, id))) : null)
  const set = vi.fn(async (scope: string, id: string, value: unknown) => {
    const stored = clone(value); values.set(key(scope, id), stored); return clone(stored)
  })
  return { kv: { get, set }, get, set, values, key }
}

function candidates(count: number, factLength = 18): Candidate[] {
  return Array.from({ length: count }, (_, index) => {
    const id = `candidate-${index + 1}`
    const fact = `Synthetic claim ${index + 1} ${'x'.repeat(factLength)}`
    return { id, key: fact.toLowerCase(), fact, confidence: 0.8, sourceSessionIds: [`session-${index + 1}`], children: [], legacyCandidateIds: [id] }
  })
}

function candidatePrompt(input: Candidate[]): string {
  return buildSemanticCandidatePrompt(input.map(candidate => ({ fact: candidate.fact, confidence: candidate.confidence,
    sourceSessionIds: candidate.sourceSessionIds, candidateIds: candidate.legacyCandidateIds ?? [candidate.id] })))
}

function candidateIdsFromRequest(prompt: string): string[] {
  const marker = 'Candidate facts with source evidence:\n\n'
  const offset = prompt.indexOf(marker) + marker.length
  if (offset < marker.length) throw new Error('synthetic reducer prompt is malformed')
  const input = JSON.parse(prompt.slice(offset)) as Array<{ candidateIds: string[] }>
  return [...new Set(input.flatMap(candidate => candidate.candidateIds))]
}

function factResponse(groups: string[][]): string {
  return `<facts>${groups.map((ids, index) => `<fact confidence="0.9" candidateIds="${ids.join(',')}">Synthetic merged claim ${index + 1}</fact>`).join('')}</facts>`
}

function rootWithCandidates(id: string, input: Candidate[], round = 0) {
  const sessions = [...new Set(input.flatMap(candidate => candidate.sourceSessionIds))]
  const root = newRoot(id, 'semantic', 'synthetic-project', sessions.map(sessionId => ({ id: `summary-${sessionId}`, sessionId,
    scope: KV.summaries, kind: 'summary' as const, project: 'synthetic-project', rowVersion: '1' })))
  root.phase = 'reduce'
  root.candidatePages = 1
  root.candidateCount = input.length
  root.finalRound = round
  root.finalPages = round > 0 ? 1 : 0
  root.reduction = { round, inputPages: 1, inputCount: input.length, page: 0, offset: 0, outputPages: 0, outputCount: 0 }
  return root
}

async function seedReductionPage(fixture: ReturnType<typeof createKV>, root: ReturnType<typeof rootWithCandidates>, input: Candidate[], round = 0) {
  for (const sessionId of new Set(input.flatMap(candidate => candidate.sourceSessionIds))) {
    await fixture.set(KV.sessions, sessionId, { project: root.project })
  }
  const id = pageKey(root.id, round === 0 ? 'candidates' : `reduction:${round}`, 0)
  await fixture.set(KV.state, id, { id, candidates: input, examined: input.length, rejected: 0 })
}

function requestsFor(fixture: ReturnType<typeof createKV>, manifestId: string, provider = vi.fn(async () => '')) {
  const requests = new ConsolidationRequests(fixture.kv as never, manifestId, provider, undefined, { used: 0, maximum: 4 })
  return { requests, provider }
}

function resumedRootAndCandidates() {
  const input = candidates(3)
  const root = rootWithCandidates('legacy-global-reduction-resume', input, 1)
  root.finalRound = 1
  root.finalPages = 1
  return { root, input }
}

describe('global semantic reduction completion', () => {
  it('verifies a single reduction page after the 11-to-3 global pass without opening another round', async () => {
    const input = candidates(11)
    const root = rootWithCandidates('single-global-reduction', input)
    const fixture = createKV()
    await seedReductionPage(fixture, root, input)
    const provider = vi.fn(async (_system: string, prompt: string) => {
      const ids = candidateIdsFromRequest(prompt)
      return factResponse([ids.slice(0, 4), ids.slice(4, 8), ids.slice(8)])
    })
    const requests = new ConsolidationRequests(fixture.kv as never, root.id, provider, undefined, { used: 0, maximum: 2 })

    await reductionCut(fixture.kv as never, root as never, requests)
    expect(provider).toHaveBeenCalledTimes(1)
    expect(root.reduction).toMatchObject({ round: 0, page: 1, outputPages: 1, outputCount: 3 })
    await reductionCut(fixture.kv as never, root as never, requests)

    expect(provider).toHaveBeenCalledTimes(1)
    expect(root).toMatchObject({ phase: 'verify', finalRound: 1, finalPages: 1 })
    expect(root.reduction.round).toBe(0)
    const output = await fixture.kv.get(KV.state, pageKey(root.id, 'reduction:1', 0)) as { candidates: Candidate[] }
    expect(output.candidates).toHaveLength(3)
    expect(output.candidates.flatMap(candidate => candidate.legacyCandidateIds ?? []).sort())
      .toEqual(input.flatMap(candidate => candidate.legacyCandidateIds ?? []).sort())
  })

  it('keeps multi-group passes and merges their outputs in the next round', async () => {
    const input = candidates(8, 2000)
    expect(requestInputBytes(SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, candidatePrompt(input))).toBeGreaterThan(7800)
    expect(requestInputBytes(SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, candidatePrompt(input.slice(0, 2)))).toBeLessThanOrEqual(7800)
    const root = rootWithCandidates('multi-group-reduction', input)
    const fixture = createKV()
    await seedReductionPage(fixture, root, input)
    const provider = vi.fn(async (_system: string, prompt: string) => factResponse([candidateIdsFromRequest(prompt)]))
    const requests = new ConsolidationRequests(fixture.kv as never, root.id, provider, undefined, { used: 0, maximum: 8 })

    for (let cut = 0; root.reduction.page < root.reduction.inputPages; cut++) {
      if (cut > 8) throw new Error('synthetic multi-group reduction did not advance')
      await reductionCut(fixture.kv as never, root as never, requests)
    }
    const firstPassGroups = root.reduction.outputPages
    expect(firstPassGroups).toBeGreaterThan(1)
    expect(provider.mock.calls.length).toBe(firstPassGroups)
    await reductionCut(fixture.kv as never, root as never, requests)
    expect(root.reduction).toMatchObject({ round: 1, inputPages: firstPassGroups, inputCount: firstPassGroups, page: 0 })

    await reductionCut(fixture.kv as never, root as never, requests)
    expect(provider).toHaveBeenCalledTimes(firstPassGroups + 1)
    await reductionCut(fixture.kv as never, root as never, requests)

    expect(root).toMatchObject({ phase: 'verify', finalRound: 2, finalPages: 1 })
    expect(provider).toHaveBeenCalledTimes(firstPassGroups + 1)
  })

  it('verifies an untouched legacy next round only after exact current and compatible request point reads miss', async () => {
    const { root, input } = resumedRootAndCandidates()
    const fixture = createKV()
    await seedReductionPage(fixture, root, input, 1)
    const { requests, provider } = requestsFor(fixture, root.id)
    const currentId = fingerprintId('conrequest', JSON.stringify([root.id, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM,
      `Consolidation manifest: ${root.id}\n\n${candidatePrompt(input)}`]))
    const legacyId = fingerprintId('conrequest', JSON.stringify([root.id, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY,
      `Consolidation manifest: ${root.id}\n\n${candidatePrompt(input)}`]))

    await reductionCut(fixture.kv as never, root as never, requests)

    expect(provider).not.toHaveBeenCalled()
    expect(root).toMatchObject({ phase: 'verify', finalRound: 1, finalPages: 1 })
    expect(root.reduction).toMatchObject({ round: 1, page: 0, outputPages: 0, outputCount: 0 })
    expect(fixture.get).toHaveBeenCalledWith(KV.state, currentId)
    expect(fixture.get).toHaveBeenCalledWith(KV.state, legacyId)
  })

  it('recovers an exact captured next-round request instead of bypassing it', async () => {
    const { root, input } = resumedRootAndCandidates()
    const fixture = createKV()
    await seedReductionPage(fixture, root, input, 1)
    const userPrompt = `Consolidation manifest: ${root.id}\n\n${candidatePrompt(input)}`
    const response = factResponse([input.flatMap(candidate => candidate.legacyCandidateIds ?? [candidate.id])])
    const requestId = fingerprintId('conrequest', JSON.stringify([root.id, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, userPrompt]))
    const responsePage = { count: 1, sha256: createHash('sha256').update(response).digest('hex'), bytes: Buffer.byteLength(response), index: 0, text: response }
    const request = { id: requestId, manifestId: root.id, systemPrompt: SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, userPrompt,
      status: 'completed', responsePages: { count: responsePage.count, sha256: responsePage.sha256, bytes: responsePage.bytes }, createdAt: '2026-10-11T00:00:00.000Z' }
    const responseKey = pageKey(root.id, `response:${requestId}`, 0)
    await fixture.set(KV.state, requestId, request)
    await fixture.set(KV.state, responseKey, responsePage)
    const { requests, provider } = requestsFor(fixture, root.id)

    await reductionCut(fixture.kv as never, root as never, requests)
    expect(provider).not.toHaveBeenCalled()
    expect(root.reduction).toMatchObject({ round: 1, page: 1, outputPages: 1, outputCount: 1 })
    expect(fixture.values.get(fixture.key(KV.state, requestId))).toEqual(request)
    expect(fixture.values.get(fixture.key(KV.state, responseKey))).toEqual(responsePage)
  })

  it('preserves pending queued and uncertain direct next-round requests', async () => {
    for (const mode of ['queued', 'uncertain'] as const) {
      const { root, input } = resumedRootAndCandidates()
      const fixture = createKV()
      await seedReductionPage(fixture, root, input, 1)
      const userPrompt = `Consolidation manifest: ${root.id}\n\n${candidatePrompt(input)}`
      const requestId = fingerprintId('conrequest', JSON.stringify([root.id, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, userPrompt]))
      const provider = vi.fn(async () => factResponse([input.flatMap(candidate => candidate.legacyCandidateIds ?? [candidate.id])]))
      const queue = { enqueue: vi.fn(async () => ({ queued: true, workItemId: 'unexpected' })) }
      const requests = new ConsolidationRequests(fixture.kv as never, root.id, provider, queue as never, { used: 0, maximum: 2 })
      if (mode === 'queued') {
        const workItemId = 'existing-queued-request'
        await fixture.set(KV.state, requestId, { id: requestId, manifestId: root.id, systemPrompt: SEMANTIC_CANDIDATE_REDUCTION_SYSTEM,
          userPrompt, status: 'pending', dispatchMode: 'queue', workItemId, createdAt: '2026-10-11T00:00:00.000Z' })
        await fixture.set(KV.fireworksBatchWorkItems, workItemId, { id: workItemId, task: 'consolidation', correlationId: requestId,
          customId: requestId, systemPrompt: SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, userPrompt,
          metadata: { consolidationManifestId: root.id, consolidationPartitionId: requestId, sourceFingerprint: requestId }, state: 'queued' })
        await expect(reductionCut(fixture.kv as never, root as never, requests)).rejects.toBeInstanceOf(ConsolidationPending)
        expect(queue.enqueue).not.toHaveBeenCalled()
      } else {
        await fixture.set(KV.state, requestId, { id: requestId, manifestId: root.id, systemPrompt: SEMANTIC_CANDIDATE_REDUCTION_SYSTEM,
          userPrompt, status: 'pending', dispatchMode: 'direct', providerAttempt: { state: 'started', attempt: 1 },
          createdAt: '2026-10-11T00:00:00.000Z' })
        await expect(reductionCut(fixture.kv as never, root as never, requests)).rejects.toBeInstanceOf(ConsolidationRequestBlocked)
      }
      expect(provider).not.toHaveBeenCalled()
      expect(root.phase).toBe('reduce')
    }
  })
})
