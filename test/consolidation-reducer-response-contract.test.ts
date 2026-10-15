import { createHash } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { parseProcedural, parseReduced, parseSemantic, reductionCut } from '../src/functions/consolidation-reduction.js'
import { ConsolidationRequests } from '../src/functions/consolidation-partitions.js'
import { newRoot, pageKey } from '../src/functions/consolidation-manifest.js'
import { buildSemanticCandidatePrompt, SEMANTIC_CANDIDATE_FINAL_SYSTEM, SEMANTIC_CANDIDATE_FINAL_SYSTEM_LEGACY,
  SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY } from '../src/prompts/consolidation.js'
import { KV, fingerprintId } from '../src/state/schema.js'

type Candidate = {
  id: string
  key: string
  fact: string
  confidence: number
  sourceSessionIds: string[]
  children: string[]
  legacyCandidateIds: string[]
}

type StoredValues = Map<string, unknown>

function wireClone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T
}

function createKV(initial: Array<{ scope: string; key: string; value: unknown }> = []) {
  const values: StoredValues = new Map(initial.map(item => [`${item.scope}\0${item.key}`, wireClone(item.value)]))
  const key = (scope: string, id: string) => `${scope}\0${id}`
  const get = vi.fn(async (scope: string, id: string) => values.has(key(scope, id)) ? wireClone(values.get(key(scope, id))) : null)
  const set = vi.fn(async (scope: string, id: string, value: unknown) => {
    const stored = wireClone(value)
    values.set(key(scope, id), stored)
    return wireClone(stored)
  })
  return { kv: { get, set }, get, set, values, key }
}

function sanitizedLegacyFixture() {
  const manifestId = 'sanitized-reducer-manifest'
  const candidates: Candidate[] = [
    { id: 'candidate-1', key: 'first synthetic claim', fact: 'First synthetic claim', confidence: 0.8, sourceSessionIds: ['session-a'], children: [], legacyCandidateIds: ['candidate-1'] },
    { id: 'candidate-2', key: 'second synthetic claim', fact: 'Second synthetic claim', confidence: 0.7, sourceSessionIds: ['session-b'], children: [], legacyCandidateIds: ['candidate-2'] },
    { id: 'candidate-3', key: 'third synthetic claim', fact: 'Third synthetic claim', confidence: 0.6, sourceSessionIds: ['session-c'], children: [], legacyCandidateIds: ['candidate-3'] },
  ]
  const input = candidates.map(candidate => ({ fact: candidate.fact, confidence: candidate.confidence,
    sourceSessionIds: candidate.sourceSessionIds, candidateIds: candidate.legacyCandidateIds }))
  const userPrompt = `Consolidation manifest: ${manifestId}\n\n${buildSemanticCandidatePrompt(input)}`
  const systemPrompt = SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY
  const text = [
    'Sanitized synthetic reducer fixture.',
    '<facts>',
    '<fact confidence="0.85-1.0" candidateIds="candidate-1">First grounded synthetic claim</fact>',
    '<fact confidence="0.9-1.0" candidateIds="candidate-2">Second grounded synthetic claim</fact>',
    '<fact confidence="1.0" candidateIds="candidate-3">Third grounded synthetic claim</fact>',
    '</facts>',
  ].join('\n')
  const id = fingerprintId('conrequest', JSON.stringify([manifestId, systemPrompt, userPrompt]))
  const sha256 = createHash('sha256').update(text).digest('hex')
  const requestSnapshot = { id, manifestId, systemPrompt, userPrompt, status: 'completed' as const,
    responsePages: { count: 1, sha256, bytes: Buffer.byteLength(text, 'utf8') }, createdAt: '2026-10-11T00:00:00.000Z' }
  const responsePage = { ...requestSnapshot.responsePages, index: 0, text }
  return { manifestId, requestSnapshot, responsePage, candidates }
}

function reducerResponse(confidence: string, candidateIds = 'candidate-1'): string {
  return `<facts><fact confidence="${confidence}" candidateIds="${candidateIds}">A grounded claim</fact></facts>`
}

function twoCandidates(): Candidate[] {
  return [
    { id: 'candidate-1', key: 'one', fact: 'First claim', confidence: 0.8, sourceSessionIds: ['session-a'], children: [], legacyCandidateIds: ['candidate-1'] },
    { id: 'candidate-2', key: 'two', fact: 'Second claim', confidence: 0.7, sourceSessionIds: ['session-b'], children: [], legacyCandidateIds: ['candidate-2'] },
  ]
}

describe('semantic reducer response contract', () => {
  it('replays a sanitized completed legacy request by its exact old identity without another provider call', async () => {
    const { manifestId, requestSnapshot, responsePage, candidates } = sanitizedLegacyFixture()
    expect(requestSnapshot.systemPrompt).toBe(SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY)
    expect(fingerprintId('conrequest', JSON.stringify([requestSnapshot.manifestId, requestSnapshot.systemPrompt, requestSnapshot.userPrompt])))
      .toBe(requestSnapshot.id)
    expect(createHash('sha256').update(responsePage.text).digest('hex')).toBe(requestSnapshot.responsePages.sha256)
    expect(Buffer.byteLength(responsePage.text, 'utf8')).toBe(requestSnapshot.responsePages.bytes)
    expect(buildSemanticCandidatePrompt(candidates.map(candidate => ({ fact: candidate.fact, confidence: candidate.confidence,
      sourceSessionIds: candidate.sourceSessionIds, candidateIds: candidate.legacyCandidateIds })))
      .replace(/^/, `Consolidation manifest: ${manifestId}\n\n`)).toBe(requestSnapshot.userPrompt)

    const key = (scope: string, id: string) => `${scope}\0${id}`
    const responseKey = pageKey(manifestId, `response:${requestSnapshot.id}`, 0)
    const retainedRecord = wireClone(requestSnapshot)
    const retainedPage = wireClone(responsePage)
    const initial = [
      { scope: KV.state, key: requestSnapshot.id, value: retainedRecord },
      { scope: KV.state, key: responseKey, value: retainedPage },
      ...candidates.flatMap(candidate => candidate.sourceSessionIds).filter((id, index, ids) => ids.indexOf(id) === index)
        .map(sessionId => ({ scope: KV.sessions, key: sessionId, value: { project: 'synthetic-project' } })),
    ]
    const fixture = createKV(initial)
    const root = newRoot(manifestId, 'semantic', 'synthetic-project', [...new Set(candidates.flatMap(candidate => candidate.sourceSessionIds))]
      .map(sessionId => ({ id: `summary-${sessionId}`, sessionId, scope: KV.summaries, kind: 'summary' as const, project: 'synthetic-project', rowVersion: '1' })))
    root.phase = 'reduce'
    root.candidatePages = 1
    root.candidateCount = candidates.length
    root.reduction = { round: 0, inputPages: 1, inputCount: candidates.length, page: 0, offset: 0, outputPages: 0, outputCount: 0 }
    const inputPageKey = pageKey(root.id, 'candidates', 0)
    await fixture.set(KV.state, inputPageKey, { id: inputPageKey, candidates, examined: candidates.length, rejected: 0 })
    const provider = vi.fn(async () => { throw new Error('captured response must be replayed without provider dispatch') })
    const requests = new ConsolidationRequests(fixture.kv as never, root.id, provider, undefined, { used: 0, maximum: 1 })

    await reductionCut(fixture.kv as never, root as never, requests)

    expect(provider).not.toHaveBeenCalled()
    const output = await fixture.kv.get(KV.state, pageKey(root.id, 'reduction:1', 0)) as { candidates: Candidate[] }
    expect(output.candidates).toHaveLength(3)
    expect(output.candidates.map(candidate => candidate.confidence)).toEqual([0.85, 0.9, 1])
    expect(output.candidates.every(candidate => candidate.sourceSessionIds.length > 0)).toBe(true)
    expect(output.candidates.flatMap(candidate => candidate.legacyCandidateIds ?? []).sort())
      .toEqual(candidates.flatMap(candidate => candidate.legacyCandidateIds).sort())
    expect(fixture.values.get(key(KV.state, requestSnapshot.id))).toEqual(retainedRecord)
    expect(fixture.values.get(key(KV.state, responseKey))).toEqual(retainedPage)
  })

  it('reuses an exact pending legacy queue request without creating a replacement identity', async () => {
    const manifestId = 'pending-manifest'
    const userPrompt = 'Candidate facts with source evidence:\n\n[{"fact":"A","confidence":0.8,"sourceSessionIds":["s1"],"candidateIds":["candidate-1"]}]'
    const exactUserPrompt = `Consolidation manifest: ${manifestId}\n\n${userPrompt}`
    const requestId = fingerprintId('conrequest', JSON.stringify([manifestId, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY, exactUserPrompt]))
    const workItemId = 'work-existing'
    const request = { id: requestId, manifestId, systemPrompt: SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY, userPrompt: exactUserPrompt,
      status: 'pending', dispatchMode: 'queue', workItemId, createdAt: '2026-10-11T00:00:00.000Z' }
    const workItem = { id: workItemId, task: 'consolidation', correlationId: requestId, customId: requestId,
      systemPrompt: request.systemPrompt, userPrompt: request.userPrompt,
      metadata: { consolidationManifestId: manifestId, consolidationPartitionId: requestId, sourceFingerprint: requestId }, state: 'queued' }
    const fixture = createKV([
      { scope: KV.state, key: requestId, value: request },
      { scope: KV.fireworksBatchWorkItems, key: workItemId, value: workItem },
    ])
    const provider = vi.fn(async () => { throw new Error('pending request must not dispatch') })
    const queue = { enqueue: vi.fn(async () => ({ queued: true, workItemId: 'unexpected' })) }
    const requests = new ConsolidationRequests(fixture.kv as never, manifestId, provider, queue as never)

    await expect(requests.request(SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, userPrompt, [SEMANTIC_CANDIDATE_REDUCTION_SYSTEM_LEGACY]))
      .resolves.toBeUndefined()
    expect(requests.pending).toBe(true)
    expect(provider).not.toHaveBeenCalled()
    expect(queue.enqueue).not.toHaveBeenCalled()
    expect(fixture.values.has(`${KV.state}\0${fingerprintId('conrequest', JSON.stringify([manifestId, SEMANTIC_CANDIDATE_REDUCTION_SYSTEM, exactUserPrompt]))}`)).toBe(false)
  })

  it('accepts scalar confidence unchanged and normalizes an ordered interval to its conservative lower bound', () => {
    expect(parseReduced(reducerResponse('0.9'), twoCandidates().slice(0, 1))[0].confidence).toBe(0.9)
    expect(parseReduced(reducerResponse('0.85-1.0'), twoCandidates().slice(0, 1))[0].confidence).toBe(0.85)
    expect(parseSemantic('<facts><fact confidence="0.85-1.0" sourceIds="session-a">Claim</fact></facts>', ['session-a'])[0].confidence)
      .toBe(0.85)
  })

  it('accepts reordered structural attributes and explicit empty candidate collections', () => {
    expect(parseSemantic('<facts><fact sourceIds="session-a" confidence="0.85">Claim</fact></facts>', ['session-a']))
      .toMatchObject([{ fact: 'Claim', confidence: 0.85, sourceSessionIds: ['session-a'] }])
    expect(parseReduced('<facts><fact candidateIds="candidate-1" confidence="0.8">Claim</fact></facts>', twoCandidates().slice(0, 1)))
      .toMatchObject([{ fact: 'Claim', confidence: 0.8, children: ['candidate-1'] }])
    expect(parseProcedural('<procedures><procedure trigger="when needed" name="safe retry"><step>Check state</step><step>Retry once</step></procedure></procedures>', ['session-a']))
      .toMatchObject([{ name: 'safe retry', trigger: 'when needed', sourceSessionIds: ['session-a'], steps: ['Check state', 'Retry once'] }])
    expect(parseSemantic('<facts/>', [])).toEqual([])
    expect(parseReduced('<facts/>', [])).toEqual([])
    expect(parseProcedural('<procedures/>', [])).toEqual([])
  })

  it.each(['', ' ', '1e-1', '0x1'])('rejects blank and non-decimal confidence lexical value %j', confidence => {
    expect(() => parseSemantic(`<facts><fact confidence="${confidence}" sourceIds="session-a">Claim</fact></facts>`, ['session-a']))
      .toThrow('consolidation_confidence_invalid')
  })

  it('rejects unknown structural attributes and missing procedural source provenance', () => {
    expect(() => parseSemantic('<facts><fact confidence="0.8" sourceIds="session-a" ignored="x">Claim</fact></facts>', ['session-a']))
      .toThrow('Semantic provider returned malformed fact output')
    expect(() => parseProcedural('<procedures><procedure name="safe retry" trigger="when needed"><step>Check state</step><step>Retry once</step></procedure></procedures>', []))
      .toThrow('Empty or unsupported procedural candidate')
  })

  it.each(['1.01', '-0.1', '0.9-0.8', '0.8-1.1', 'NaN', 'Infinity', '0.8-Infinity', '0.9-NaN'])
    ('rejects invalid confidence value %s', confidence => {
      expect(() => parseReduced(reducerResponse(confidence), twoCandidates().slice(0, 1)))
        .toThrow('consolidation_confidence_invalid')
    })

  it('accepts a bounded plain-text preface before one facts root and rejects XML wrappers, extra roots, and trailing text', () => {
    const input = twoCandidates().slice(0, 1)
    const fact = '<fact confidence="0.8" candidateIds="candidate-1">Claim</fact>'
    expect(parseReduced(`${'a'.repeat(255)}\n<facts>${fact}</facts>`, input)).toHaveLength(1)
    expect(() => parseReduced(`${'a'.repeat(257)}<facts>${fact}</facts>`, input)).toThrow('malformed fact output')
    expect(() => parseReduced(`Note <b>important</b>\n<facts>${fact}</facts>`, input)).toThrow('malformed fact output')
    expect(() => parseReduced(`<facts>${fact}</facts> extra`, input)).toThrow('malformed fact output')
    expect(() => parseReduced(`<facts>${fact}</facts><facts>${fact}</facts>`, input)).toThrow('malformed fact output')
    expect(() => parseReduced(`<facts><facts>${fact}</facts></facts>`, input)).toThrow('malformed fact output')
  })

  it('preserves candidate coverage and rejects unknown, duplicated, or omitted candidate provenance', () => {
    const input = twoCandidates()
    expect(() => parseReduced(reducerResponse('0.8', 'unknown'), input)).toThrow('consolidation_reducer_coverage_invalid')
    expect(() => parseReduced(`<facts>${'<fact confidence="0.8" candidateIds="candidate-1">One</fact>'.repeat(2)}</facts>`, input))
      .toThrow('consolidation_reducer_coverage_invalid')
    expect(() => parseReduced(reducerResponse('0.8', 'candidate-1'), input)).toThrow('Semantic reducer omitted candidate facts')
  })

  it('uses scalar examples in new reducer prompts while retaining exact legacy prompt texts for replay', () => {
    expect(SEMANTIC_CANDIDATE_REDUCTION_SYSTEM).toContain('confidence="0.85"')
    expect(SEMANTIC_CANDIDATE_REDUCTION_SYSTEM).toContain('never return a range')
    expect(SEMANTIC_CANDIDATE_REDUCTION_SYSTEM).not.toContain('0.0-1.0')
    expect(SEMANTIC_CANDIDATE_FINAL_SYSTEM).toContain('confidence="0.85"')
    expect(SEMANTIC_CANDIDATE_FINAL_SYSTEM).not.toContain('0.0-1.0')
    expect(SEMANTIC_CANDIDATE_FINAL_SYSTEM_LEGACY).toContain('confidence="0.0-1.0"')
  })
})
