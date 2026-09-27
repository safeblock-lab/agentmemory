import type { LlmCallOptions, MemoryProvider } from '../types.js'
import { getEnvVar } from '../config.js'
import { fetchWithTimeout } from './_fetch.js'
import { extractLlmTokenUsage, startLlmCallTelemetry } from './_llm-logging.js'

/**
 * MiniMax provider using raw fetch to call MiniMax's Anthropic-compatible API.
 *
 * The Anthropic SDK automatically injects `x-stainless-*` headers that MiniMax
 * rejects with 403. This provider bypasses the SDK and calls the API directly.
 *
 * Required env vars (loaded from ~/.agentmemory/.env or process.env):
 *   MINIMAX_API_KEY  — your MiniMax API key
 *   MINIMAX_MODEL    — model name (default: MiniMax-M3)
 *   MAX_TOKENS       — max output tokens (default: 4096)
 *
 * Optional:
 *   MINIMAX_BASE_URL — base URL without path (default: https://api.minimax.io/anthropic)
 */
import { summaryOutputTokens } from './task-output-limits.js';

export class MinimaxProvider implements MemoryProvider {
  name = 'minimax'
  private apiKey: string
  private model: string
  private maxTokens: number
  private baseUrl: string

  constructor(apiKey: string, model: string, maxTokens: number) {
    this.apiKey = apiKey
    this.model = model
    this.maxTokens = maxTokens
    this.baseUrl =
      getEnvVar('MINIMAX_BASE_URL') || 'https://api.minimax.io/anthropic'
  }

  async compress(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(systemPrompt, userPrompt, 'compress', options)
  }

  async summarize(systemPrompt: string, userPrompt: string, options?: LlmCallOptions): Promise<string> {
    return this.call(systemPrompt, userPrompt, 'summarize', options)
  }

  private async call(systemPrompt: string, userPrompt: string, operation: 'compress' | 'summarize', options?: LlmCallOptions): Promise<string> {
    const url = `${this.baseUrl}/v1/messages`
    const telemetry = startLlmCallTelemetry({ provider: this.name, model: this.model, operation })
    let response: Response
    try {
      response = await fetchWithTimeout(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': this.apiKey,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: this.model,
        max_tokens: summaryOutputTokens(options, this.maxTokens),
        system: systemPrompt,
        messages: [{ role: 'user', content: userPrompt }],
      }),
      })
    } catch (error) {
      telemetry.failure({ errorKind: error instanceof Error && error.name === 'AbortError' ? 'timeout' : 'network' })
      throw error
    }

    if (!response.ok) {
      telemetry.failure({ httpStatus: response.status, errorKind: 'provider_response' })
      const text = await response.text()
      throw new Error(`MiniMax API error ${response.status}: ${text}`)
    }

    let data: {
      content?: Array<{ type: string; text?: string }>
      usage?: unknown
    }
    try {
      data = (await response.json()) as typeof data
    } catch {
      telemetry.failure({ httpStatus: response.status, errorKind: 'invalid_response' })
      throw new Error('MiniMax returned an invalid JSON response')
    }
    const textBlock = data.content?.find((b) => b.type === 'text')
    const content = textBlock?.text ?? ''
    telemetry.success({ httpStatus: response.status, usage: extractLlmTokenUsage(data.usage), responseChars: content.length })
    return content
  }
}
