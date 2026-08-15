import Anthropic from '@anthropic-ai/sdk'
import type { MemoryProvider } from '../types.js'
import { extractLlmTokenUsage, startLlmCallTelemetry } from './_llm-logging.js'

export class AnthropicProvider implements MemoryProvider {
  name = 'anthropic'
  private client: Anthropic
  private model: string
  private maxTokens: number

  constructor(apiKey: string, model: string, maxTokens: number, baseURL?: string) {
    this.client = new Anthropic({ apiKey, ...(baseURL ? { baseURL } : {}) })
    this.model = model
    this.maxTokens = maxTokens
  }

  async compress(systemPrompt: string, userPrompt: string): Promise<string> {
    return this.call(systemPrompt, userPrompt, 'compress')
  }

  async summarize(systemPrompt: string, userPrompt: string): Promise<string> {
    return this.call(systemPrompt, userPrompt, 'summarize')
  }

  async describeImage(imageData: string, mimeType: string, prompt: string): Promise<string> {
    const telemetry = startLlmCallTelemetry({ provider: this.name, model: this.model, operation: 'describe_image' })
    try {
      const response = await this.client.messages.create({
      model: this.model,
      max_tokens: this.maxTokens,
      messages: [{
        role: 'user',
        content: [
          {
            type: 'image',
            source: { type: 'base64', media_type: mimeType as 'image/png' | 'image/jpeg' | 'image/gif' | 'image/webp', data: imageData },
          },
          { type: 'text', text: prompt },
        ],
      }],
      })

      const content = response.content.find((b) => b.type === 'text')?.text ?? ''
      telemetry.success({ usage: extractLlmTokenUsage(response.usage), responseChars: content.length })
      return content
    } catch (error) {
      telemetry.failure({ errorKind: error instanceof Error && error.name === 'AbortError' ? 'timeout' : 'network' })
      throw error
    }
  }

  private async call(systemPrompt: string, userPrompt: string, operation: 'compress' | 'summarize'): Promise<string> {
    const telemetry = startLlmCallTelemetry({ provider: this.name, model: this.model, operation })
    try {
      const response = await this.client.messages.create({
      model: this.model,
      max_tokens: this.maxTokens,
      system: systemPrompt,
      messages: [{ role: 'user', content: userPrompt }],
      })

      const content = response.content.find((b) => b.type === 'text')?.text ?? ''
      telemetry.success({ usage: extractLlmTokenUsage(response.usage), responseChars: content.length })
      return content
    } catch (error) {
      telemetry.failure({ errorKind: error instanceof Error && error.name === 'AbortError' ? 'timeout' : 'network' })
      throw error
    }
  }
}
