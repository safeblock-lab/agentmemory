import { resolve } from 'node:path';
import type { EmbeddingProvider } from '../types.js';

type Extractor = (text: string[], options: { pooling: string; normalize: boolean }) => Promise<{ tolist(): number[][] }>;
export class IndexedLocalEmbedding implements EmbeddingProvider {
  readonly name = 'Xenova/all-MiniLM-L6-v2';
  readonly dimensions = 384;
  private loading: Promise<Extractor> | null = null;
  private tail: Promise<void> = Promise.resolve();
  private pending = 0;
  async embed(text: string): Promise<Float32Array> { return (await this.embedBatch([text]))[0]; }
  async embedBatch(texts: string[]): Promise<Float32Array[]> {
    if (texts.length > 32) throw new Error('Local embedding batch exceeds 32 documents.');
    if (this.pending >= 8) throw new Error('Local embedding queue exceeds 8 batches.');
    this.pending++;
    const previous = this.tail;
    let release!: () => void;
    this.tail = new Promise(resolve => { release = resolve; });
    await previous;
    try {
      this.loading ??= this.load().catch(error => { this.loading = null; throw error; });
      const output = await (await this.loading)(texts.map(text => text.slice(0, 16_000)), { pooling: 'mean', normalize: true });
      const result = output.tolist().map(value => new Float32Array(value));
      if (result.length !== texts.length || result.some(value => value.length !== this.dimensions || value.some(number => !Number.isFinite(number)))) throw new Error('Invalid local embedding output.');
      return result;
    } finally { this.pending--; release(); }
  }
  private async load(): Promise<Extractor> {
    const transformers = await import('@huggingface/transformers');
    const modelPath = resolve(process.cwd(), '.cache', 'agentmemory', 'embeddings', ...this.name.split('/'));
    return await transformers.pipeline('feature-extraction', modelPath, {
      dtype: 'q8', local_files_only: true, cache_dir: resolve(process.cwd(), '.cache', 'agentmemory', 'embeddings'),
      session_options: { intraOpNumThreads: 1, interOpNumThreads: 1 },
    }) as unknown as Extractor;
  }
}
