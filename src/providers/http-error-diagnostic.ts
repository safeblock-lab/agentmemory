export interface HttpErrorDiagnostic { code?: string; param?: string }
const safeIdentifier = (value: unknown): string | undefined => typeof value === 'string'
  && /^[A-Za-z0-9_.:[\]-]{1,128}$/.test(value) ? value : undefined;

export async function readHttpErrorDiagnostic(response: Response): Promise<HttpErrorDiagnostic> {
  const reader = response.body?.getReader();
  if (!reader) return {};
  let timer: ReturnType<typeof setTimeout> | undefined;
  const read = async (): Promise<HttpErrorDiagnostic> => {
    const chunks: Uint8Array[] = [];
    let bytes = 0;
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      bytes += part.value.byteLength;
      if (bytes > 8192) return {};
      chunks.push(part.value);
    }
    try {
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      const error = body?.error;
      return { code: safeIdentifier(error?.code), param: safeIdentifier(error?.param) };
    } catch { return {}; }
  };
  try {
    return await Promise.race([read(), new Promise<HttpErrorDiagnostic>(resolve => {
      timer = setTimeout(() => resolve({}), 1000);
    })]);
  } catch { return {}; }
  finally {
    if (timer !== undefined) clearTimeout(timer);
    void reader.cancel().catch(() => undefined);
  }
}
