interface TransportDiagnostics {
  connection: string;
  queuedMessages: number;
  queuedBytes: number;
  pendingInvocations: number;
  pendingInvocationBytes: number;
  bufferedBytes: number;
  rejected: number;
}

function transportDiagnostics(client: unknown): TransportDiagnostics | undefined {
  if (typeof client !== 'object' || client === null ||
      !('__agentmemoryTransportDiagnostics' in client) ||
      typeof client.__agentmemoryTransportDiagnostics !== 'function') return undefined;
  const value: unknown = client.__agentmemoryTransportDiagnostics();
  if (typeof value !== 'object' || value === null ||
      !('connection' in value) || typeof value.connection !== 'string' ||
      !('queuedMessages' in value) || typeof value.queuedMessages !== 'number' ||
      !('queuedBytes' in value) || typeof value.queuedBytes !== 'number' ||
      !('pendingInvocations' in value) || typeof value.pendingInvocations !== 'number' ||
      !('pendingInvocationBytes' in value) || typeof value.pendingInvocationBytes !== 'number' ||
      !('bufferedBytes' in value) || typeof value.bufferedBytes !== 'number' ||
      !('rejected' in value) || typeof value.rejected !== 'number') return undefined;
  return { connection: value.connection, queuedMessages: value.queuedMessages,
    queuedBytes: value.queuedBytes, pendingInvocations: value.pendingInvocations,
    pendingInvocationBytes: value.pendingInvocationBytes,
    bufferedBytes: value.bufferedBytes, rejected: value.rejected };
}

export function startMemoryDiagnostics(client: unknown): () => void {
  const samples: Array<ReturnType<typeof sample>> = [];
  let lastLog = 0;
  let lastConnection: string | undefined;
  let lastHeapBand = -1;
  function sample() {
    const { rss, heapUsed, heapTotal, external, arrayBuffers } = process.memoryUsage();
    return { at: new Date().toISOString(), rss, heapUsed, heapTotal, external,
      arrayBuffers, transport: transportDiagnostics(client) };
  }
  const capture = () => {
    const current = sample();
    if (samples.length === 30) samples.shift();
    samples.push(current);
    const heapBand = Math.floor(current.heapUsed / (256 * 1024 * 1024));
    const changed = current.transport?.connection !== lastConnection || heapBand !== lastHeapBand;
    if (!lastLog || ((changed || Date.now() - lastLog >= 15 * 60000) && Date.now() - lastLog >= 60000)) {
      console.info('[agentmemory] Memory diagnostics', JSON.stringify(current));
      lastLog = Date.now();
      lastConnection = current.transport?.connection;
      lastHeapBand = heapBand;
    }
  };
  capture();
  const timer = setInterval(capture, 30000);
  timer.unref();
  return () => { clearInterval(timer); samples.length = 0; };
}
