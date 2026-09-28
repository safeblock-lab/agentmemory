export const UPDATE_SECRET_REQUIRED = "Set AGENTMEMORY_UPDATE_SECRET to at least 32 characters in ~/.agentmemory/.env and restart AgentMemory.";

export function configuredUpdateSecret(): string | undefined {
  const secret = process.env["AGENTMEMORY_UPDATE_SECRET"];
  return secret && secret.length >= 32 ? secret : undefined;
}
