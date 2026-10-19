import { Buffer } from "node:buffer";
import { fingerprintId } from "../state/schema.js";
import type { PublicationOutputContract } from "./publication-call-options.js";

export const PUBLICATION_DISPOSITION_MIGRATION_VERSION = "publication-judge-disposition-v2";
export interface PublicationDispositionMigration {
  version: typeof PUBLICATION_DISPOSITION_MIGRATION_VERSION;
  sourceIdentity: string;
  sourceRecordFingerprint: string;
  targetContract: PublicationOutputContract;
}

const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export function validPublicationDispositionMigration(value: unknown): value is PublicationDispositionMigration {
  if (!object(value) || Object.keys(value).length !== 4
    || value.version !== PUBLICATION_DISPOSITION_MIGRATION_VERSION
    || typeof value.sourceIdentity !== "string" || !value.sourceIdentity
    || Buffer.byteLength(value.sourceIdentity, "utf8") > 524_288
    || typeof value.sourceRecordFingerprint !== "string" || !/^publication-disposition-source_[a-f0-9]{16}$/.test(value.sourceRecordFingerprint)) return false;
  const contract = value.targetContract;
  return object(contract) && Object.keys(contract).length === 4
    && contract.version === "publication-verdict-v2" && contract.parser === "closed-json-verdict-v2"
    && object(contract.binding) && contract.binding.contractVersion === contract.version
    && object(contract.options) && object(contract.options.responseFormat)
    && contract.options.responseFormat.type === "json_schema";
}

export function publicationDispositionSourceFingerprint(record: object): string {
  const canonical = (value: unknown): unknown => Array.isArray(value) ? value.map(canonical)
    : object(value) ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
  const { supersededBy: _link, ...source } = record as Record<string, unknown>;
  return fingerprintId("publication-disposition-source", JSON.stringify(canonical(source)));
}
