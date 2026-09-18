import {
  getTypeSafeConfig,
  type TypeSafeConfig,
  type TypeSafeFeature,
} from "../config.js";

export const TYPESAFE_API_ENDPOINT = "https://api.typesafe.ai/v1/systemone";

export interface TypeSafeNoulQuestion {
  type: "noul";
  instructions: string;
  criteria?: { true?: string; false?: string };
}

export interface TypeSafeChoiceQuestion {
  type: "choice";
  instructions: string;
  criteria: Readonly<Record<string, string | null>>;
}

export interface TypeSafeScoreQuestion {
  type: "score";
  instructions: string;
  criteria: readonly string[];
}

export type TypeSafeQuestion =
  | TypeSafeNoulQuestion
  | TypeSafeChoiceQuestion
  | TypeSafeScoreQuestion;

export interface TypeSafeNoulAnswer {
  type: "noul";
  noul: number;
}

export interface TypeSafeChoiceAnswer {
  type: "choice";
  choice: string;
  probabilities: Readonly<Record<string, number>>;
  confidence: number;
}

export interface TypeSafeScoreAnswer {
  type: "score";
  score: number;
  legend: Readonly<Record<string, string>>;
  probabilities: Readonly<Record<string, number>>;
  confidence: number;
}

export type TypeSafeAnswer =
  | TypeSafeNoulAnswer
  | TypeSafeChoiceAnswer
  | TypeSafeScoreAnswer;

export interface TypeSafeDecisionProviderOptions {
  config?: TypeSafeConfig;
  fetcher?: typeof fetch;
}

const MAX_QUESTIONS = 16;
const MAX_QUESTION_CHARS = 16_000;
const MAX_RESPONSE_BYTES = 64 * 1024;
const MAX_REQUEST_BYTES = 96 * 1024;
const MAX_INSTRUCTIONS_CHARS = 2_000;
const MAX_CRITERION_CHARS = 512;
const MAX_CHOICE_OPTIONS = 32;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNumberInRange(value: unknown, min: number, max: number): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= min && value <= max;
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const keys = Object.keys(value);
  return keys.length === expected.length && expected.every((key) => Object.hasOwn(value, key));
}

function parseProbabilityMap(
  value: unknown,
  expectedKeys: readonly string[],
): Readonly<Record<string, number>> | undefined {
  if (!isRecord(value) || !hasExactKeys(value, expectedKeys)) return undefined;
  const entries: [string, number][] = [];
  let total = 0;
  for (const key of expectedKeys) {
    const probability = value[key];
    if (!isNumberInRange(probability, 0, 1)) return undefined;
    total += probability;
    entries.push([key, probability]);
  }
  if (Math.abs(total - 1) > 0.02) return undefined;
  return Object.fromEntries(entries);
}

function validateQuestion(question: TypeSafeQuestion): boolean {
  if (
    typeof question.instructions !== "string" ||
    question.instructions.trim().length === 0 ||
    question.instructions.length > MAX_INSTRUCTIONS_CHARS
  ) return false;

  if (question.type === "noul") {
    if (question.criteria === undefined) return true;
    if (!isRecord(question.criteria) || Object.keys(question.criteria).length > 2) return false;
    return Object.entries(question.criteria).every(([key, value]) =>
      (key === "true" || key === "false") &&
      typeof value === "string" &&
      value.trim().length > 0 &&
      value.length <= MAX_CRITERION_CHARS,
    );
  }

  if (question.type === "choice") {
    const criteria = question.criteria;
    const keys = Object.keys(criteria);
    return keys.length >= 2 && keys.length <= MAX_CHOICE_OPTIONS && keys.every((key) =>
      key.trim().length > 0 && key.length <= 128 &&
      (criteria[key] === null || (
        typeof criteria[key] === "string" && criteria[key]!.length <= MAX_CRITERION_CHARS
      )),
    );
  }

  return question.type === "score" &&
    Array.isArray(question.criteria) &&
    question.criteria.length >= 2 &&
    question.criteria.length <= 10 &&
    question.criteria.every((criterion) =>
      typeof criterion === "string" &&
      criterion.trim().length > 0 &&
      criterion.length <= MAX_CRITERION_CHARS,
    );
}

function validateAnswer(question: TypeSafeQuestion, value: unknown): TypeSafeAnswer | undefined {
  if (!isRecord(value) || (value.type !== undefined && value.type !== question.type)) return undefined;
  if (question.type === "noul") {
    return isNumberInRange(value.noul, 0, 1)
      ? { type: "noul", noul: value.noul }
      : undefined;
  }

  if (question.type === "choice") {
    const options = Object.keys(question.criteria);
    const probabilities = parseProbabilityMap(value.probabilities, options);
    if (
      typeof value.choice !== "string" ||
      !options.includes(value.choice) ||
      !probabilities ||
      !isNumberInRange(value.confidence, 0, 1)
    ) return undefined;
    return {
      type: "choice",
      choice: value.choice,
      probabilities,
      confidence: value.confidence,
    };
  }

  const levels = question.criteria.map((_, index) => String(index));
  const probabilities = parseProbabilityMap(value.probabilities, levels);
  if (
    !isNumberInRange(value.score, 0, question.criteria.length - 1) ||
    !isNumberInRange(value.confidence, 0, 1) ||
    !probabilities
  ) return undefined;
  if (value.legend !== undefined) {
    if (!isRecord(value.legend)) return undefined;
    const legend = value.legend;
    if (!hasExactKeys(legend, levels)) return undefined;
    if (levels.some((level, index) => legend[level] !== question.criteria[index])) return undefined;
  }
  return {
    type: "score",
    score: value.score,
    legend: Object.fromEntries(question.criteria.map((description, index) => [String(index), description])),
    probabilities,
    confidence: value.confidence,
  };
}

function compactState(value: unknown, maxChars: number): unknown | undefined {
  if (typeof value !== "string" && (!value || typeof value !== "object")) return undefined;
  let serialized: string | undefined;
  try {
    serialized = typeof value === "string" ? value : JSON.stringify(value);
  } catch {
    return undefined;
  }
  if (serialized === undefined) return undefined;
  if (serialized.length <= maxChars) return value;

  const marker = "\n[TypeSafe state truncated]\n";
  const available = maxChars - marker.length;
  if (available <= 0) return undefined;
  const headChars = Math.ceil(available / 2);
  return `${serialized.slice(0, headChars)}${marker}${serialized.slice(-(available - headChars))}`;
}

async function readBoundedResponse(response: Response): Promise<string | undefined> {
  const declaredLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(declaredLength) && declaredLength > MAX_RESPONSE_BYTES) return undefined;
  const reader = response.body?.getReader();
  if (!reader) return undefined;

  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > MAX_RESPONSE_BYTES) {
        await reader.cancel().catch(() => undefined);
        return undefined;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}

function validateResponse(
  response: unknown,
  questions: Readonly<Record<string, TypeSafeQuestion>>,
): Record<string, TypeSafeAnswer> | undefined {
  if (!isRecord(response)) return undefined;
  if (response.model !== undefined && (typeof response.model !== "string" || response.model.length === 0)) return undefined;
  const ids = Object.keys(questions);
  if (!isRecord(response.answers) || !hasExactKeys(response.answers, ids)) return undefined;
  if (response.usage !== undefined) {
    if (!isRecord(response.usage)) return undefined;
    for (const key of ["input_tokens", "output_tokens"] as const) {
      const count = response.usage[key];
      if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) return undefined;
    }
  }

  const answers: Record<string, TypeSafeAnswer> = {};
  for (const id of ids) {
    const answer = validateAnswer(questions[id]!, response.answers[id]);
    if (!answer) return undefined;
    answers[id] = answer;
  }
  return answers;
}

export class TypeSafeDecisionProvider {
  private readonly config: TypeSafeConfig;
  private readonly fetcher: typeof fetch | undefined;

  constructor(options: TypeSafeDecisionProviderOptions = {}) {
    this.config = options.config ?? getTypeSafeConfig();
    this.fetcher = options.fetcher ?? globalThis.fetch?.bind(globalThis);
  }

  async evaluate(
    feature: TypeSafeFeature,
    state: unknown,
    questions: Readonly<Record<string, TypeSafeQuestion>>,
  ): Promise<Record<string, TypeSafeAnswer> | undefined> {
    try {
      if (!this.config.enabled || !this.config.features[feature] || !this.config.apiKey || !this.fetcher) {
        return undefined;
      }
      const ids = Object.keys(questions);
      if (
        ids.length === 0 ||
        ids.length > MAX_QUESTIONS ||
        !ids.every((id) => /^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(id)) ||
        !ids.every((id) => validateQuestion(questions[id]!))
      ) return undefined;

      if (JSON.stringify(questions).length > MAX_QUESTION_CHARS) return undefined;
      const stateLimit = Math.max(256, Math.min(64_000, this.config.maxStateChars));
      const boundedState = compactState(state, stateLimit);
      if (boundedState === undefined) return undefined;

      const body = JSON.stringify({ state: boundedState, model: "jev-latest", questions });
      if (new TextEncoder().encode(body).byteLength > MAX_REQUEST_BYTES) return undefined;

      const controller = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<undefined>((resolve) => {
        timer = setTimeout(() => {
          controller.abort();
          resolve(undefined);
        }, this.config.timeoutMs);
      });
      const request = this.fetchAndValidate(body, questions, controller.signal);
      try {
        return await Promise.race([request, timeout]);
      } finally {
        if (timer !== undefined) clearTimeout(timer);
      }
    } catch {
      return undefined;
    }
  }

  async evaluateNoul(
    feature: TypeSafeFeature,
    state: unknown,
    instructions: string,
    criteria?: TypeSafeNoulQuestion["criteria"],
  ): Promise<number | undefined> {
    const answers = await this.evaluate(feature, state, {
      result: { type: "noul", instructions, criteria },
    });
    const answer = answers?.result;
    return answer?.type === "noul" ? answer.noul : undefined;
  }

  async evaluateChoice(
    feature: TypeSafeFeature,
    state: unknown,
    instructions: string,
    criteria: TypeSafeChoiceQuestion["criteria"],
  ): Promise<TypeSafeChoiceAnswer | undefined> {
    const answers = await this.evaluate(feature, state, {
      result: { type: "choice", instructions, criteria },
    });
    const answer = answers?.result;
    return answer?.type === "choice" ? answer : undefined;
  }

  async evaluateScore(
    feature: TypeSafeFeature,
    state: unknown,
    instructions: string,
    criteria: readonly string[],
  ): Promise<TypeSafeScoreAnswer | undefined> {
    const answers = await this.evaluate(feature, state, {
      result: { type: "score", instructions, criteria },
    });
    const answer = answers?.result;
    return answer?.type === "score" ? answer : undefined;
  }

  private async fetchAndValidate(
    body: string,
    questions: Readonly<Record<string, TypeSafeQuestion>>,
    signal: AbortSignal,
  ): Promise<Record<string, TypeSafeAnswer> | undefined> {
    try {
      const response = await this.fetcher!(TYPESAFE_API_ENDPOINT, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.config.apiKey}`,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body,
        signal,
      });
      if (!response.ok) return undefined;
      const responseText = await readBoundedResponse(response);
      if (responseText === undefined) return undefined;
      return validateResponse(JSON.parse(responseText) as unknown, questions);
    } catch {
      return undefined;
    }
  }
}
