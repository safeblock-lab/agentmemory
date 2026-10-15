import { Tiktoken } from "js-tiktoken/lite";
import o200kBase from "js-tiktoken/ranks/o200k_base";

let encoding: Tiktoken | undefined;

// Sol's server tokenizer and message framing are not publicly specified.
// o200k_base counts text locally; provider usage remains authoritative.
export function countContextTokens(text: string): number {
  encoding ??= new Tiktoken(o200kBase);
  return encoding.encode(text, [], []).length;
}
