import { parseXmlResult, type XmlElement } from "../utils/xml-result.js";

export interface ReflectionInsight { title: string; content: string; confidence: number }

export function parseReflectionResult(response: string): readonly ReflectionInsight[] {
  const invalid = (): never => { throw new Error("reflect_generation_invalid"); };
  const xml = response.trim().replace(/^```(?:xml)?\s*\n([\s\S]*?)\n```$/, "$1");
  let root: XmlElement;
  try { root = parseXmlResult(xml, { maxBytes: 16384 }); } catch { return invalid(); }
  if (root.name !== "insights" || Object.keys(root.attributes).length || root.text.trim() || root.children.length > 5) return invalid();
  return root.children.map(element => {
    if (element.name !== "insight" || Object.keys(element.attributes).some(key => !["confidence", "title"].includes(key))) return invalid();
    const number = element.attributes.confidence;
    if (!number?.trim() || !/^(?:0(?:\.\d+)?|1(?:\.0+)?)$/.test(number)) return invalid();
    const confidence = Number(number);
    if (!Number.isFinite(confidence)) return invalid();
    if (element.children.some(child => child.name !== "title" || child.children.length || Object.keys(child.attributes).length) || element.children.length > 1) return invalid();
    const attributeTitle = element.attributes.title?.trim();
    const childTitle = element.children[0]?.text.trim();
    if (attributeTitle && childTitle && attributeTitle !== childTitle) return invalid();
    const title = attributeTitle ?? childTitle;
    const content = element.text.trim();
    if (!title || !content) return invalid();
    return { title, content, confidence };
  });
}
