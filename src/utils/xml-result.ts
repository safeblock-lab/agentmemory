import sax from "sax";

export interface XmlElement {
  name: string;
  attributes: Readonly<Record<string, string>>;
  children: readonly XmlElement[];
  text: string;
}

interface BuildingElement {
  name: string;
  attributes: Record<string, string>;
  children: XmlElement[];
  text: string;
}

export function parseXmlResult(xml: string, options: { maxBytes: number }): XmlElement {
  if (!Number.isSafeInteger(options.maxBytes) || options.maxBytes < 1) {
    throw new RangeError("XML result maxBytes must be a positive safe integer");
  }
  const fail = (reason: string): never => {
    throw new Error(`Invalid XML result: ${reason}`);
  };
  if (Buffer.byteLength(xml, "utf8") > options.maxBytes) fail("byte limit exceeded");

  const settings = { strictEntities: true, xmlns: true, position: false };
  const reader = sax.parser(true, settings);
  const stack: BuildingElement[] = [];
  let root: XmlElement | undefined;
  let nodes = 0;
  let attributes: Record<string, string> = Object.create(null);
  let expandedAttributes = new Set<string>();

  reader.onerror = () => fail("malformed document");
  reader.ondoctype = () => fail("DOCTYPE is forbidden");
  reader.onsgmldeclaration = () => fail("declarations are forbidden");
  reader.onopentagstart = () => {
    attributes = Object.create(null);
    expandedAttributes = new Set();
  };
  reader.onattribute = (attribute) => {
    if (Object.hasOwn(attributes, attribute.name)) fail("duplicate attribute");
    if ("uri" in attribute && "local" in attribute) {
      const expandedName = JSON.stringify([attribute.uri, attribute.local]);
      if (expandedAttributes.has(expandedName)) fail("duplicate attribute");
      expandedAttributes.add(expandedName);
    }
    attributes[attribute.name] = attribute.value;
  };
  reader.onopentag = (tag) => {
    if (stack.length === 0 && root) fail("multiple roots");
    if (stack.length >= 32) fail("depth limit exceeded");
    if (++nodes > 4096) fail("node limit exceeded");
    stack.push({ name: tag.name, attributes, children: [], text: "" });
  };
  const appendText = (text: string) => {
    const element = stack.at(-1);
    if (element) element.text += text;
    else if (text.trim()) fail("text outside root");
  };
  reader.ontext = appendText;
  reader.oncdata = appendText;
  reader.onclosetag = () => {
    const element = stack.pop();
    if (!element) return fail("unmatched close tag");
    const completed: XmlElement = Object.freeze({
      ...element,
      attributes: Object.freeze(element.attributes),
      children: Object.freeze(element.children),
    });
    const parent = stack.at(-1);
    if (parent) parent.children.push(completed);
    else root = completed;
  };
  reader.write(xml).close();
  if (!root || stack.length) return fail("missing or incomplete root");
  return root;
}
