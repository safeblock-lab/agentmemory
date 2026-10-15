import { describe, expect, it } from "vitest";
import { parseXmlResult } from "../src/utils/xml-result.js";

const parse = (xml: string) => parseXmlResult(xml, { maxBytes: 1024 * 1024 });

describe("parseXmlResult", () => {
  it("preserves attribute-independent structure, decoded text and CDATA", () => {
    const first = parse('<insights><insight confidence="0.9" title="A &amp; B"><title><![CDATA[A < B]]></title>before &quot;x&quot; &apos;y&apos; &lt; &gt; &#65; &#x42;<detail/>after</insight></insights>');
    const reordered = parse('<insights><insight title="A &amp; B" confidence="0.9"><title><![CDATA[A < B]]></title>before &quot;x&quot; &apos;y&apos; &lt; &gt; &#65; &#x42;<detail/>after</insight></insights>');
    expect(first).toEqual(reordered);
    expect(first.children[0]).toMatchObject({
      attributes: { confidence: "0.9", title: "A & B" },
      text: 'before "x" \'y\' < > A Bafter',
      children: [{ name: "title", text: "A < B" }, { name: "detail", text: "" }],
    });
    expect(first.text).toBe("");
  });

  it("accepts explicit empty roots and surrounding whitespace", () => {
    expect(parse(" \n<insights/>\t")).toEqual({ name: "insights", attributes: {}, children: [], text: "" });
    expect(parse("<insights></insights>")).toEqual(parse("<insights/>"));
  });

  it("preserves whitespace and handles prototype-like attribute names", () => {
    const result = parse('<root __proto__="value" hasOwnProperty="yes">  a\n b </root>');
    expect(result.text).toBe("  a\n b ");
    expect(result.attributes.__proto__).toBe("value");
    expect(result.attributes.hasOwnProperty).toBe("yes");
  });

  it.each([
    "", "   ", "<a>", "<a><b></a>", "<a></b>", '<a x="1" x="2"/>',
    '<a xmlns:p="urn:x" xmlns:q="urn:x" p:x="1" q:x="2"/>',
    "<a>&unknown;</a>", "<a>&nbsp;</a>", '<a x="&unknown;"/>',
    "<a>&#0;</a>", "<a>&#x110000;</a>", "<a>&#65junk;</a>",
    '<!DOCTYPE a><a/>', '<!DOCTYPE a [<!ENTITY custom "value">]><a>&custom;</a>',
    "<!ENTITY custom 'value'><a/>", "<a/><b/>", "<a/>junk", "junk<a/>",
    '<a x=unquoted/>', '<a x="unfinished/>', "<a><!bad></a>",
  ])("rejects malformed or forbidden XML: %s", (xml) => {
    expect(() => parse(xml)).toThrow(/Invalid XML result/);
  });

  it("enforces UTF-8 bytes at the exact caller limit", () => {
    const xml = "<a>é</a>";
    expect(() => parseXmlResult(xml, { maxBytes: Buffer.byteLength(xml) })).not.toThrow();
    expect(() => parseXmlResult(xml, { maxBytes: Buffer.byteLength(xml) - 1 })).toThrow(/byte limit/);
  });

  it.each([0, -1, 1.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1])("rejects invalid byte bound %s", (maxBytes) => {
    expect(() => parseXmlResult("<a/>", { maxBytes })).toThrow(RangeError);
  });

  it("accepts depth32 and rejects depth33", () => {
    expect(() => parse("<a>".repeat(32) + "</a>".repeat(32))).not.toThrow();
    expect(() => parse("<a>".repeat(33) + "</a>".repeat(33))).toThrow(/depth limit/);
  });

  it("accepts4096 nodes including the root and rejects4097", () => {
    expect(() => parse("<a>" + "<b/>".repeat(4095) + "</a>")).not.toThrow();
    expect(() => parse("<a>" + "<b/>".repeat(4096) + "</a>")).toThrow(/node limit/);
  });
});
