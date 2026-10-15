# Provider XML result contract

`src/utils/xml-result.ts` provides the shared structural reader for provider XML. Domain consumers remain responsible for their own allowed roots, attributes, children, required fields, numeric ranges, enums and provenance. A successful structural parse alone does not authorize publication or acknowledge a work unit.

```typescript
interface XmlElement {
  name: string;
  attributes: Readonly<Record<string, string>>;
  children: readonly XmlElement[];
  text: string;
}

parseXmlResult(xml: string, options: { maxBytes: number }): XmlElement;
```

The reader uses `sax` **1.6.1** in strict mode with `strictEntities` and namespace processing enabled; development declarations use `@types/sax` **1.2.7**. Both versions are pinned exactly. Workspace installation uses `npm install --ignore-scripts --save-exact sax@1.6.1` and `npm install --ignore-scripts --save-dev --save-exact @types/sax@1.2.7`. See the [official sax documentation](https://github.com/isaacs/sax-js).

Attribute order does not matter. Predefined XML entities, numeric references and CDATA are decoded. Names remain case-sensitive. `text` concatenates direct text segments in order, including text before, between and after children; child text remains on the child. Whitespace is preserved. Consumers must reject inappropriate mixed content explicitly. XML comments and processing instructions do not contribute text. The reader does not strip Markdown fences or repair malformed XML; any boundary normalization must be explicit in its caller.

The caller must supply a positive safe-integer UTF-8 byte ceiling. The reader rejects input exceeding that ceiling, element depth above **32**, or more than **4096 elements**, including the root. It rejects missing roots, malformed nesting, duplicate attribute names or expanded namespace names, unknown entities, DTDs/custom entity declarations, multiple roots and non-whitespace text outside the root. Invalid bounds throw `RangeError`; invalid documents throw `Error` with an `Invalid XML result:` prefix and a bounded reason, without embedding provider text. No partial tree is returned. Returned elements, attribute maps and child arrays are frozen.

Valid empty roots produce an empty child array and empty text. Domain schemas decide whether that represents a valid empty result. An invalid response must remain distinct from a valid empty response before state advances or effects are applied. This reader performs no I/O, persistence, retries or provider requests.
