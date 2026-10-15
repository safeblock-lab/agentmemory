# Complete ordered provenance-list census and representation model

The pass traversed all 76,069 nodes and 121,451 edges, with no invalid lists, codec errors or retries. It completed in 51.16 seconds with peak working set 99,643,392 bytes and closed its reader. Source remained live: data version changed 3 to 72; WAL stayed 17,287,552 bytes.

There are 74,197,800 ordered ID occurrences: 37,766,686 in nodes and 36,431,114 in edges. Node occurrences exactly match the native membership count at the associated interval. No list contains a duplicate ID. The dictionary contains 96,798 unique IDs, each 25 UTF-8 bytes. Maximum list lengths were 47,098 for nodes and 14,361 for edges.

The full global fingerprint census identifies 39,299 ordered-list groups, 30,208 used by nodes and 18,679 by edges, with 9,588 shared across both. Equivalence preserves string bytes, order, case and multiplicity; it does not sort sets or merge logical graph identities. Globally unique list groups contain 28,092,497 item incidences.

Canonical arrays total 2,077,735,920 bytes; unique-group canonical arrays total 786,629,215 bytes. Their 1,291,106,705-byte difference is a complete fingerprint candidate measure. The separate 32 MiB/80,000-entry byte-proof cache confirms 148,630 repeated occurrences and 1,189,515,606 repeated bytes, a strict lower bound; 40,470 cache evictions prevent claiming global byte proof. An actual encoder must compare content before reuse and preserve collision handling.

## Fixed-width representation model

| Model component | Derivation | Bytes |
| --- | --- | ---: |
| Ordered list items | 28,092,497 × 3-byte dictionary IDs | 84,277,491 |
| UTF-8 ID dictionary with lengths | 96,798 × (25 + 4) | 2,807,142 |
| List offsets | 39,299 × 8 | 314,392 |
| Row-to-list references | 197,520 × 4 | 790,080 |
| Total | Components above | 88,189,105 |

This is a data representation model, not a built native database. It excludes reverse indexes, native page/header overhead, liveness and transaction metadata, original JSON-format reconstruction metadata, key/version/order fields and rollback storage. The 3-byte ID domain supports all 96,798 observed dictionary entries; dictionary overflow must have an explicit future encoding rather than truncation.

Nodes and edges currently have 973,588,034 stored BLOB bytes and 43,619,512 decoded bytes outside these arrays. Those bytes remain necessary and must be included in any candidate. The native membership table and reverse index occupy an independently measured 1.176 GB; a joint design must avoid storing both shared lists and the same expanded pair corpus again. Query direction, insertion order, pagination, IDs, provenance, generations and source versions must remain compatible.

The model demonstrates a strong architectural opportunity, not a tenfold whole-database result. It overlaps both the field-sharing lower bounds and chunk-compression experiments. A coherent StateModule-owned shadow build is required to measure the combined physical result and decoded tuple parity.
