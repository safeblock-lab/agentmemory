# Complete native embedding equality census

Global native vector traversal completed over 232,592 rows and 357,261,312 raw embedding bytes; the reader then closed. All records belonged to epoch 1 during this later interval. Earlier vector counts differ because the source was live.

SHA256 grouping found 219,860 unique groups. Every one of 12,732 duplicate hits was byte-compared with its representative and the current record in a single short read snapshot. No representative/current drift or hash collisions were observed. Duplicate raw bytes: 19,556,352, or about 5.47%. Unique float32 payload: 337,704,960 bytes before reference maps, record metadata and search indexes.

Lossless zlib level 6 produced 344,048,043 per-vector bytes. Framed 1 MiB chunks produced 330,765,028 bytes before deduplication. These alternatives overlap with deduplication and cannot be added. No float quantization, re-embedding, dimension reduction or model replacement was performed.

Resource observations: 228 batches of at most 1,024 vectors, approximately 1.5 MiB each; peak working set 124,096,512 bytes; four bounded SQL retries; maximum fetch call 7.172 seconds including closed/reopened retry attempts; total duration 123.84 seconds. Data version changed 3 to 39; WAL stayed 17,287,552 bytes. This is full temporal traversal and exact duplicate proof at comparison time, not a frozen global content cut.

Vector duplication alone cannot support a tenfold database reduction. Its measured raw duplicate saving is about 19.6 MB, whereas the separately measured WITHOUT ROWID overflow-page layout wastes roughly 689 MB inside pages. A shared canonical/native float32 authority and a different native record layout remain distinct candidate improvements; their joint final size must be measured before deployment.
