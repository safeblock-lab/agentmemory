# Frozen release assets

This workflow publishes an existing GitHub draft only after confirming that its assets match the frozen AgentMemory package candidate for the exact pushed tag commit.

## Required draft contents

Before pushing a `vX.Y.Z` tag, create a GitHub draft release for that tag and upload exactly these nine files:

- The seven files listed in `artifacts/agentmemory-vX.Y.Z/package-candidate.json` under `publicAssets`.
- `package-candidate.json`.
- `SHA256SUMS`.

Commit the frozen candidate metadata and checksum file at those paths before creating the tag. The workflow uses them as its trusted expectations. Keep private CUDA runtime archives, database files, caches, credentials, and evaluation data out of the draft.

## What the tag workflow checks

On a `v*` tag push, the workflow confirms that the tag matches the root package version and resolves to the commit that started the workflow. It uses `gh release view TAG --repo REPOSITORY --json databaseId,isDraft,tagName` to resolve the exact tag, requires matching tag metadata and `isDraft: true`, validates the positive release ID, then fetches the REST release by ID. A missing release or invalid lookup response stops the workflow before asset download. The verifier rejects missing or extra files, non-regular files, an unapproved asset name, size differences, altered hashes, mismatched candidate metadata, and checksum-file drift. The public list is limited to the AgentMemory tarball, Qwen model, CPU runtime archive, upstream manifest, model notice, Apache license, and README.

After those checks pass, the workflow changes that same draft to published and confirms that GitHub reports it as published. It does not build the native engine, run npm install/build/pack, regenerate the scorer, create another release, publish to npm, or replace draft assets. If any check fails, the existing draft stays a draft.

## Local checks

Run the focused offline fixtures with:

```sh
npx vitest run test/release-draft-lookup.test.ts
npx vitest run test/release-assets.test.ts
```

The lookup fixtures validate mocked `gh release view` responses for a matching draft, a missing release, a non-draft release, a wrong tag, and malformed IDs. Asset fixtures cover a same-size altered asset, a wrong tag or commit, private CUDA/database/cache files, an unapproved private archive in the candidate list, a missing asset, and a release that is not a draft. They do not call GitHub or require credentials.
