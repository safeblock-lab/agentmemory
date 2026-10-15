import type { IIIClient } from "iii-sdk";
import { KV } from "../state/schema.js";
import { StateKV } from "../state/kv.js";
import { touchImage } from "../utils/image-store.js";
import { withKeyedLock } from "../state/keyed-mutex.js";

export async function getImageRefCount(kv: StateKV, filePath: string): Promise<number> {
  const count = await kv.get<number>(KV.imageRefs, filePath);
  if (count === null) return 0;
  if (!Number.isSafeInteger(count) || count < 0) throw new Error("IMAGE_REF_COUNT_INVALID");
  return count;
}

export async function incrementImageRef(kv: StateKV, filePath: string): Promise<void> {
  return withKeyedLock(`imgRef:${filePath}`, async () => {
    const current = await getImageRefCount(kv, filePath);
    await kv.set(KV.imageRefs, filePath, current + 1);
    await touchImage(filePath);
  });
}

export async function decrementImageRef(kv: StateKV, _sdk: IIIClient, filePath: string): Promise<void> {
  return withKeyedLock(`imgRef:${filePath}`, async () => {
    const current = await getImageRefCount(kv, filePath);
    // A zero refcount does not prove absence of replay, version, or rollback roots.
    // Preserve source bytes and embeddings until an authorized reachability collector owns reclamation.
    if (current > 0) await kv.set(KV.imageRefs, filePath, current - 1);
  });
}
