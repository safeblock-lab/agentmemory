# Qwen3 reranker model notice

The optional model asset is the Q8_0 GGUF from
[`ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF`](https://huggingface.co/ggml-org/Qwen3-Reranker-0.6B-Q8_0-GGUF),
revision `a02f48bb4f057028298c21fa033da2b30d7742d5`, file
`qwen3-reranker-0.6b-q8_0.gguf`, SHA-256
`22c9979ce4fbcdc5acdc310c6641c32797eff1aa980b8f7a2db8a8ea23429a48`.
The upstream model card identifies the model license as Apache-2.0; the license
text is included in [Apache-2.0.txt](licenses/Apache-2.0.txt).

This GGUF is a separate model asset, not part of the AgentMemory npm tarball.
The release candidate carries it as a separately hashed download with this
notice and license. The CPU runtime ZIP contains the upstream llama.cpp MIT
license and its included LLVM OpenMP notice. Optional CUDA runtime binaries
are acquired from the official pinned upstream URLs in
[qwen-gpu-upstream-assets.json](qwen-gpu-upstream-assets.json) and are not
redistributed by AgentMemory.
