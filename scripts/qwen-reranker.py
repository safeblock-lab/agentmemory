import argparse
import ctypes
import hashlib
import io
import json
import math
import os
import pathlib
import sys

YES_TOKEN = 9693
NO_TOKEN = 2152
CONTEXT = 4096
MODEL_SHA256 = "22c9979ce4fbcdc5acdc310c6641c32797eff1aa980b8f7a2db8a8ea23429a48"
MAX_LINE = 2_000_000

class StartupError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code

class ModelParams(ctypes.Structure):
    _fields_ = [
        ("devices", ctypes.c_void_p),
        ("tensor_buft_overrides", ctypes.c_void_p),
        ("n_gpu_layers", ctypes.c_int32),
        ("split_mode", ctypes.c_int32),
        ("load_mode", ctypes.c_int32),
        ("lazy_mode", ctypes.c_int32),
        ("main_gpu", ctypes.c_int32),
        ("tensor_split", ctypes.c_void_p),
        ("progress_callback", ctypes.c_void_p),
        ("progress_callback_user_data", ctypes.c_void_p),
        ("kv_overrides", ctypes.c_void_p),
        ("vocab_only", ctypes.c_bool),
        ("check_tensors", ctypes.c_bool),
        ("use_extra_bufts", ctypes.c_bool),
        ("no_host", ctypes.c_bool),
        ("no_alloc", ctypes.c_bool),
        ("load_mtp", ctypes.c_bool),
    ]


class ContextParams(ctypes.Structure):
    _fields_ = [
        ("n_ctx", ctypes.c_uint32),
        ("n_batch", ctypes.c_uint32),
        ("n_ubatch", ctypes.c_uint32),
        ("n_seq_max", ctypes.c_uint32),
        ("n_rs_seq", ctypes.c_uint32),
        ("n_outputs_max", ctypes.c_uint32),
        ("n_outputs_max_per_seq", ctypes.c_uint32),
        ("n_threads", ctypes.c_int32),
        ("n_threads_batch", ctypes.c_int32),
        ("ctx_type", ctypes.c_int32),
        ("rope_scaling_type", ctypes.c_int32),
        ("pooling_type", ctypes.c_int32),
        ("attention_type", ctypes.c_int32),
        ("flash_attn_type", ctypes.c_int32),
        ("rope_freq_base", ctypes.c_float),
        ("rope_freq_scale", ctypes.c_float),
        ("yarn_ext_factor", ctypes.c_float),
        ("yarn_attn_factor", ctypes.c_float),
        ("yarn_beta_fast", ctypes.c_float),
        ("yarn_beta_slow", ctypes.c_float),
        ("yarn_orig_ctx", ctypes.c_uint32),
        ("defrag_thold", ctypes.c_float),
        ("cb_eval", ctypes.c_void_p),
        ("cb_eval_user_data", ctypes.c_void_p),
        ("type_k", ctypes.c_int32),
        ("type_v", ctypes.c_int32),
        ("abort_callback", ctypes.c_void_p),
        ("abort_callback_data", ctypes.c_void_p),
        ("embeddings", ctypes.c_bool),
        ("offload_kqv", ctypes.c_bool),
        ("no_perf", ctypes.c_bool),
        ("op_offload", ctypes.c_bool),
        ("swa_full", ctypes.c_bool),
        ("kv_unified", ctypes.c_bool),
        ("samplers", ctypes.c_void_p),
        ("n_samplers", ctypes.c_size_t),
        ("ctx_other", ctypes.c_void_p),
    ]


class Batch(ctypes.Structure):
    _fields_ = [
        ("n_tokens", ctypes.c_int32),
        ("token", ctypes.POINTER(ctypes.c_int32)),
        ("embd", ctypes.POINTER(ctypes.c_float)),
        ("pos", ctypes.POINTER(ctypes.c_int32)),
        ("n_seq_id", ctypes.POINTER(ctypes.c_int32)),
        ("seq_id", ctypes.POINTER(ctypes.POINTER(ctypes.c_int32))),
        ("logits", ctypes.POINTER(ctypes.c_int8)),
    ]



class Scorer:
    def __init__(self, model, runtime, device):
        self.context = None
        self.model = None
        self.lib = None
        self.dll_directory = None
        self.log_handle = io.StringIO()
        self.gpu_proof = set()
        self.device = device
        try:
            self._initialize(model, runtime)
        except BaseException:
            self.close()
            raise

    def _initialize(self, model, runtime):
        if ctypes.sizeof(ctypes.c_void_p) != 8 or ctypes.sizeof(ModelParams) != 80 or ctypes.sizeof(ContextParams) != 160 or ctypes.sizeof(Batch) != 56:
            raise StartupError("integrity", "Qwen requires 64-bit Python and pinned llama.cpp b11371 ABI.")
        if not model.is_file() or model.stat().st_size != 639153184:
            raise StartupError("integrity", "Provision the pinned 639 MB Qwen GGUF model.")
        digest = hashlib.sha256()
        with model.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != MODEL_SHA256:
            raise StartupError("integrity", "Qwen model SHA256 does not match the pinned release.")
        if not (runtime / "llama.dll").is_file():
            raise StartupError("gpu-unavailable" if self.device == "gpu" else "runtime", "Provision the pinned Windows x64 b11371 runtime DLLs.")
        os.environ["PATH"] = str(runtime) + os.pathsep + os.environ.get("PATH", "")
        self.dll_directory = os.add_dll_directory(str(runtime))
        try:
            self.backend_lib = ctypes.CDLL(str(runtime / "ggml.dll"), use_last_error=True)
            self.backend_base_lib = ctypes.CDLL(str(runtime / "ggml-base.dll"))
            self.lib = ctypes.CDLL(str(runtime / "llama.dll"))
        except OSError as error:
            raise StartupError("gpu-unavailable" if self.device == "gpu" else "runtime", "Pinned runtime dependencies are unavailable.") from error
        self._bind()
        self.log_callback = ctypes.CFUNCTYPE(None, ctypes.c_int, ctypes.c_char_p, ctypes.c_void_p)(self._log)
        self.lib.llama_log_set(self.log_callback, None)
        self.backend_lib.ggml_backend_load.argtypes = [ctypes.c_char_p]
        self.backend_lib.ggml_backend_load.restype = ctypes.c_void_p
        if self.device == "cpu":
            self.backend_lib.ggml_backend_load_all_from_path.argtypes = [ctypes.c_char_p]
            self.backend_lib.ggml_backend_load_all_from_path.restype = None
            self.backend_lib.ggml_backend_load_all_from_path(os.fsencode(runtime))
            self.backend_lib.ggml_backend_dev_count.restype = ctypes.c_size_t
            if not self.backend_lib.ggml_backend_dev_count():
                raise StartupError("runtime", "Pinned compatible CPU backend is unavailable.")
        elif not self.backend_lib.ggml_backend_load(os.fsencode(runtime / "ggml-cpu-x64.dll")):
            raise StartupError("runtime", "Pinned CPU backend is missing or incompatible.")
        if self.device == "gpu":
            self._enable_gpu(runtime)
        self.lib.llama_backend_init()
        params = self.lib.llama_model_default_params()
        params.n_gpu_layers = 99 if self.device == "gpu" else 0
        self.model = self.lib.llama_model_load_from_file(os.fsencode(model), params)
        if not self.model:
            raise StartupError("gpu-allocation" if self.device == "gpu" else "runtime", "Model allocation failed.")
        context = self.lib.llama_context_default_params()
        context.n_ctx, context.n_batch, context.n_ubatch, context.n_seq_max = CONTEXT, 2048, 512, 1
        context.n_threads = context.n_threads_batch = 4
        self.context = self.lib.llama_init_from_model(self.model, context)
        if not self.context:
            raise StartupError("gpu-allocation" if self.device == "gpu" else "runtime", "Full-context allocation failed.")
        self.vocab = self.lib.llama_model_get_vocab(self.model)
        self.vocab_count = self.lib.llama_vocab_n_tokens(self.vocab)
        if self.vocab_count <= max(YES_TOKEN, NO_TOKEN):
            raise StartupError("integrity", "Qwen vocabulary is invalid.")
        self._validate_special_tokenization()
        try:
            self._warm_full_context()
        except RuntimeError as error:
            if self.device == "gpu" and any(word in self.log_handle.getvalue().lower() for word in ("out of memory", "failed to allocate")):
                raise StartupError("gpu-allocation", "Full-context GPU warmup allocation failed.") from error
            raise
        if self.device == "gpu":
            required = ("offloaded 29/29 layers to GPU", "CUDA0 model buffer size", "CUDA0 KV buffer size", "CUDA0 compute buffer size")
            if not all(value in self.gpu_proof for value in required):
                raise StartupError("integrity", "Full GPU model, context and compute offload could not be verified.")
        self.controls = [
            self.score("What is the capital of France?", "Paris is the capital and largest city of France."),
            self.score("What is the capital of France?", "Berlin is the capital and largest city of Germany."),
        ]
        if self.controls[0]["deltaLogitYesMinusNo"] <= 0 or self.controls[1]["deltaLogitYesMinusNo"] >= 0:
            raise StartupError("quality", "Qwen positive/negative startup controls failed.")

    def _enable_gpu(self, runtime):
        cuda = self.backend_lib.ggml_backend_load(os.fsencode(runtime / "ggml-cuda.dll"))
        if not cuda or not self.lib.llama_supports_gpu_offload():
            raise StartupError("gpu-unavailable", "CUDA backend or compatible GPU is unavailable.")
        self.backend_lib.ggml_backend_dev_count.restype = ctypes.c_size_t
        self.backend_lib.ggml_backend_dev_get.argtypes = [ctypes.c_size_t]
        self.backend_lib.ggml_backend_dev_get.restype = ctypes.c_void_p
        self.backend_base_lib.ggml_backend_dev_type.argtypes = [ctypes.c_void_p]
        self.backend_base_lib.ggml_backend_dev_type.restype = ctypes.c_int
        self.backend_base_lib.ggml_backend_dev_memory.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t)]
        eligible = []
        for index in range(self.backend_lib.ggml_backend_dev_count()):
            device = self.backend_lib.ggml_backend_dev_get(index)
            if self.backend_base_lib.ggml_backend_dev_type(device) == 1:
                free, total = ctypes.c_size_t(), ctypes.c_size_t()
                self.backend_base_lib.ggml_backend_dev_memory(device, ctypes.byref(free), ctypes.byref(total))
                eligible.append(free.value)
        # 4096 context, batch 2048/512 and one sequence: measured 1351 MiB plus allocation headroom.
        if not eligible or eligible[0] < 2048 * 1024 * 1024:
            raise StartupError("gpu-allocation", "GPU has less than 2 GiB free for the complete fixed workload.")

    def _log(self, level, message, user_data):
        if message:
            text = message.decode("utf-8", errors="replace")
            normalized = " ".join(text.split())
            for marker in ("offloaded 29/29 layers to GPU", "CUDA0 model buffer size", "CUDA0 KV buffer size", "CUDA0 compute buffer size"):
                if marker in normalized:
                    self.gpu_proof.add(marker)
            if self.log_handle.tell() < 128_000:
                self.log_handle.write(text)
            if level <= 3:
                sys.stderr.write(text[:4096])

    def _bind(self):
        lib = self.lib
        lib.llama_log_set.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        lib.llama_model_default_params.restype = ModelParams
        lib.llama_context_default_params.restype = ContextParams
        lib.llama_backend_init.argtypes = []
        lib.llama_model_load_from_file.argtypes = [ctypes.c_char_p, ModelParams]
        lib.llama_model_load_from_file.restype = ctypes.c_void_p
        lib.llama_model_free.argtypes = [ctypes.c_void_p]
        lib.llama_model_get_vocab.argtypes = [ctypes.c_void_p]
        lib.llama_model_get_vocab.restype = ctypes.c_void_p
        lib.llama_vocab_n_tokens.argtypes = [ctypes.c_void_p]
        lib.llama_vocab_n_tokens.restype = ctypes.c_int32
        lib.llama_init_from_model.argtypes = [ctypes.c_void_p, ContextParams]
        lib.llama_init_from_model.restype = ctypes.c_void_p
        lib.llama_free.argtypes = [ctypes.c_void_p]
        lib.llama_tokenize.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_int32, ctypes.POINTER(ctypes.c_int32), ctypes.c_int32, ctypes.c_bool, ctypes.c_bool]
        lib.llama_tokenize.restype = ctypes.c_int32
        lib.llama_batch_init.argtypes = [ctypes.c_int32, ctypes.c_int32, ctypes.c_int32]
        lib.llama_batch_init.restype = Batch
        lib.llama_batch_free.argtypes = [Batch]
        lib.llama_decode.argtypes = [ctypes.c_void_p, Batch]
        lib.llama_decode.restype = ctypes.c_int32
        lib.llama_get_memory.argtypes = [ctypes.c_void_p]
        lib.llama_get_memory.restype = ctypes.c_void_p
        lib.llama_memory_clear.argtypes = [ctypes.c_void_p, ctypes.c_bool]
        lib.llama_get_logits_ith.argtypes = [ctypes.c_void_p, ctypes.c_int32]
        lib.llama_get_logits_ith.restype = ctypes.POINTER(ctypes.c_float)
        lib.llama_supports_gpu_offload.argtypes = []
        lib.llama_supports_gpu_offload.restype = ctypes.c_bool

    def _tokenize(self, text, add_special=False, parse_special=True):
        encoded = text.encode("utf-8")
        capacity = min(CONTEXT * 2, max(64, len(encoded) + 16))
        while capacity <= CONTEXT * 2:
            tokens = (ctypes.c_int32 * capacity)()
            count = self.lib.llama_tokenize(self.vocab, encoded, len(encoded), tokens, capacity, add_special, parse_special)
            if count >= 0:
                return [tokens[index] for index in range(count)]
            capacity = -count
        raise RuntimeError("Prompt tokenization exceeded the bounded tokenizer capacity.")

    def _validate_special_tokenization(self):
        yes = self._tokenize("yes", False, False)
        no = self._tokenize("no", False, False)
        if yes != [YES_TOKEN] or no != [NO_TOKEN]:
            raise RuntimeError("Expected exact one-token IDs yes=9693/no=2152, got yes={} no={}.".format(yes, no))
        tokens = self._tokenize("<|im_start|>assistant\n<think>\n\n</think>\n\n", False, True)
        if not tokens or tokens[0] != 151644 or tokens.count(151644) != 1:
            raise RuntimeError("Exact Qwen final prompt suffix tokenization has unexpected BOS/chat boundary.")
        self.log_handle.write("yes_token_ids=" + json.dumps(yes) + " no_token_ids=" + json.dumps(no) + "\n")
        self.log_handle.write("prompt_suffix_token_ids=" + json.dumps(tokens) + "\n")

    def _decode_tokens(self, tokens, position_offset=0, emit_last_logits=True):
        if not tokens or len(tokens) > CONTEXT:
            raise ValueError("Token sequence is empty or exceeds the full 4096-token context.")
        for start in range(0, len(tokens), 2048):
            chunk = tokens[start:start + 2048]
            batch = self.lib.llama_batch_init(len(chunk), 0, 1)
            if not batch.token:
                raise RuntimeError("llama_batch_init returned a batch without token storage.")
            try:
                batch.n_tokens = len(chunk)
                for index, token in enumerate(chunk):
                    batch.token[index] = token
                    batch.pos[index] = position_offset + start + index
                    batch.n_seq_id[index] = 1
                    batch.seq_id[index][0] = 0
                    batch.logits[index] = 1 if emit_last_logits and index == len(chunk) - 1 else 0
                result = self.lib.llama_decode(self.context, batch)
                if result != 0:
                    raise RuntimeError("llama_decode returned {} for {} prompt tokens at offset {}".format(result, len(chunk), position_offset + start))
            finally:
                self.lib.llama_batch_free(batch)

    def _warm_full_context(self):
        newline = self._tokenize("\n", False, False)
        if len(newline) != 1:
            raise RuntimeError("Synthetic full-context warmup token must be exactly one newline token.")
        full = [newline[0]] * CONTEXT
        memory = self.lib.llama_get_memory(self.context)
        self.lib.llama_memory_clear(memory, False)
        self._decode_tokens(full, position_offset=0, emit_last_logits=True)
        self.lib.llama_memory_clear(memory, False)
        self.log_handle.write("warmup_full_context_tokens=4096 batch=2048 ubatch=512 concurrency=1\n")

    def score(self, query, document):
        if not isinstance(query, str) or not isinstance(document, str):
            raise ValueError("query and document must be strings")
        if len(query) > 256 or len(document) > 8192:
            raise ValueError("Input exceeds frozen campaign bounds query<=256 chars and document<=8192 chars; no truncation is applied.")
        bounded_query = query
        bounded_document = document
        prompt = (
            "<|im_start|>system\nJudge whether the Document meets the requirements based on the Query and the Instruct provided. Note that the answer can only be \"yes\" or \"no\".<|im_end|>\n"
            "<|im_start|>user\n<Instruct>: Given a web search query, retrieve relevant passages that answer the query.\n"
            "<Query>: {}\n<Document>: {}<|im_end|>\n"
            "<|im_start|>assistant\n<think>\n\n</think>\n\n"
        ).format(bounded_query, bounded_document)
        tokens = self._tokenize(prompt, False, True)
        if len(tokens) > CONTEXT:
            raise ValueError("Complete Qwen scoring prompt is {} tokens, above context 4096; truncation is forbidden.".format(len(tokens)))
        self.lib.llama_memory_clear(self.lib.llama_get_memory(self.context), False)
        self._decode_tokens(tokens, position_offset=0, emit_last_logits=True)
        logits = self.lib.llama_get_logits_ith(self.context, -1)
        if not logits:
            raise RuntimeError("llama_get_logits_ith(ctx, -1) returned NULL")
        yes = float(logits[YES_TOKEN])
        no = float(logits[NO_TOKEN])
        if not math.isfinite(yes) or not math.isfinite(no):
            raise RuntimeError("Direct final-prompt yes/no logits are not finite.")
        delta = yes - no
        score = 1.0 / (1.0 + math.exp(-max(-700.0, min(700.0, delta))))
        return {"yesTokenId": YES_TOKEN, "noTokenId": NO_TOKEN, "yesLogit": yes, "noLogit": no, "deltaLogitYesMinusNo": delta, "score": score, "promptTokens": len(tokens), "queryCharacters": len(query), "documentCharacters": len(document), "truncationApplied": False}


    def close(self):
        if self.context:
            self.lib.llama_free(self.context)
            self.context = None
        if self.model:
            self.lib.llama_model_free(self.model)
            self.model = None
        if self.dll_directory:
            self.dll_directory.close()
            self.dll_directory = None

def emit(value):
    print(json.dumps(value, separators=(",", ":")), flush=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--device", choices=("gpu", "cpu"), required=True)
    args = parser.parse_args()
    scorer = None
    try:
        scorer = Scorer(pathlib.Path(args.model), pathlib.Path(args.runtime), args.device)
        emit({"ready": True, "device": args.device, "context": CONTEXT, "gpuVerified": args.device == "gpu", "controls": scorer.controls})
        while True:
            line = sys.stdin.buffer.readline(MAX_LINE + 1)
            if not line:
                break
            if len(line) > MAX_LINE or not line.endswith(b"\n"):
                raise ValueError("IPC input exceeds bounded request size.")
            request = json.loads(line)
            if request.get("shutdown") is True:
                break
            pairs = request.get("pairs")
            if not isinstance(pairs, list) or not 1 <= len(pairs) <= 50:
                raise ValueError("Score batch must contain 1..50 pairs.")
            try:
                scores = [scorer.score(pair["query"], pair["document"])["score"] for pair in pairs]
                emit({"id": request.get("id"), "scores": scores})
            except Exception:
                emit({"id": request.get("id"), "error": "Qwen scoring failed; check input and context bounds."})
    except Exception as error:
        emit({"error": str(error) if isinstance(error, StartupError) else "Qwen runtime failed.", "code": getattr(error, "code", "runtime")})
        return 1
    finally:
        if scorer:
            scorer.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
