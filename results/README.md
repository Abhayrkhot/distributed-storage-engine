# Benchmark data

Raw JSON lines written by `scripts/bench-*.sh`, one object per run. They are
rendered by `scripts/report.py`.

| Directory | Host | Rendered as |
|---|---|---|
| `dedicated/` | Exclusive HPC node: 2× Xeon Platinum 8276 (112 threads, 2.2 GHz), Dell BOSS SATA SSD, RHEL 9. Run in the `docker/bench.Dockerfile` image via Apptainer, using `scripts/slurm/kvbench.sbatch`. | [docs/results.md](../docs/results.md) |
| `shared-box/` | i7-12700H (20 threads, up to 4.7 GHz), SK hynix PC801 NVMe, Ubuntu. A shared development machine: other projects' clusters and databases were flushing the same disk. | [docs/results-shared-box.md](../docs/results-shared-box.md) |

The two hosts stress different things:

- **The dedicated node** has fast, uncontended flushes (138 µs per 4 KiB
  fdatasync) but slow cores. Its numbers are clean for anything flush-bound:
  blocking writes, the lock-contention comparison, failover.
- **The shared box** has cores about twice as fast, so CPU-bound work (recovery,
  the pipelined write path, gRPC request handling) does better there. Its
  flushes were noisy, though, because other tenants were writing to the same
  disk.

Regenerate the documents with:

```bash
python3 scripts/report.py --raw results/dedicated --out docs/results.md --title "Results: dedicated server"
python3 scripts/report.py --raw results/shared-box --out docs/results-shared-box.md --title "Results: shared development machine"
```
