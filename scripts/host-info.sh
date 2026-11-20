#!/usr/bin/env bash
# Records the benchmark host (CPU, memory, disk, kernel, raw fdatasync
# latency) into results/raw/host.json. Run inside the dev container.
set -euo pipefail
OUT=${OUT:-results/raw/host.json}
DATA=${DATA:-/data}
mkdir -p "$(dirname "$OUT")"
python3 - "$OUT" "$DATA" <<'EOF'
import json, os, platform, subprocess, sys, time
out, data = sys.argv[1], sys.argv[2]
def first(path, key):
    try:
        for line in open(path):
            if line.startswith(key):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "?"
def probe(overwrite, n=1000):
    p = os.path.join(data, "fsync-probe.dat")
    fd = os.open(p, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    buf = b"x" * 4096
    if overwrite:
        for i in range(n):
            os.write(fd, buf)
        os.fsync(fd)
        os.lseek(fd, 0, 0)
    lat = []
    for i in range(n):
        os.write(fd, buf)
        t = time.perf_counter(); os.fdatasync(fd); lat.append(time.perf_counter() - t)
    os.close(fd); os.unlink(p)
    lat.sort()
    return f"p50 {lat[n//2]*1e6:.0f} µs, p99 {lat[n*99//100]*1e6:.0f} µs"
disk = "?"
try:
    disk = open("/sys/block/nvme0n1/device/model").read().strip()
except OSError:
    pass
info = {
    "cpu": first("/proc/cpuinfo", "model name"),
    "cores": os.cpu_count(),
    "memory": first("/proc/meminfo", "MemTotal"),
    "disk": disk,
    "kernel": platform.release(),
    "fdatasync (4 KiB append)": probe(False),
    "fdatasync (4 KiB overwrite)": probe(True),
    "load average at capture": " ".join(f"{x:.2f}" for x in os.getloadavg()),
    "note": os.environ.get("HOST_NOTE", ""),
}
json.dump(info, open(out, "w"), indent=1)
print(json.dumps(info, indent=1))
EOF
