| Strategy | Guarantees | GoBench | Distributed | Avg Runs | Worst | ms/run | Total ms |
|----------|-----------|---------|-------------|----------|-------|--------|----------|
| CHESS (k=3) | Complete to bound k | 11/14 | -- | 8.5 | 74 | 1.39 | 153 |
| CHESS G+L (k=2) | Complete to bound k | -- | 1/1 | 27.0 | 27 | 1.41 | 38 |
| CHESS G-only (k=2) | Complete to bound k | -- | 0/1 | -- | -- | 1.36 | 621 |
| PCT (d=2) | Probabilistic | 11/14 | 1/1 | 3.8 | 26 | 1.57 | 2418 |
| PCT (d=3) | Probabilistic | 11/14 | 1/1 | 4.4 | 26 | 0.28 | 436 |
| Random | None | 11/14 | 1/1 | 1.8 | 8 | 0.06 | 88 |

**etcd#7443 seed sweep (100 seeds) — the hard bug:**

| Strategy | Seed | Runs to Bug | Est. Time |
|----------|------|-------------|-----------|
| CHESS (k=3) | deterministic | 74 | 1.9ms |
| Random | best | 1 | 0.0ms |
| Random | median | 30 | 0.8ms |
| Random | worst | 111 | 2.9ms |
| PCT(d=2) | best | 1 | 0.0ms |
| PCT(d=2) | median | 33 | 1.0ms |
| PCT(d=2) | worst | 83 | 2.4ms |
| PCT(d=3) | best | 1 | 0.0ms |
| PCT(d=3) | median | 33 | 0.8ms |
| PCT(d=3) | worst | 83 | 1.9ms |
