# Shopping Sessions

Hand-written parallel Rust pipeline: mmap'd CSVs, strict dialect parsing, radix-partitioned
dedup and person routing, per-person sessionizing and click attribution, Parquet output.

Build: `docker build -t shopping .`. Entry point: `/app/shopping` (reads `DATA_DIR`, `OUTPUT_DIR`, `NCPU`).
