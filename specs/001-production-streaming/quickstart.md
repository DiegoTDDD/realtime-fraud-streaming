# Quickstart: verify the feature

1. `docker compose up -d` and create the topic (`rpk topic create transactions -p 3`).
2. `python contract/registry.py register` — registers the schema with BACKWARD compatibility.
3. `python spark/streaming_job.py`, then `python producer/producer.py`.
4. `python spark/health.py` — exits 0 while the job runs.
5. Stop the job, restart it; `python spark/maintenance.py --check-duplicates` reports 0 duplicates.
6. `python spark/maintenance.py` — files before and after per table.
7. `python contract/registry.py check tests/fixtures/incompatible.schema.json` — rejected.
8. Stop the job; `python spark/health.py` exits 1 with "stale progress".
