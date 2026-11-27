# realtime-fraud-streaming Constitution

## Core Principles

### I. One contract, enforced at every hop
The event contract (`contract/transaction.py`) is the single source of truth. The producer, the
schema registry and the Spark job derive their rules from it; a test fails if any of them drifts.
An event that breaks the contract is quarantined with its reasons, never dropped silently and
never counted in Gold.

### II. Correct under failure
Every sink must give the same result if the job is killed and restarted at any point: no event
lost, none written twice. Claims of exactly-once are demonstrated by a test that kills the job,
not asserted in prose.

### III. Tested without infrastructure, proven with it
Transformations, the contract and the monitoring rules are unit-tested on a local SparkSession
with no Kafka or Docker. The full path (Redpanda, schema registry, Spark, storage, dashboard) runs
in CI on every relevant push, following the README's own commands.

### IV. Operable by someone else
A streaming job is only done when its health can be read without opening the code: progress
metrics are recorded, a health check states pass or fail against written thresholds, and an
alert can reach a person. Each operational decision has an ADR.

### V. Honest limits
The README states what the project does not do. Numbers in documentation come from runs, not
estimates.

## Constraints
Free to run: local Docker (Redpanda), local Spark, GitHub Actions. No paid cloud service. Python
3.11, Spark 3.5. The public dashboard serves a committed sample (ADR 0006).

## Development Workflow
Spec Kit: constitution, specify, clarify, plan, tasks, implement. Each feature lives in
`specs/<nnn>-<name>/`. Lint (ruff) and unit tests gate every push; the pipeline check gates
changes to the job, producer, contract or compose file.

## Governance
This constitution overrides habits. Changing a principle needs an ADR that says why.

**Version**: 1.0.0 | **Ratified**: 2026-11-26 | **Last Amended**: 2026-11-26
