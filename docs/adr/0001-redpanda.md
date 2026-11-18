# 0001. Redpanda as the Kafka-compatible broker

**Status:** Accepted

## Context
The pipeline needs a durable, partitioned log that Spark can read with its standard Kafka connector. It must run on a laptop (Windows with Docker Desktop) and on free CI runners.

## Decision
Use Redpanda in a single Docker container, plus Redpanda Console for inspecting messages. Clients talk to it with the Kafka protocol on port 19092.

## Alternatives considered
- **Apache Kafka.** The reference implementation, but heavier to run locally (JVM, KRaft or ZooKeeper configuration).
- **A managed service (Confluent Cloud, MSK).** Costs money and needs credentials. The portfolio runs locally at no cost.
- **No broker (Spark reading files or a socket).** Loses partitioning, replay from offsets and the decoupling between producer and consumer that the project is meant to show.

## Consequences
- One `docker compose up` starts the broker and the console.
- The code only uses the Kafka API. The Windows CI job runs the same pipeline against Apache Kafka, because GitHub's Windows runners cannot run Linux containers, and needs no code change.
