# 0005. Key messages by card_id over 3 partitions

**Status:** Accepted

## Context
Fraud signals such as card-testing bursts depend on the order of one card's transactions. Kafka guarantees order only within a partition.

## Decision
The producer uses `card_id` as the message key, so all of one card's events go to the same partition, in order. The topic has 3 partitions.

## Alternatives considered
- **No key (round-robin).** Spreads load evenly, but one card's events would be split across partitions with no ordering guarantee between them.
- **One partition.** Total order, but no parallelism for consumers.

## Consequences
- Per-card order is preserved, which the downstream scoring job relies on for its velocity features.
- With 200 cards, load is spread evenly. A very active card would load its partition more heavily than the others.
