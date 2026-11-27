"""
Real-time card transaction producer.

Simulates a payment system emitting card transactions to a Redpanda (Kafka)
topic. The vast majority of events are legitimate; roughly 2% are injected
frauds of three classic types (high amount, card testing bursts, impossible
travel). Each event carries a ground-truth label (`is_fraud`, `fraud_type`)
so a downstream ML model (Project 6) can be trained and evaluated, while the
streaming layer detects fraud purely from behavioral patterns.

Every event is checked against the shared contract (contract/transaction.py)
before it is sent; an event that breaks it is logged and dropped, so a bug in
the generator never reaches the topic. Events are serialized through the schema
registry (ADR 0008): each message carries the registered schema's id, and the
serializer validates the event against that schema. Register it first with
`python contract/registry.py register`.

Usage:
    python producer/producer.py
Stop with Ctrl+C.
"""

import logging
import os
import random
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from faker import Faker

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from contract.transaction import SCHEMA_PATH, validate_event  # noqa: E402

log = logging.getLogger(__name__)
Transaction = dict[str, Any]

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:19092"
)  # Redpanda OUTSIDE listener
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "transactions"
EVENTS_PER_SECOND = 10  # base throughput
FRAUD_RATE = 0.02  # ~2% of events are fraudulent

fake = Faker()
random.seed(42)
Faker.seed(42)

# A fixed population of cards, each "anchored" to a home city. Legitimate
# transactions usually happen in or near the home city; impossible-travel
# fraud deliberately violates this.
CITIES = [
    "Sao Paulo",
    "Rio de Janeiro",
    "Belo Horizonte",
    "Curitiba",
    "Porto Alegre",
    "Salvador",
    "Recife",
    "Fortaleza",
    "Brasilia",
    "Manaus",
]

CATEGORIES = {
    "groceries": (10, 120),
    "restaurant": (15, 200),
    "electronics": (100, 2000),
    "fashion": (30, 600),
    "travel": (200, 3000),
    "fuel": (40, 300),
    "pharmacy": (10, 150),
    "entertainment": (20, 250),
}

NUM_CARDS = 200
CARDS = [
    {
        "card_id": f"card_{i:04d}",
        "home_city": random.choice(CITIES),
    }
    for i in range(NUM_CARDS)
]

# Cards currently in a "card-testing" burst: card_id -> remaining burst count
_active_bursts: dict[str, int] = {}


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Called once per message to report success or failure."""
    if err is not None:
        log.error("delivery failed: %s", err)


def make_legit_transaction() -> Transaction:
    card = random.choice(CARDS)
    category = random.choice(list(CATEGORIES.keys()))
    low, high = CATEGORIES[category]
    amount = round(random.uniform(low, high), 2)
    # 85% of the time the purchase is in the card's home city.
    city = card["home_city"] if random.random() < 0.85 else random.choice(CITIES)
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "amount": amount,
        "merchant": fake.company(),
        "category": category,
        "city": city,
        "timestamp": datetime.now(UTC).isoformat(),
        "is_fraud": False,
        "fraud_type": None,
    }


def make_fraud_transaction() -> Transaction:
    """Produce one of three classic fraud patterns."""
    fraud_type = random.choice(["high_amount", "card_testing", "impossible_travel"])
    card = random.choice(CARDS)
    base: Transaction = {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "merchant": fake.company(),
        "timestamp": datetime.now(UTC).isoformat(),
        "is_fraud": True,
        "fraud_type": fraud_type,
    }

    if fraud_type == "high_amount":
        category = random.choice(list(CATEGORIES.keys()))
        base.update(
            {
                "amount": round(random.uniform(5000, 15000), 2),
                "category": category,
                "city": card["home_city"],
            }
        )

    elif fraud_type == "card_testing":
        # Small "probe" charge; also schedule a burst of follow-ups.
        _active_bursts[card["card_id"]] = random.randint(5, 12)
        base.update(
            {
                "amount": round(random.uniform(0.5, 5.0), 2),
                "category": "entertainment",
                "city": card["home_city"],
            }
        )

    else:  # impossible_travel
        far_city = random.choice([c for c in CITIES if c != card["home_city"]])
        base.update(
            {
                "amount": round(random.uniform(100, 1500), 2),
                "category": random.choice(list(CATEGORIES.keys())),
                "city": far_city,
            }
        )

    return base


def make_burst_transaction(card_id: str) -> Transaction:
    """Follow-up probe charge for an ongoing card-testing burst."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card_id,
        "amount": round(random.uniform(0.5, 5.0), 2),
        "merchant": fake.company(),
        "category": "entertainment",
        "city": random.choice(CITIES),
        "timestamp": datetime.now(UTC).isoformat(),
        "is_fraud": True,
        "fraud_type": "card_testing",
    }


def next_transaction() -> Transaction:
    # Drain any active card-testing bursts first.
    if _active_bursts:
        card_id = next(iter(_active_bursts))
        _active_bursts[card_id] -= 1
        if _active_bursts[card_id] <= 0:
            del _active_bursts[card_id]
        return make_burst_transaction(card_id)

    if random.random() < FRAUD_RATE:
        return make_fraud_transaction()
    return make_legit_transaction()


def build_serializer() -> JSONSerializer:
    """Serializer bound to the registered schema. It does not register schemas itself: a schema
    change goes through contract/registry.py, where its compatibility is checked first."""
    registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    return JSONSerializer(SCHEMA_PATH.read_text(), registry, conf={"auto.register.schemas": False})


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    serializer = build_serializer()
    context = SerializationContext(TOPIC, MessageField.VALUE)
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    frauds = 0
    log.info("Producing to '%s' at ~%d/s. Ctrl+C to stop.", TOPIC, EVENTS_PER_SECOND)
    try:
        while True:
            tx = next_transaction()
            problems = validate_event(tx)
            if problems:
                log.error("dropped an event that breaks the contract %s: %s", problems, tx)
                continue
            # Key by card_id so all events for a card land in the same
            # partition (preserves per-card ordering for the consumer).
            producer.produce(
                TOPIC,
                key=tx["card_id"],
                value=serializer(tx, context),
                callback=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if tx["is_fraud"]:
                frauds += 1
            if sent % 50 == 0:
                log.info("  sent=%d  frauds=%d  (%.1f%%)", sent, frauds, 100 * frauds / sent)
            time.sleep(interval)
    except KeyboardInterrupt:
        log.info("Stopping. Flushing... total sent=%d, frauds=%d", sent, frauds)
        producer.flush(10)


if __name__ == "__main__":
    main()
