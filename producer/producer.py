"""
Real-time card transaction producer.

Simulates a payment system emitting card transactions to a Redpanda (Kafka)
topic. The vast majority of events are legitimate; roughly 2% are injected
frauds of three classic types (high amount, card testing bursts, impossible
travel). Each event carries a ground-truth label (`is_fraud`, `fraud_type`)
so a downstream ML model (Project 6) can be trained and evaluated, while the
streaming layer detects fraud purely from behavioral patterns.

Usage:
    python producer/producer.py
Stop with Ctrl+C.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = "localhost:19092"   # Redpanda OUTSIDE listener
TOPIC = "transactions"
EVENTS_PER_SECOND = 10                   # base throughput
FRAUD_RATE = 0.02                        # ~2% of events are fraudulent

fake = Faker()
random.seed(42)
Faker.seed(42)

# A fixed population of cards, each "anchored" to a home city. Legitimate
# transactions usually happen in or near the home city; impossible-travel
# fraud deliberately violates this.
CITIES = [
    "Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba",
    "Porto Alegre", "Salvador", "Recife", "Fortaleza", "Brasilia", "Manaus",
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


def delivery_report(err, msg):
    """Called once per message to report success or failure."""
    if err is not None:
        print(f"  ! delivery failed: {err}")


def make_legit_transaction() -> dict:
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_fraud": False,
        "fraud_type": None,
    }


def make_fraud_transaction() -> dict:
    """Produce one of three classic fraud patterns."""
    fraud_type = random.choice(["high_amount", "card_testing", "impossible_travel"])
    card = random.choice(CARDS)
    base = {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card["card_id"],
        "merchant": fake.company(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_fraud": True,
        "fraud_type": fraud_type,
    }

    if fraud_type == "high_amount":
        category = random.choice(list(CATEGORIES.keys()))
        base.update({
            "amount": round(random.uniform(5000, 15000), 2),
            "category": category,
            "city": card["home_city"],
        })

    elif fraud_type == "card_testing":
        # Small "probe" charge; also schedule a burst of follow-ups.
        _active_bursts[card["card_id"]] = random.randint(5, 12)
        base.update({
            "amount": round(random.uniform(0.5, 5.0), 2),
            "category": "entertainment",
            "city": card["home_city"],
        })

    else:  # impossible_travel
        far_city = random.choice([c for c in CITIES if c != card["home_city"]])
        base.update({
            "amount": round(random.uniform(100, 1500), 2),
            "category": random.choice(list(CATEGORIES.keys())),
            "city": far_city,
        })

    return base


def make_burst_transaction(card_id: str) -> dict:
    """Follow-up probe charge for an ongoing card-testing burst."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "card_id": card_id,
        "amount": round(random.uniform(0.5, 5.0), 2),
        "merchant": fake.company(),
        "category": "entertainment",
        "city": random.choice(CITIES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "is_fraud": True,
        "fraud_type": "card_testing",
    }


def next_transaction() -> dict:
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


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    frauds = 0
    print(f"Producing to '{TOPIC}' at ~{EVENTS_PER_SECOND}/s. Ctrl+C to stop.")
    try:
        while True:
            tx = next_transaction()
            # Key by card_id so all events for a card land in the same
            # partition (preserves per-card ordering for the consumer).
            producer.produce(
                TOPIC,
                key=tx["card_id"],
                value=json.dumps(tx),
                callback=delivery_report,
            )
            producer.poll(0)
            sent += 1
            if tx["is_fraud"]:
                frauds += 1
            if sent % 50 == 0:
                print(f"  sent={sent}  frauds={frauds}  ({frauds/sent:.1%})")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nStopping. Flushing... total sent={sent}, frauds={frauds}")
        producer.flush(10)


if __name__ == "__main__":
    main()
