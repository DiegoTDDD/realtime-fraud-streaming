"""The event contract, and the producer's events against it."""

import pytest
from conftest import event, without

import producer
from contract.transaction import FRAUD_TYPES, validate_event


def test_a_legitimate_and_a_fraudulent_event_are_valid():
    assert validate_event(event()) == []
    assert validate_event(event(is_fraud=True, fraud_type="card_testing", amount=1)) == []


@pytest.mark.parametrize(
    ("bad", "reasons"),
    [
        (without("card_id"), ["invalid:card_id"]),
        (event(amount=None), ["invalid:amount"]),
        (event(amount="12.5"), ["invalid:amount"]),
        (event(amount=True), ["invalid:amount"]),  # a JSON true is not a number
        (event(is_fraud="no"), ["invalid:is_fraud"]),
        (event(card_id=123), ["invalid:card_id"]),
        (event(timestamp="yesterday"), ["bad_timestamp"]),
        (event(amount=0), ["amount_not_positive"]),
        (event(amount=-5), ["amount_not_positive"]),
        (event(is_fraud=True, fraud_type="phishing"), ["unknown_fraud_type"]),
        (event(is_fraud=True), ["label_mismatch"]),
        (event(fraud_type="high_amount"), ["label_mismatch"]),
        (
            event(amount=-1, timestamp="nope", card_id=None),
            ["invalid:card_id", "bad_timestamp", "amount_not_positive"],
        ),
    ],
)
def test_each_rule_reports_its_reason(bad, reasons):
    assert validate_event(bad) == reasons


def test_a_timestamp_without_timezone_is_accepted_as_utc():
    assert validate_event(event(timestamp="2026-01-01T10:00:30")) == []


def test_every_generated_event_meets_the_contract():
    producer.random.seed(1)
    producer._active_bursts.clear()
    events = [producer.next_transaction() for _ in range(3000)]
    assert all(validate_event(e) == [] for e in events)
    assert {e["fraud_type"] for e in events if e["is_fraud"]} == set(FRAUD_TYPES)


def test_generated_fraud_patterns_follow_their_definitions():
    producer.random.seed(2)
    producer._active_bursts.clear()
    events = [producer.next_transaction() for _ in range(3000)]
    homes = {c["card_id"]: c["home_city"] for c in producer.CARDS}
    for e in events:
        if e["fraud_type"] == "high_amount":
            assert e["amount"] >= 5000 and e["city"] == homes[e["card_id"]]
        elif e["fraud_type"] == "card_testing":
            assert e["amount"] <= 5.0 and e["category"] == "entertainment"
        elif e["fraud_type"] == "impossible_travel":
            assert e["city"] != homes[e["card_id"]]


def test_card_testing_bursts_stay_on_one_card():
    producer._active_bursts.clear()
    producer._active_bursts["card_0007"] = 3
    burst = [producer.next_transaction() for _ in range(3)]
    assert {e["card_id"] for e in burst} == {"card_0007"}
    assert all(e["fraud_type"] == "card_testing" for e in burst)
    assert not producer._active_bursts
