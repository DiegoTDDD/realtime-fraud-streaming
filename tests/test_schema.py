"""The registered schema is generated from the contract and cannot drift from it."""

import json

import jsonschema
import pytest
from conftest import ROOT, event

from contract.transaction import FRAUD_TYPES, REQUIRED, SCHEMA_PATH, json_schema, validate_event


def test_the_committed_schema_is_the_one_the_contract_generates():
    committed = json.loads(SCHEMA_PATH.read_text())
    assert committed == json_schema(), (
        "contract/transactions-value.schema.json is out of date: regenerate it with "
        'python -c "import json; from contract.transaction import json_schema, SCHEMA_PATH; '
        "SCHEMA_PATH.write_text(json.dumps(json_schema(), indent=2) + '\\n')\""
    )


def test_schema_lists_the_required_fields_and_fraud_types():
    schema = json_schema()
    assert schema["required"] == REQUIRED
    assert schema["properties"]["fraud_type"]["enum"] == [*FRAUD_TYPES, None]


@pytest.mark.parametrize(
    "case",
    [
        event(),
        event(is_fraud=True, fraud_type="card_testing", amount=1),
        event(amount=0),
        event(amount="12"),
        event(fraud_type="phishing", is_fraud=True),
        {k: v for k, v in event().items() if k != "card_id"},
    ],
)
def test_schema_rejects_what_the_contract_rejects_for_field_rules(case):
    """Every field-level rule is in both; the cross-field rule (label_mismatch) is only in code."""
    schema_ok = jsonschema.Draft7Validator(json_schema()).is_valid(case)
    field_reasons = [r for r in validate_event(case) if r != "label_mismatch"]
    assert schema_ok == (not field_reasons)


def test_the_incompatible_fixture_really_changes_a_type():
    bad = json.loads((ROOT / "tests" / "fixtures" / "incompatible.schema.json").read_text())
    assert bad["properties"]["amount"]["type"] == "string"
