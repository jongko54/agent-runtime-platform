"""Bounded JSON Schema validation without reference retrieval."""

import json
from typing import Any, Protocol, cast

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

from agent_platform.application.errors import InvalidInput


class _PayloadValidator(Protocol):
    def is_valid(self, instance: object) -> bool: ...


def _reject_references(value: Any) -> None:
    if isinstance(value, dict):
        for key, child in cast(dict[str, Any], value).items():
            if key in {"$ref", "$dynamicRef", "$recursiveRef"}:
                raise ValueError("Schema references are not supported")
            _reject_references(child)
    elif isinstance(value, list):
        for child in cast(list[Any], value):
            _reject_references(child)


def validate_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Validate registry schemas before publishing them; no local or remote refs."""
    try:
        json.dumps(schema, allow_nan=False)
        _reject_references(schema)
        if (
            "$schema" in schema
            and schema["$schema"] != "https://json-schema.org/draft/2020-12/schema"
        ):
            raise ValueError("Unsupported JSON Schema dialect")
        Draft202012Validator.check_schema(schema)
    except (SchemaError, TypeError, ValueError, RecursionError):
        raise ValueError("Invalid or unsupported JSON Schema") from None
    return schema


def validate_payload(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    """Never include rejected fields, values or schema internals in errors."""
    try:
        validate_schema(schema)
        json.dumps(payload, allow_nan=False)
        # jsonschema's deprecated _schema overload lacks complete type annotations.
        validator = cast(_PayloadValidator, Draft202012Validator(schema))
        if not validator.is_valid(payload):
            raise ValueError("Invalid payload")
    except (TypeError, ValueError, RecursionError):
        raise InvalidInput("Payload does not satisfy the registered schema") from None
