"""Money and invoice lines (ADR 0012).

An invoice is a **set of lines**, not worked-days × day-rate with decorations: some invoices
carry no worked days at all. `Line` is therefore the unit every producing stage contributes,
and a producer contributing *zero* lines is ordinary rather than exceptional.

Amounts are `Decimal`. The prior art carried them as strings through a key-value store and
re-parsed them with `parseFloat(String(v).replace(',', '.'))` — and `String.replace` with a
string argument replaces only the **first** occurrence, so `1,234.50` became `1.234.50` and
then `1.234`. A €1,234.50 expense silently became €1.23. Floats would reintroduce a subtler
version of the same class of bug, so nothing here touches one.
"""

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

# Symbols and spacers a human might type into an amount field; stripped before parsing.
_NOISE = re.compile(r"[\s  €$£]")
_SEPARATORS = (".", ",")


class AmbiguousAmountError(ValueError):
    """Raised when an amount string could mean two different numbers.

    Refusing to guess is the whole point: `1,234` is either 1234 or 1.234 depending on
    locale, and the failure mode of guessing wrong on money is silent and expensive. A loud
    error at parse time is strictly better than an invoice that is off by a factor of 1000.
    """


def parse_amount(value) -> Decimal:
    """Coerce a table cell, JSON number or human-typed string into an exact `Decimal`.

    Numeric inputs are taken exactly (via `str` for floats, so binary artifacts like
    0.1+0.2 never enter). Strings are parsed only where the meaning is unambiguous.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):                      # bool is an int subclass; never money
        raise ValueError(f"not an amount: {value!r}")
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if not isinstance(value, str):
        raise ValueError(f"not an amount: {value!r}")

    text = _NOISE.sub("", value)
    if not text:
        raise ValueError("empty amount")

    present = [s for s in _SEPARATORS if s in text]
    if len(present) == 2:
        # Both present: the LAST one is the decimal separator, the other groups thousands.
        # This is the case the prior art got wrong.
        decimal_sep = max(present, key=text.rfind)
        thousands_sep = "." if decimal_sep == "," else ","
        text = text.replace(thousands_sep, "").replace(decimal_sep, ".")
    elif len(present) == 1:
        sep = present[0]
        head, _, tail = text.rpartition(sep)
        if text.count(sep) > 1:
            text = text.replace(sep, "")             # 1.234.567 — grouping only
        elif len(tail) == 3 and head.lstrip("+-").isdigit() and len(head.lstrip("+-")) <= 3:
            # "1,234" / "1.234" — grouping or three decimal places? Genuinely undecidable.
            raise AmbiguousAmountError(
                f"amount {value!r} could be {head}{tail} or {head}.{tail}; "
                "write it with two decimal places or no grouping"
            )
        else:
            text = f"{head}.{tail}" if head else f"0.{tail}"

    try:
        return Decimal(text)
    except InvalidOperation as e:
        raise ValueError(f"not an amount: {value!r}") from e


@dataclass(frozen=True)
class Line:
    """One invoice line. `kind` records which producer contributed it."""

    description: str
    amount: Decimal
    kind: str

    def as_payload(self) -> dict:
        """Serialise for a milestone body — amount as a string, never a float."""
        return {"description": self.description, "amount": str(self.amount), "kind": self.kind}


def lines_from(payload: dict) -> list[Line]:
    """Read back the lines a milestone payload carries (empty when it produced none)."""
    return [
        Line(description=raw["description"], amount=parse_amount(raw["amount"]),
             kind=raw.get("kind", "unknown"))
        for raw in payload.get("lines", [])
    ]
