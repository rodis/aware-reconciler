"""Tests for amount parsing (ADR 0012).

The prior art's total summed with `parseFloat(String(v).replace(',', '.'))`. `String.replace`
with a string argument replaces only the FIRST occurrence, so a €1,234.50 expense became
€1.23 — silently. These tests exist so that cannot come back.
"""

from decimal import Decimal

import pytest

from reconciler.money import AmbiguousAmountError, Line, lines_from, parse_amount


# --- the bug that motivated Decimal ------------------------------------------------------

def test_us_grouping_with_decimals_is_not_truncated():
    # parseFloat("1,234.50".replace(",", ".")) === parseFloat("1.234.50") === 1.234
    assert parse_amount("1,234.50") == Decimal("1234.50")


def test_eu_grouping_with_decimals():
    assert parse_amount("1.234,50") == Decimal("1234.50")


def test_repeated_grouping_separators():
    assert parse_amount("1.234.567") == Decimal("1234567")
    assert parse_amount("1,234,567") == Decimal("1234567")


def test_currency_symbols_and_spacing_are_tolerated():
    assert parse_amount("€ 1.234,50") == Decimal("1234.50")
    assert parse_amount(" $1,234.50 ") == Decimal("1234.50")


# --- refusing to guess -------------------------------------------------------------------

def test_ambiguous_grouping_raises_rather_than_guessing():
    """`1,234` is either 1234 or 1.234. Guessing wrong on money is silent and expensive."""
    with pytest.raises(AmbiguousAmountError):
        parse_amount("1,234")
    with pytest.raises(AmbiguousAmountError):
        parse_amount("1.234")


def test_unambiguous_three_decimals_are_kept():
    # Four+ integer digits can't be grouping-with-one-group, so it's decimal places.
    assert parse_amount("1234.567") == Decimal("1234.567")


# --- ordinary inputs ---------------------------------------------------------------------

def test_the_live_extras_row_parses_exactly():
    # The one surviving n8n data table row: Coursera Annual sbscription, 239.4
    assert parse_amount(239.4) == Decimal("239.4")


def test_two_decimal_places():
    assert parse_amount("239,40") == Decimal("239.40")
    assert parse_amount("239.40") == Decimal("239.40")


def test_integers_and_decimals_pass_through():
    assert parse_amount(800) == Decimal(800)
    assert parse_amount(Decimal("0.10")) == Decimal("0.10")


def test_floats_go_via_str_so_binary_artifacts_never_enter():
    assert parse_amount(0.1) == Decimal("0.1")
    assert parse_amount(0.1) + parse_amount(0.2) == Decimal("0.3")


def test_leading_decimal_separator():
    assert parse_amount(",50") == Decimal("0.50")


def test_non_amounts_are_rejected():
    for bad in (None, [], {}, "", "abc", True, False):
        with pytest.raises(ValueError):
            parse_amount(bad)


# --- lines -------------------------------------------------------------------------------

def test_a_line_serialises_its_amount_as_a_string():
    payload = Line("Travel", Decimal("340.00"), "manual").as_payload()
    assert payload == {"description": "Travel", "amount": "340.00", "kind": "manual"}
    assert isinstance(payload["amount"], str)   # never a float on the wire


def test_lines_round_trip_through_a_payload():
    original = [Line("A", Decimal("1234.50"), "manual"), Line("B", Decimal("10"), "manual")]
    payload = {"lines": [line.as_payload() for line in original]}
    assert lines_from(payload) == original


def test_a_payload_with_no_lines_reads_as_empty():
    assert lines_from({}) == []
    assert lines_from({"lines": []}) == []
