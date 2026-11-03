"""`N8nGmailMetadataQuery`'s parsing — the headers-only shape, measured on the live relay
2026-11-03 (two LinkedIn job alerts, 1.6 KB for both)."""

from reconciler.adapters.gmail import metadata_time, normalise_metadata

LIVE_SHAPE = {
    "id": "1a1a0b8dd089e94b",
    "threadId": "1a1a0b8dd089e94b",
    "snippet": "Jobgether Lead Tech SRE: This position is listed on behalf of a partner",
    "sizeEstimate": 112349,
    "internalDate": "1793697831000",
    "From": "LinkedIn Job Alerts <jobalerts-noreply@linkedin.com>",
    "Subject": "Lead Tech SRE at Jobgether",
}


def test_the_address_is_parsed_out_of_the_from_header():
    m = normalise_metadata(LIVE_SHAPE)
    assert m["from"] == "jobalerts-noreply@linkedin.com"
    assert m["from_name"] == "LinkedIn Job Alerts"
    assert m["from_domain"] == "linkedin.com"
    assert m["upstream_id"] == "1a1a0b8dd089e94b" and m["subject"] == "Lead Tech SRE at Jobgether"


def test_a_bare_address_and_a_missing_header_both_parse():
    assert normalise_metadata({"From": "noreply@class.it"})["from"] == "noreply@class.it"
    assert normalise_metadata({})["from"] == ""


def test_a_display_name_that_looks_like_an_address_is_not_the_address():
    """The exact-sender guard compares `from`; a spoofed display name must not reach it."""
    m = normalise_metadata({"From": '"mftradingguide@class.it" <someone@else.com>'})
    assert m["from"] == "someone@else.com"


def test_time_is_gmails_internal_date_in_seconds():
    assert metadata_time(LIVE_SHAPE) == 1793697831
    assert metadata_time({}) == 0 and metadata_time({"internalDate": "garbage"}) == 0
