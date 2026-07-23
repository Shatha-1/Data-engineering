"""
The data contract that every message must pass before it is allowed past
the ingestion boundary.

The source system publishes checkout records in the shape used by most
library circulation systems: a checkout identifier, the format of the
item, a checkout and return timestamp, the branch it was checked out
from and (once processed) the branch it was returned to, and a patron
category. In practice that feed is not clean — items still in
circulation when the extract runs have no return branch yet, clock skew
occasionally produces a return time before the checkout time, and a
handful of scans are logged with a blank patron category.
`LibraryCheckoutContract` is the single place that decides what "clean
enough for Bronze" means.
"""
import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator

CHECKOUT_ID_PATTERN = re.compile(r"^[A-Za-z0-9]{16}$")
VALID_ITEM_FORMATS = {"book", "dvd", "audiobook"}
VALID_PATRON_TYPES = {"member", "guest"}

MIN_LOAN_SECONDS = 120              # shorter than this is almost always a scan error
MAX_LOAN_DAYS = 21                  # the library's maximum loan period


class LibraryCheckoutContract(BaseModel):
    """Canonical shape of one validated checkout record."""

    checkout_id: str
    item_format: str
    checked_out_at: datetime
    returned_at: Optional[datetime] = None
    checkout_branch_id: str
    return_branch_id: Optional[str] = None
    patron_type: str

    @field_validator("checkout_id")
    @classmethod
    def checkout_id_well_formed(cls, value: str) -> str:
        if not value or not CHECKOUT_ID_PATTERN.match(value):
            raise ValueError("checkout_id must be a 16-character alphanumeric token")
        return value

    @field_validator("item_format")
    @classmethod
    def item_format_known(cls, value: str) -> str:
        if value not in VALID_ITEM_FORMATS:
            raise ValueError(f"item_format '{value}' is not a recognised item format")
        return value

    @field_validator("patron_type")
    @classmethod
    def patron_type_known(cls, value: str) -> str:
        if value not in VALID_PATRON_TYPES:
            raise ValueError(f"patron_type '{value}' is not a recognised patron category")
        return value

    @field_validator("checkout_branch_id")
    @classmethod
    def checkout_branch_present(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("checkout_branch_id must not be blank")
        return value

    @model_validator(mode="after")
    def loan_window_is_sane(self) -> "LibraryCheckoutContract":
        # An item still checked out has no returned_at yet — that is a
        # normal, accepted state, not a contract violation.
        if self.returned_at is None:
            return self

        if self.returned_at <= self.checked_out_at:
            raise ValueError("returned_at must be after checked_out_at")
        duration = (self.returned_at - self.checked_out_at).total_seconds()
        if duration < MIN_LOAN_SECONDS:
            raise ValueError(f"loan duration {duration:.0f}s is below the {MIN_LOAN_SECONDS}s floor")
        if duration > MAX_LOAN_DAYS * 24 * 60 * 60:
            raise ValueError(f"loan duration exceeds the {MAX_LOAN_DAYS}-day ceiling")
        return self

    @property
    def loan_duration_sec(self) -> Optional[float]:
        if self.returned_at is None:
            return None
        return (self.returned_at - self.checked_out_at).total_seconds()


def business_key(record: dict) -> str:
    """
    The natural key a Silver MERGE upserts on.

    This feed already carries a globally unique `checkout_id` per loan, so
    the business key is simply that value passed through untouched. Keeping
    the derivation as an explicit function (rather than inlining
    `record["checkout_id"]` everywhere) means the key logic has exactly one
    place to change if a future source ever needs a composite key instead.
    """
    return str(record["checkout_id"])
