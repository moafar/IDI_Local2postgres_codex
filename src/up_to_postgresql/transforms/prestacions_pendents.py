"""Source checks and business rules for the pending prestations snapshot."""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


FLOW_NAME = "prestacions_pendents"
SOURCE_FILENAME = f"{FLOW_NAME}.csv"
YEAR = "Any prestació (YYYY)"
PERIOD = "Periode prestació (YYYY-MM)"
SERVICE_DATE = "Data prestació"
CENTRE = "Centre Codi"
KEY = (CENTRE, "Prestació número")
DESCRIPTION = "Prestació descripció"
LEVEL_9 = "Prestació nivell 9 desc"
COUNT = "Número de prestacions pendents de realitzar"
DERIVED_COLUMNS = (
    "Centre nom",
    "Període informe",
    "Data tall",
    "Mes prestació número",
    "Mes",
    "Acumulat",
)
PERIOD_PATTERN = r"(?!0000)[0-9]{4}-(?:0[1-9]|1[0-2])"
DATE_PATTERN = re.compile(r"[0-9]{2}/[0-9]{2}/[0-9]{2}")


class PendingSnapshotError(ValueError):
    """A validation error safe to include in aggregate reports."""


def check_csv(path: Path, expected_header: list[str], report: dict[str, Any]) -> int:
    # pandas can skip quoted empty records or infer an index from excess fields.
    # Check the record widths before passing the file to the configured reader.
    with path.open(encoding="utf-8", newline="") as source:
        records = (row for row in csv.reader(source, delimiter=";", strict=True) if row)
        if next(records, None) != ["Detall"]:
            raise PendingSnapshotError("Expected the CSV title Detall.")
        header = next(records, None)
        if header != expected_header:
            raise PendingSnapshotError("Unexpected CSV header.")
        rows_read = malformed_rows = 0
        try:
            for row in records:
                rows_read += 1
                malformed_rows += len(row) != len(expected_header) or any(
                    "\x00" in field for field in row
                )
        except csv.Error:
            malformed_rows += 1
            raise PendingSnapshotError("Invalid CSV quoting or record size.") from None
        finally:
            report.update(
                rows_read=rows_read,
                columns_read=len(expected_header),
                malformed_rows=malformed_rows,
            )
    if malformed_rows:
        raise PendingSnapshotError(f"Malformed CSV records: {malformed_rows}.")
    return rows_read


def _valid_service_date(value: str, period: str) -> bool:
    if value == "":
        return True
    if not DATE_PATTERN.fullmatch(value):
        return False
    day, month, year = value.split("/")
    if month != period[5:] or year != period[2:4]:
        return False
    try:
        date.fromisoformat(f"{period}-{day}")
    except ValueError:
        return False
    return True


def _counts(frame: pd.DataFrame, report: dict[str, Any]) -> pd.Series:
    periods = frame[PERIOD]
    invalid_period = ~periods.str.fullmatch(PERIOD_PATTERN) | periods.str[:4].ne(
        frame[YEAR]
    )

    dates = frame[SERVICE_DATE]
    date_pairs = list(zip(dates, periods))
    validity = {pair: _valid_service_date(*pair) for pair in set(date_pairs)}
    invalid_date_rows = sum(not validity[pair] for pair in date_pairs)

    count = frame[COUNT]
    significant = count.str.lstrip("0")
    length = significant.str.len()
    invalid_count = (
        ~count.str.fullmatch(r"[0-9]+")
        | (length > 10)
        | ((length == 10) & (significant > "2147483647"))
    )
    invalid_value_rows = {
        "invalid_period_rows": int(invalid_period.sum()),
        "invalid_date_rows": invalid_date_rows,
        "invalid_count_rows": int(invalid_count.sum()),
    }
    report.update(invalid_value_rows)
    if any(invalid_value_rows.values()):
        raise PendingSnapshotError(
            "Invalid service periods, dates or counts. See report."
        )
    return significant.replace("", "0").astype("int64")


def transform(
    frame: pd.DataFrame, settings: dict[str, Any], report: dict[str, Any]
) -> pd.DataFrame:
    counts = _counts(frame, report)
    interventional = frame[DESCRIPTION].eq(settings["intervencionistes"])
    level_9 = frame[LEVEL_9].fillna("").str.casefold()
    exceptions = level_9.isin(
        [item.casefold() for item in settings["nivell_9_exactes"]]
    )
    for fragment in settings["nivell_9_fragments"]:
        exceptions |= level_9.str.contains(fragment.casefold(), regex=False)
    excluded = interventional & ~exceptions
    period = settings["periode_informe"]
    in_month = frame[PERIOD].eq(period)
    included = ~excluded
    report.update(
        interventional_rows=int(interventional.sum()),
        rows_excluded=int(excluded.sum()),
        rows_included=int(included.sum()),
        count_before_filter=int(counts.sum()),
        mes_before_filter=int(counts[in_month].sum()),
        excluded_mes=int(counts[excluded & in_month].sum()),
        acumulat=int(counts[included].sum()),
        mes=int(counts[included & in_month].sum()),
    )
    if not included.any():
        raise PendingSnapshotError("No rows remain after the inclusion filter.")

    result = frame.loc[included].copy()
    result["Centre nom"] = result[CENTRE].replace(settings["centres"])
    result["Període informe"] = period
    result["Data tall"] = settings["data_tall"]
    result["Mes prestació número"] = result[PERIOD].str[-2:].astype("int64")
    result["Mes"] = counts.loc[included].where(in_month.loc[included], 0)
    result["Acumulat"] = counts.loc[included]
    return result
