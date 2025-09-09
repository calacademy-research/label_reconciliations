"""Core orchestration logic for label reconciliation, including a DataFrame-first API."""

from __future__ import annotations

import argparse
import os
import tempfile
import warnings
import zipfile
from os.path import basename
from typing import Optional, Tuple, Union, Dict, Iterable

import pandas as pd

from . import summary, utils
from .table import Table  # type: ignore

VERSION = "0.8.4"


# --------------------------- Helpers --------------------------- #
def _table_to_dataframe(tbl: Table, args=None) -> pd.DataFrame:
    if args is not None and hasattr(tbl, "to_df"):
        try:
            return tbl.to_df(args, add_note=False)
        except TypeError:
            return tbl.to_df(args)
    if hasattr(tbl, "to_records"):
        return pd.DataFrame(tbl.to_records(add_note=False))
    if hasattr(tbl, "to_pandas") and callable(getattr(tbl, "to_pandas")):
        return tbl.to_pandas()
    if hasattr(tbl, "dataframe"):
        return getattr(tbl, "dataframe")
    try:
        return pd.DataFrame(tbl)
    except Exception as exc:
        raise TypeError(
            "Cannot convert Table to pandas DataFrame. "
            "Implement Table.to_df(args) or Table.to_records()."
        ) from exc



def _normalize_column_types(
    column_types: Optional[Union[Dict[str, str], Iterable[str]]]
) -> Optional[Iterable[str]]:
    """
    Accept either:
      - dict like {"foo": "select", "bar": "text"}  -> ["foo:select,bar:text"]
      - iterable of strings already in "col:type" or "col:type,col2:type2" form
    Returns a list to match argparse 'append' behavior (could be None).
    """
    if column_types is None:
        return None
    if isinstance(column_types, dict):
        joined = ",".join(f"{k}:{v}" for k, v in column_types.items())
        return [joined]
    return list(column_types)


def zip_outputs(
    *,
    zip_path: Optional[str],
    unreconciled_path: Optional[str],
    reconciled_path: Optional[str],
    summary_path: Optional[str],
    keep_originals: bool = False,
) -> None:
    """Zip output files if requested."""
    if not zip_path:
        return

    members = [p for p in (unreconciled_path, reconciled_path, summary_path) if p]
    if not members:
        return

    with zipfile.ZipFile(zip_path, mode="w") as zippy:
        for path in members:
            if os.path.exists(path):
                zippy.write(path, arcname=basename(path), compress_type=zipfile.ZIP_DEFLATED)

    if not keep_originals:
        for path in members:
            if path and os.path.exists(path):
                os.remove(path)


# --------------------------- File-based API (used by CLI) --------------------------- #
def read_unreconciled(args) -> Table:
    formats = utils.get_plugins("formats")
    return formats[args.format].read(args)


def write_unreconciled(args, unreconciled: Table) -> None:
    if getattr(args, "unreconciled", None):
        unreconciled.to_csv(args, args.unreconciled)


def reconcile_data(args, unreconciled: Table) -> Table:
    return unreconciled.reconcile(args)


def write_reconciled(args, reconciled: Table) -> None:
    if getattr(args, "reconciled", None):
        reconciled.to_csv(args, args.reconciled, getattr(args, "explanations", False))


def write_summary(args, unreconciled: Table, reconciled: Table) -> None:
    if getattr(args, "summary", None):
        summary.report(args, unreconciled, reconciled)


def run(args) -> dict:
    """Programmatic entry point (file-based). Returns dict with Table objects."""
    unreconciled: Table = read_unreconciled(args)

    if len(unreconciled) == 0:
        utils.error_exit(f"Workflow {args.workflow_id} has no data.")

    write_unreconciled(args, unreconciled)

    reconciled = None
    if getattr(args, "reconciled", None) or getattr(args, "summary", None):
        reconciled = reconcile_data(args, unreconciled)
        write_reconciled(args, reconciled)
        write_summary(args, unreconciled, reconciled)

    # Support both --zip and --zip-keep styles (keep_originals iff zip_keep provided)
    zip_arg = getattr(args, "zip", None) or getattr(args, "zip_keep", None)
    if zip_arg:
        zip_outputs(
            zip_path=zip_arg,
            unreconciled_path=getattr(args, "unreconciled", None),
            reconciled_path=getattr(args, "reconciled", None),
            summary_path=getattr(args, "summary", None),
            keep_originals=bool(getattr(args, "zip_keep", None)),
        )

    return {"unreconciled": unreconciled, "reconciled": reconciled}


# --------------------------- DataFrame-first API --------------------------- #
def run_on_dataframe(
    df: pd.DataFrame,
    *,
    column_types: Optional[Union[Dict[str, str], Iterable[str]]] = None,
    group_by: str = "subject_id",
    workflow_name: Optional[str] = None,
    workflow_id: Optional[int] = None,
    fuzzy_ratio_threshold: int = 90,
    fuzzy_set_threshold: int = 50,
    join_distance: int = 6,
    page_size: int = 20,
    no_summary_detail: bool = False,
    explanations: bool = False,
    workflow_csv: str = "",
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Run reconciliation fully in-memory:
      - Input: pandas DataFrame
      - Output: (unreconciled_df, reconciled_df or None)

    Notes:
      * Uses the existing 'csv_format' plugin via a temporary CSV.
      * Does NOT write any output files. Returns DataFrames directly.
      * 'column_types' is recommended for CSV/JSON; provide a dict or iterable.
        Example dict: {"field1": "select", "field2": "text"}
    """
    # Temp CSV for the csv_format reader
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_in:
        tmp_in_path = tmp_in.name
    df.to_csv(tmp_in_path, index=False)

    # Build an argparse-like Namespace mirroring CLI defaults/behavior
    args = argparse.Namespace(
        input_file=tmp_in_path,
        unreconciled=None,
        reconciled=None,
        summary=None,
        explanations=bool(explanations),
        zip=None,
        zip_keep=None,
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        fuzzy_ratio_threshold=int(fuzzy_ratio_threshold),
        fuzzy_set_threshold=int(fuzzy_set_threshold),
        join_distance=int(join_distance),
        workflow_csv=str(workflow_csv or ""),
        format="csv_format",  # force CSV pipeline
        column_types=_normalize_column_types(column_types),
        group_by=str(group_by),
        page_size=int(page_size),
        no_summary_detail=bool(no_summary_detail),
        row_key="classification_id",
        user_column="user_name",
        max_transcriptions=50,
    )

    # Validations similar to CLI
    if args.fuzzy_ratio_threshold < 0 or args.fuzzy_ratio_threshold > 100:
        utils.error_exit("--fuzzy-ratio-threshold must be between 0 and 100.")
    if args.fuzzy_set_threshold < 0 or args.fuzzy_set_threshold > 100:
        utils.error_exit("--fuzzy-set-threshold must be between 0 and 100.")
    if not args.column_types:
        warnings.warn(
            "No --column-types provided. Default field type is NoOp; "
            "reconciliation may not change fields as expected."
        )

    out = run(args)

    # Convert Tables -> DataFrames
    unreconciled_tbl = out.get("unreconciled")
    reconciled_tbl = out.get("reconciled")

    unreconciled_df = _table_to_dataframe(unreconciled_tbl, args) if unreconciled_tbl is not None else None
    reconciled_df = _table_to_dataframe(reconciled_tbl, args) if reconciled_tbl is not None else None

    try:
        os.remove(tmp_in_path)
    except Exception:
        pass

    return unreconciled_df, reconciled_df
