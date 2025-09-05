#!/usr/bin/env python3
from __future__ import annotations

import os
import warnings
import zipfile
import tempfile
from os.path import basename
from typing import Optional, Tuple, Union, Dict, Iterable

import pandas as pd

# Local imports (pylib lives inside the package)
from .pylib import summary
from .pylib import utils
from .pylib.table import Table

VERSION = "0.8.4"


def zip_files(args) -> None:
    """
    Zip output files into args.zip (or args.zip_keep if present), then
    remove the originals. Matches the legacy behavior.
    """
    zip_file = getattr(args, "zip", None) or getattr(args, "zip_keep", None)
    if not zip_file:
        return

    args_dict = vars(args)
    arg_files = ["unreconciled", "reconciled", "summary"]

    with zipfile.ZipFile(zip_file, mode="w") as zippy:
        for arg_file in arg_files:
            if args_dict.get(arg_file):
                zippy.write(
                    args_dict[arg_file],
                    arcname=basename(args_dict[arg_file]),
                    compress_type=zipfile.ZIP_DEFLATED,
                )

    for arg_file in arg_files:
        if args_dict.get(arg_file):
            try:
                os.remove(args_dict[arg_file])
            except OSError:
                pass


def _table_to_dataframe(tbl: Table) -> pd.DataFrame:
    """
    Best-effort conversion from the internal Table to a pandas DataFrame.
    Adjust if your Table exposes a different accessor.
    """
    if hasattr(tbl, "to_pandas") and callable(getattr(tbl, "to_pandas")):
        return tbl.to_pandas()
    if hasattr(tbl, "dataframe"):
        return getattr(tbl, "dataframe")
    try:
        return pd.DataFrame(tbl)
    except Exception as exc:
        raise TypeError(
            "Cannot convert Table to pandas DataFrame. "
            "Expose a .to_pandas() or .dataframe attribute on Table."
        ) from exc


def run(args) -> dict:
    """
    File-based entry point used by the CLI. Returns a dict with
    {'unreconciled': Table, 'reconciled': Optional[Table]}.
    """
    formats = utils.get_plugins("formats")
    unreconciled: Table = formats[args.format].read(args)

    if len(unreconciled) == 0:
        utils.error_exit(f"Workflow {args.workflow_id} has no data.")

    if getattr(args, "unreconciled", None):
        unreconciled.to_csv(args, args.unreconciled)

    reconciled = None
    if getattr(args, "reconciled", None) or getattr(args, "summary", None):
        reconciled = unreconciled.reconcile(args)

        if getattr(args, "reconciled", None):
            reconciled.to_csv(args, args.reconciled, getattr(args, "explanations", False))

        if getattr(args, "summary", None):
            summary.report(args, unreconciled, reconciled)

    if getattr(args, "zip", None) or getattr(args, "zip_keep", None):
        zip_files(args)

    return {"unreconciled": unreconciled, "reconciled": reconciled}


# ------------------------ DataFrame-first API ------------------------ #
def _normalize_column_types(
    column_types: Optional[Union[Dict[str, str], Iterable[str]]]
) -> Optional[Iterable[str]]:
    """
    Accept either:
      - dict like {"foo": "select", "bar": "text"} -> ["foo:select,bar:text"]
      - iterable of strings already in "col:type" or "col:type,col2:type2" form
    Returns a list to match argparse 'append' behavior (could be None).
    """
    if column_types is None:
        return None
    if isinstance(column_types, dict):
        joined = ",".join(f"{k}:{v}" for k, v in column_types.items())
        return [joined]
    return list(column_types)


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
    In-memory reconciliation:
      - Input: pandas DataFrame
      - Output: (unreconciled_df, reconciled_df or None)

    Uses the 'csv_format' pipeline internally by writing a temporary CSV.
    """
    # Temp CSV for the csv_format reader
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_in:
        tmp_in_path = tmp_in.name
    df.to_csv(tmp_in_path, index=False)

    # Build a lightweight Namespace that mirrors CLI defaults
    import argparse as _argparse  # local import to avoid hard dep at import-time
    args = _argparse.Namespace(
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

    # Validate thresholds similar to CLI behavior
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

    unreconciled_tbl = out.get("unreconciled")
    reconciled_tbl = out.get("reconciled")

    unreconciled_df = _table_to_dataframe(unreconciled_tbl) if unreconciled_tbl is not None else None
    reconciled_df = _table_to_dataframe(reconciled_tbl) if reconciled_tbl is not None else None

    # Cleanup the temp CSV
    try:
        os.remove(tmp_in_path)
    except Exception:
        pass

    return unreconciled_df, reconciled_df


__all__ = ["VERSION", "run", "run_on_dataframe"]
