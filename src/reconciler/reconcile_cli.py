#!/usr/bin/env python3
from __future__ import annotations

import argparse
import textwrap

from .reconcile_util import VERSION, run
from .pylib import utils


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        fromfile_prefix_chars="@",
        description=textwrap.dedent(
            """
            This takes raw Notes from Nature classifications and creates a
            reconciliation of the classifications for a particular workflow.
            That is, it reduces n classifications per subject to the "best"
            values."""
        ),
        epilog=textwrap.dedent(
            """
            Current reconciliation types
            ----------------------------
            select: Reconcile a fixed list of options.
            text:   Reconcile free text entries.
            same:   Check that all items in a group are the same.
            box:    Reconcile drawn bounding boxes, the mean of the corners.
                    Required box format:
                    {"x": <int>, "y": <int>, "width": <int>, "height": <int>}
            point:  Calculate the mean of a point. Required point format:
                    {"x": <int>, "y": <int>}
            noop:   Do nothing with this field.
            length: Calculate the length of a drawn line. It first calculates the
                    mean of the end points and then uses a scale to get the
                    calibrated length relative to the scale. Required length format:
                    {"x1": <int>, "y1": <int>, "x2": <int>, "y2": <int>}
                    To get actual lengths (vs. pixel) you will need a scale length
                    header with a number and column with units. Ex: "scale 0.5 mm".
            """
        ),
    )

    parser.add_argument("input_file", metavar="INPUT-FILE", help="The input file.")

    parser.add_argument(
        "-u", "--unreconciled",
        help="Write the unreconciled workflow classifications to this CSV file.",
    )
    parser.add_argument(
        "-r", "--reconciled",
        help="Write the reconciled classifications to this CSV file.",
    )
    parser.add_argument(
        "-s", "--summary",
        help="Write a summary of the reconciliation to this HTML file.",
    )
    parser.add_argument(
        "-e", "--explanations",
        action="store_true",
        help="Output reconciled explanations with the reconciled CSV.",
    )
    parser.add_argument("-z", "--zip", help="Zip the output files into this archive.")

    parser.add_argument(
        "-n", "--workflow-name",
        help="The name of the workflow. NfN extracts can find a default.",
    )
    parser.add_argument(
        "-w", "--workflow-id",
        type=int,
        help=("The workflow to extract. Required if there is more than one workflow "
              "in the classifications file. This is only used for nfn formats."),
    )

    parser.add_argument(
        "--fuzzy-ratio-threshold",
        default=90, type=int,
        help="Cutoff for fuzzy ratio matching (0-100) (default: %(default)s).",
    )
    parser.add_argument(
        "--fuzzy-set-threshold",
        default=50, type=int,
        help="Cutoff for fuzzy set matching (0-100) (default: %(default)s).",
    )
    parser.add_argument(
        "--join-distance",
        default=6, type=int,
        help="When highlighted texts are within this distance, join them (default: %(default)s).",
    )

    parser.add_argument(
        "--workflow-csv",
        default="", metavar="CSV",
        help=("Sometimes we need to translate a value from its numeric code to a "
              "human-readable string. The workflow file will contain these translations."),
    )

    parser.add_argument(
        "-f", "--format",
        choices=["nfn", "csv", "json"], default="nfn",
        help=("Input file type. nfn=Zooniverse dump; csv=flat CSV; json=JSON. "
              "For 'csv' or 'json', --column-types is required (but can override 'nfn')."),
    )
    parser.add_argument(
        "-c", "--column-types",
        action="append",
        help=('Identify reconciliation types for columns. '
              'Example: --column-types "foo:select,bar:text,baz:text". '
              'Default type is NoOp (do nothing).'),
    )

    parser.add_argument(
        "--group-by",
        default="subject_id",
        help="Group CSV/JSON rows by this column (default: subject_id).",
    )
    parser.add_argument(
        "--page-size",
        default=20, type=int,
        help="Page size for the summary report's detail section (default: %(default)s).",
    )
    parser.add_argument(
        "--no-summary-detail",
        action="store_true",
        help="Skip the Reconciliation Detail section in the summary report.",
    )

    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s {VERSION}")

    args = parser.parse_args(argv)

    # Legacy/implicit defaults to match original script
    setattr(args, "row_key", "classification_id")
    setattr(args, "user_column", "user_name")
    setattr(args, "max_transcriptions", 50)
    setattr(args, "format", f"{args.format}_format")

    # Validation parity with legacy
    if args.fuzzy_ratio_threshold < 0 or args.fuzzy_ratio_threshold > 100:
        utils.error_exit("--fuzzy-ratio-threshold must be between 0 and 100.")
    if args.fuzzy_set_threshold < 0 or args.fuzzy_set_threshold > 100:
        utils.error_exit("--fuzzy-set-threshold must be between 0 and 100.")
    if (args.format == "nfn_format") and args.column_types:
        warnings_msg = "Column types are ignored for 'nfn' format."
        import warnings
        warnings.warn(warnings_msg)

    return args


def main(argv=None):
    args = parse_args(argv)
    run(args)
