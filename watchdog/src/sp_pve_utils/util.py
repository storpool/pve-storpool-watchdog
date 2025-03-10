# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""Helper functions for the various utilities."""

from __future__ import annotations

import functools
import json
import logging
import subprocess  # noqa: S404
import sys
import time
from typing import TYPE_CHECKING, Final


if TYPE_CHECKING:
    import click

from . import defs


PVE_CLUSTER_SLEEP_INTERVAL = 10


def arg_features(_ctx: click.Context, _self: click.Parameter, value: bool) -> bool:  # noqa: FBT001
    """Display program features information and exit."""
    if not value:
        return value

    print("Features: " + " ".join(f"{name}={value}" for name, value in defs.FEATURES.items()))  # noqa: T201
    sys.exit(0)


@functools.lru_cache
def build_logger(
    *,
    debug: bool = False,
    verbose: bool = True,
    all_stderr: bool = False,
) -> logging.Logger:
    """Build a logger that outputs to the standard output and error streams.

    Messages of level `INFO` go to the standard output stream.
    Messages of level `WARNING` and higher go to the standard error stream.
    If `verbose` is true, messages of level `DEBUG` also go to the standard error stream.

    If `all_stderr` is true, messages of level `INFO` also go to the standard error stream.
    """
    logger: Final = logging.getLogger("logging_std")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.propagate = False

    if all_stderr:
        handler: Final = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.DEBUG if debug else logging.INFO if verbose else logging.WARNING)
        logger.addHandler(handler)
    else:
        diag_handler: Final = logging.StreamHandler(sys.stderr)
        diag_handler.setLevel(logging.DEBUG if debug else logging.WARNING)
        diag_handler.addFilter(lambda rec: rec.levelno != logging.INFO)
        logger.addHandler(diag_handler)

        if verbose:
            info_handler: Final = logging.StreamHandler(sys.stdout)
            info_handler.setLevel(logging.INFO)
            info_handler.addFilter(lambda rec: rec.levelno == logging.INFO)
            logger.addHandler(info_handler)

    return logger


def get_pve_cluster(log: logging.Logger) -> str:
    """Get cluster name."""
    command = ["pvesh", "get", "cluster/status", "--output-format=json"]

    def _get_cluster_name() -> str:
        log.debug("Getting PVE cluster name: %(command)s", {"command": command})
        try:
            result = subprocess.run(
                command,  # noqa: S603
                timeout=5,
                capture_output=True,
                check=True,
            )
        except subprocess.CalledProcessError:
            log.exception("Failed fetching PVE cluster information - no quorum?")
            return ""
        except FileNotFoundError:
            log.exception("Failed fetching PVE cluster information - no pvesh executable?")
            return ""

        try:
            cl_status = json.loads(result.stdout)
        except json.JSONDecodeError:
            log.exception("Failed decoding PVE cluster JSON")
            return ""

        try:
            cluster_info = next(item for item in cl_status if item.get("type") == "cluster")
        except StopIteration:
            log.exception("No PVE cluster information")
            return ""

        cluster_name = str(cluster_info.get("name", ""))
        if not cluster_name:
            log.error("Empty PVE cluster name")
            return ""
        log.debug("PVE cluster name: %(name)s", {"name": cluster_name})
        return cluster_name

    while not (cluster_name := _get_cluster_name()):
        time.sleep(PVE_CLUSTER_SLEEP_INTERVAL)
    return cluster_name
