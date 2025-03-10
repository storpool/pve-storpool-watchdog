# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""Common definitions for the sp-pve-utils library."""

from __future__ import annotations

import dataclasses
import typing


if typing.TYPE_CHECKING:
    import logging
    from typing import Final


VERSION: Final = "0.1.0"
"""The sp-pve-utils library version, semver-like."""


FEATURES: Final = {
    "sp-pve-utils": VERSION,
    "watchdog-mux": "0.1",
}
"""The list of features supported by the sp-pve-utils library."""


@dataclasses.dataclass(frozen=True)
class Config:
    """Runtime configuration for the sp-pve-utils library."""

    debug: bool
    """Debug mode; display diagnostic output."""

    log: logging.Logger
    """The logger to send diagnostic, informational, and error messages to."""
