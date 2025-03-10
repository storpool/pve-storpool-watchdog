# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""AsyncIO subprocess wrapper."""

from __future__ import annotations

import asyncio
import dataclasses
import os
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import logging


@dataclasses.dataclass(frozen=True)
class Subprocess:
    """Output from executing a subprocess."""

    pid: int
    """PID of the process."""

    exit_code: int
    """Process exit code."""

    stdout: str
    """Standard output."""

    stderr: str
    """Error output."""


async def run(
    args: list[str],
    log: logging.Logger,
    program: str = "",
    timeout: int = 5,
) -> Subprocess:
    """Run a subprocess using asyncio."""
    env = dict(os.environ)
    env.update({"LC_ALL": "C.UTF-8", "LANGUAGE": ""})
    proc = await asyncio.subprocess.create_subprocess_exec(
        program or args[0],
        *(args if program else args[1:]),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    log.info(
        "Subprocess(pid=%(pid)d, args=%(args)s) started",
        {"args": args, "pid": proc.pid},
    )

    try:
        bout, berr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        exit_code = await proc.wait()
    except asyncio.TimeoutError:
        log.error(  # noqa: TRY400
            "Subrocess(pid=%(pid)d) timed out after %(timeout)ds",
            {"pid": proc.pid, "timeout": timeout},
        )
        exit_code = -1
        try:
            log.info("Subprocess(pid=%(pid)d) being terminated", {"pid": proc.pid})
            proc.kill()
        except ProcessLookupError:
            log.info("Subprocess(pid=%(pid)d) was already gone", {"pid": proc.pid})
        bout, berr = await proc.communicate()

    result = Subprocess(
        pid=proc.pid,
        exit_code=exit_code,
        stdout=bout.decode("UTF-8"),
        stderr=berr.decode("UTF-8"),
    )
    log.info("%(result)s finished", {"result": result})
    return result
