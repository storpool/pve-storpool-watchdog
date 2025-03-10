# SPDX-FileCopyrightText: StorPool <support@storpool.com>
# SPDX-License-Identifier: BSD-2-Clause
"""Do things, or do other things."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import functools
import itertools
import json
import pathlib
import re
import signal
import socket
import subprocess  # noqa: S404
import sys
from typing import TYPE_CHECKING

import click
from storpool import spapi  # type: ignore[import-untyped]
from storpool import spconfig

from . import asyncproc
from . import defs
from . import util


if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Final


CLIENT_MSG_HEARTBEAT: Final = b"\x00"
CLIENT_MSG_WARN: Final = b"V"

CLIENT_TIMEOUT_HEARTBEAT: Final = 30  # in seconds

WD_ACTIVE_MARKER: Final = pathlib.Path("/run/watchdog-mux.active")
"""The default active connections marker for the `sp-watchdog-mux` service."""

WD_SOCK_PATH: Final = pathlib.Path("/run/watchdog-mux.sock")
"""The default listening socket for the `sp-watchdog-mux` service."""

PVE_HA_SERVICES = ["pve-ha-crm.service", "pve-ha-lrm.service", "corosync.service"]
RE_PVE_SERVICE: Final = re.compile(
    r"^pve(?!(-cluster|-guests|-daily-update|netcommit|banner|-lxc-syscalld|-ha))"
    r"[a-zA-Z0-9:\-_\.]+\.service$"
)

SP_DETACH_SLEEP = 5


@dataclasses.dataclass(frozen=True)
class Config(defs.Config):
    """Runtime configuration for the `sp-watchdog-mux` service."""

    active_marker: pathlib.Path
    """The path to the active connections marker directory."""

    listen_socket: pathlib.Path
    """The path to the Unix socket to listen on."""

    sp_ourid: int
    """StorPool cluster node ID."""

    pve_cluster: str
    """Name of this PVE cluster."""


@dataclasses.dataclass
class ConfigHolder:
    """Hold a `Config` object."""

    cfg: Config | None = None
    """The `Config` object stashed by the main function."""


def extract_cfg(ctx: click.Context) -> Config:
    """Extract the `Config` object that the main function built."""
    cfg_hold: Final = ctx.find_object(ConfigHolder)
    if cfg_hold is None:
        sys.exit("Internal error: no click config holder object")

    cfg: Final = cfg_hold.cfg
    if cfg is None:
        sys.exit("Internal error: no config in the click config holder")

    return cfg


@dataclasses.dataclass(frozen=True)
class MainMsg:
    """The top-level class for messages sent to the main thread."""


@dataclasses.dataclass(frozen=True)
class TimerMsg(MainMsg):
    """A second went by."""


@dataclasses.dataclass(frozen=True)
class NewConnMsg(MainMsg):
    """A new client connection has been established."""

    reader: asyncio.StreamReader
    """The stream to read incoming bytes from."""


@dataclasses.dataclass(frozen=True)
class ClientDoneMsg(MainMsg):
    """A client connection is done."""

    client_id: int
    """The internal ID of the client connection."""

    warned: bool
    """Did the client warn us that it might be going away?"""


@dataclasses.dataclass(frozen=True)
class SigStopMsg(MainMsg):
    """A "please stop" signal was received."""

    sig: int
    """The signal we received."""


@dataclasses.dataclass(frozen=True)
class PanicMsg(MainMsg):
    """We need to stop all VMs and PVE services."""


@dataclasses.dataclass(frozen=True)
class WDTask:
    """A task to wait for: either a client connection or the listening task."""

    name: str
    """The internal name of the task."""

    task: asyncio.Task[None]
    """The task itself."""


@dataclasses.dataclass(frozen=True)
class WDClientTask(WDTask):
    """A client connection task."""

    client_id: int
    """The internal ID of this client connection."""


@dataclasses.dataclass(frozen=True)
class WDClientTimeoutTask(WDClientTask):
    """A client heartbeat timeout task."""


@dataclasses.dataclass(frozen=True)
class GlobalState:
    """The current state of the main loop: tasks, etc."""

    cfg: Config
    """The runtime configuration of the program."""

    mainq: asyncio.Queue[MainMsg]
    """The queue that tasks send messages on."""

    tasks: list[WDTask]
    """The tasks themselves."""

    tasks_lock: asyncio.Lock
    """The mutex that MUST be held while accessing `tasks` in ANY way."""

    client_id: Iterator[int]
    """The generator for successive connection IDs."""

    panic_mode: asyncio.Event
    """Whether we need to stop all VMs and PVE services."""


async def sp_force_detach(cfg: Config) -> None:
    """Force detach SP PVE volume and snapshot attachments on this node.

    Note that this function is not truly async - the SP API is blocking,
    although we've tried to minimise time spent there, and added some async
    sleep calls.
    """
    spa = spapi.Api.fromConfig(use_env=False, timeout=15, transientRetries=0)
    volumes = []
    while True:
        try:
            attachments = spa.attachmentsList(returnRawAPIData=True)
        except Exception:
            cfg.log.exception("Failed to retrieve SP attachments")
            await asyncio.sleep(SP_DETACH_SLEEP)
            continue
        for att in attachments:
            if att.get("tags", {}).get("pve-loc") != cfg.pve_cluster or att.get("snapshot") is True:
                continue
            volumes.append(att["volume"])
        break
    if not volumes:
        return

    reassign = {
        "reassign": [
            {"volume": volume, "detach": [cfg.sp_ourid], "force": True} for volume in volumes
        ]
    }
    while True:
        try:
            spa.volumesReassignWait(reassign)
        except Exception:
            cfg.log.exception("Failed detaching volumes")
            await asyncio.sleep(SP_DETACH_SLEEP)
            continue
        return


async def restart_pve_ha(state: GlobalState) -> None:
    """Restart PVE HA services on the node."""
    state.cfg.log.info("Killing and starting PVE HA services")
    await asyncproc.run(
        ["systemctl", "kill", "--signal=SIGKILL", *PVE_HA_SERVICES],
        state.cfg.log,
        timeout=60,
    )
    await asyncproc.run(
        ["systemctl", "start", *PVE_HA_SERVICES],
        state.cfg.log,
        timeout=180,
    )


async def restart_pve_services(state: GlobalState) -> None:
    """Restart PVE services on the node."""
    result = await asyncproc.run(["systemctl", "list-units", "--output=json"], state.cfg.log)
    if result.exit_code != 0:
        state.cfg.log.error("Failed to retrieve list of systemd units")
        return

    try:
        all_units = json.loads(result.stdout)
    except json.JSONDecodeError:
        state.cfg.log.exception("Error decoding unit list JSON")
        return

    restart_services = [
        unit["unit"] for unit in all_units if RE_PVE_SERVICE.match(unit.get("unit", ""))
    ]
    if not restart_services:
        state.cfg.log.error("No PVE services to restart!")
        return

    state.cfg.log.info("Restarting PVE services: %(services)s", {"services": restart_services})
    result = await asyncproc.run(
        ["systemctl", "restart", *restart_services],
        state.cfg.log,
        timeout=180,
    )
    if result.exit_code != 0:
        state.cfg.log.error("Failed to restart PVE services")


async def _kill_vms(state: GlobalState) -> None:
    """Kill all running VMs on the node."""
    result = await asyncproc.run(["pgrep", "-P", "1", "-u", "0", "-x", "kvm"], state.cfg.log)
    if result.exit_code != 0:
        state.cfg.log.error("Failed to retrieve list of VM processes")

    if not (pids := [pid for pid in result.stdout.split("\n") if pid.strip()]):
        state.cfg.log.info("No VM processes to kill")
        return

    state.cfg.log.info("Killing VM PIDs: %(pids)s", {"pids": pids})
    result = await asyncproc.run(["kill", "--signal=KILL", *pids], state.cfg.log)
    if result.exit_code != 0:
        state.cfg.log.error("Failed to kill VM processes")


async def do_signal(state: GlobalState, sig_sock: socket.socket) -> None:
    """Send a "oof, got a signal" message to the main thread when a signal arrives."""
    reader, _ = await asyncio.open_unix_connection(sock=sig_sock)
    state.cfg.log.debug("Waiting for signals on %(reader)r", {"reader": reader})
    while True:
        data = await reader.read(1)
        state.cfg.log.warning("Got some sort of signal: %(data)r", {"data": data})
        await state.mainq.put(SigStopMsg(data[0]))


async def do_timer(state: GlobalState) -> None:
    """Send a "hey, some time passed" message to the main queue every now and then."""
    while True:
        await asyncio.sleep(1)
        if not state.panic_mode.is_set():
            # panic mode handling takes a long time during which messages are just piling up
            # in the queue since its processing is sequential, so no point in filling it up
            await state.mainq.put(TimerMsg())


async def do_listen(state: GlobalState) -> None:
    """Listen for incoming client connections, pass them on to the main thread."""

    async def got_client(reader: asyncio.StreamReader, _writer: asyncio.StreamWriter) -> None:
        """Handle a client connection."""
        state.cfg.log.info("New connection")
        await state.mainq.put(NewConnMsg(reader))

    srv: Final = await asyncio.start_unix_server(got_client, path=state.cfg.listen_socket)
    state.cfg.log.info(
        "Listening on %(sockets)s",
        {"sockets": ", ".join(sock.getsockname() for sock in srv.sockets)},
    )
    async with srv:
        await srv.serve_forever()


async def client_read(state: GlobalState, client_id: int, reader: asyncio.StreamReader) -> None:
    """Read data from a client."""
    warned = False
    state.cfg.log.debug("Waiting for stuff to come in via %(reader)r", {"reader": reader})
    while True:
        if not (data := await reader.read(1)):
            break
        state.cfg.log.debug(
            "Client %(client_id)d said %(data)r",
            {"client_id": client_id, "data": data},
        )
        if CLIENT_MSG_HEARTBEAT in data:
            state.cfg.log.debug("Client %(client_id)d checked in", {"client_id": client_id})
            await client_reset_timeout(state, client_id)
        if CLIENT_MSG_WARN in data:
            state.cfg.log.info(
                "Client %(client_id)d said it might go away soon",
                {"client_id": client_id},
            )
            warned = True

    if warned:
        state.cfg.log.info("Client %(client_id)d went away gracefully", {"client_id": client_id})
    else:
        state.cfg.log.warning(
            "Client %(client_id)d went away without saying goodbye",
            {"client_id": client_id},
        )
    message = ClientDoneMsg(client_id, warned)
    if state.panic_mode.is_set():
        state.cfg.log.info("Panic mode, not adding %(message)s to queue", {"message": message})
        return
    await state.mainq.put(message)


async def trigger_panic_mode(state: GlobalState) -> None:
    """Trigger panic mode if not already triggered."""
    if state.panic_mode.is_set():
        state.cfg.log.info("Already in panic mode")
        return
    state.panic_mode.set()
    await state.mainq.put(PanicMsg())


async def check_heartbeat_timeout(state: GlobalState, client_id: int) -> None:
    """Trigger panic mode if a client's heartbeat does not cancel this task on time."""
    await asyncio.sleep(CLIENT_TIMEOUT_HEARTBEAT)
    state.cfg.log.warning(
        "Triggering panic mode due to client %(client)d reaching heartbeat timeout",
        {"client": client_id},
    )
    await trigger_panic_mode(state)


async def client_reset_timeout(state: GlobalState, client_id: int) -> None:
    """Cancel any current timeout checks for this client and launch a new one."""
    async with state.tasks_lock:
        to_remove: Final[list[int]] = []
        for idx, task in (
            (idx, task)
            for idx, task in enumerate(state.tasks)
            if isinstance(task, WDClientTimeoutTask) and task.client_id == client_id
        ):
            state.cfg.log.warning("Cancelling task %(name)s", {"name": task.name})
            task.task.cancel()
            to_remove.append(idx)

        state.cfg.log.info("Removing %(count)d tasks", {"count": len(to_remove)})
        for idx in reversed(to_remove):
            state.tasks.pop(idx)

        timeout_task: Final = asyncio.create_task(check_heartbeat_timeout(state, client_id))
        state.tasks.append(
            WDClientTimeoutTask(f"client/{client_id} heartbeat check", timeout_task, client_id)
        )


@functools.singledispatch
async def handle_msg(msg: MainMsg, _state: GlobalState) -> bool:
    """Handle a single message."""
    sys.exit(f"sp-watchdog-mux internal error: unexpected mainq message {msg!r}")


@handle_msg.register
async def handle_msg_timer(_msg: TimerMsg, state: GlobalState) -> bool:
    """Check the active tasks."""
    async with state.tasks_lock:
        to_remove: Final[list[int]] = []
        for idx, task in ((idx, task) for idx, task in enumerate(state.tasks) if task.task.done()):
            state.cfg.log.warning(
                "Task %(name)s went away: %(task)r",
                {"name": task.name, "task": task},
            )
            with contextlib.suppress(asyncio.CancelledError):
                await task.task
            to_remove.append(idx)

        if to_remove:
            had_clients: Final = any(task for task in state.tasks if isinstance(task, WDClientTask))

            state.cfg.log.info("Removing %(count)d tasks", {"count": len(to_remove)})
            for idx in reversed(to_remove):
                state.tasks.pop(idx)

            if (
                had_clients
                and not any(task for task in state.tasks if isinstance(task, WDClientTask))
                and state.cfg.active_marker.exists()
            ):
                ensure_not_there(state.cfg, state.cfg.active_marker)
                state.cfg.log.info("Removed the active marker")

        if not state.tasks:
            state.cfg.log.warning("Tick: no tasks left!")
            return False

        state.cfg.log.debug("Tick!")
        return True


@handle_msg.register
async def handle_msg_new_conn(msg: NewConnMsg, state: GlobalState) -> bool:
    """Spawn a "read bytes from the new connection" task."""
    if state.panic_mode.is_set():
        state.cfg.log.info("Panic mode, ignoring new client connection")
        return True

    client_id: Final = next(state.client_id)
    client_name: Final = f"client/{client_id}"
    state.cfg.log.info("New client connection %(client_id)d", {"client_id": client_id})

    conn_task: Final = asyncio.create_task(client_read(state, client_id, msg.reader))
    timeout_task: Final = asyncio.create_task(check_heartbeat_timeout(state, client_id))
    client_tasks = [
        WDClientTask(client_name, conn_task, client_id),
        WDClientTimeoutTask(f"{client_name} heartbeat timeout", timeout_task, client_id),
    ]

    async with state.tasks_lock:
        if not any(task for task in state.tasks if isinstance(task, WDClientTask)):
            state.cfg.active_marker.mkdir(mode=0o700)
            state.cfg.log.info("Active marker created")

        state.tasks.extend(client_tasks)

    return True


@handle_msg.register
async def handle_msg_client_done(msg: ClientDoneMsg, state: GlobalState) -> bool:
    """Remove a client connection from the tasks list."""
    client_id: Final = msg.client_id
    async with state.tasks_lock:
        to_remove: Final = list(
            reversed(
                [
                    idx
                    for idx, task in enumerate(state.tasks)
                    if isinstance(task, WDClientTask) and task.client_id == client_id
                ],
            ),
        )
        if to_remove:
            state.cfg.log.info(
                "Removing %(count)d tasks, indices: %(indices)s",
                {"count": len(to_remove), "indices": to_remove},
            )
            for idx in to_remove:
                task = state.tasks.pop(idx)
                task.task.cancel()
            state.cfg.log.info("Left with %(count)d tasks", {"count": len(state.tasks)})

        if not any(task for task in state.tasks if isinstance(task, WDClientTask)):
            ensure_not_there(state.cfg, state.cfg.active_marker)
            state.cfg.log.info("Removed the active marker")

    if not msg.warned:
        state.cfg.log.warning(
            "Triggering panic mode due to unexpected disconnect from client %(client)d",
            {"client": client_id},
        )
        await trigger_panic_mode(state)
    state.cfg.log.info("Cleaned up after client %(client_id)d", {"client_id": client_id})
    return True


@handle_msg.register
async def handle_msg_sig_stop(msg: SigStopMsg, state: GlobalState) -> bool:
    """Cancel all the tasks, go away."""
    state.cfg.log.warning("Somebody told us to go away via signal %(sig)d", {"sig": msg.sig})
    async with state.tasks_lock:
        for task in state.tasks:
            state.cfg.log.warning("- cancelling task %(name)s", {"name": task.name})
            task.task.cancel()

    return True


@handle_msg.register
async def handle_msg_panic(_msg: PanicMsg, state: GlobalState) -> bool:
    """Stop all VMs and PVE services."""
    if not state.panic_mode.is_set():
        state.cfg.log.warning("Panic message, but panic mode not set?")
        return True
    state.cfg.log.info("Handling panic mode")

    state.cfg.log.info("Taking care of VMs and services")
    await asyncio.gather(
        asyncproc.run(
            ["systemctl", "kill", "--signal=SIGKILL", *PVE_HA_SERVICES],
            state.cfg.log,
            timeout=60,
        ),
        _kill_vms(state),
        asyncproc.run(["systemctl", "stop", "storpool_block"], state.cfg.log),
    )
    await restart_pve_services(state)
    await asyncproc.run(
        ["systemctl", "start", *PVE_HA_SERVICES],
        state.cfg.log,
        timeout=60,
    )

    await sp_force_detach(state.cfg)
    await asyncproc.run(["systemctl", "start", "storpool_block"], state.cfg.log)
    state.cfg.log.info("Clearing panic mode")
    state.panic_mode.clear()
    return True


def setup_signalfd_task(state: GlobalState) -> asyncio.Task[None]:
    """Set up the signal handlers and the notification socket pair."""
    sig_socket_send, sig_socket_recv = socket.socketpair()
    state.cfg.log.debug(
        "Signal socket pair: send: %(send)r recv: %(recv)r",
        {"send": sig_socket_send, "recv": sig_socket_recv},
    )

    def handle_signal(sig: int) -> None:
        """Send a 'got a signal' message to the asyncio waiter."""
        sig_socket_send.send(bytes([sig]))

    for sig in (signal.SIGINT, signal.SIGQUIT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, functools.partial(handle_signal, sig))

    return asyncio.create_task(do_signal(state, sig_socket_recv))


async def do_run(cfg: Config) -> None:
    """Accept client connections, listen for their messages, report them."""
    cfg.log.info("Starting up to listen on %(listen)s", {"listen": cfg.listen_socket})

    state: Final = GlobalState(
        cfg=cfg,
        mainq=asyncio.Queue(maxsize=16),
        tasks=[],
        tasks_lock=asyncio.Lock(),
        client_id=itertools.count(),
        panic_mode=asyncio.Event(),
    )

    async with state.tasks_lock:
        signalfd_task: Final = setup_signalfd_task(state)
        timer_task: Final = asyncio.create_task(do_timer(state))
        listen_task: Final = asyncio.create_task(do_listen(state))
        state.tasks.append(WDTask("listen", listen_task))

    while True:
        state.cfg.log.debug(
            "Waiting for something to happen with %(count)d tasks: %(tasks)s",
            {
                "count": len(state.tasks),
                "tasks": ", ".join(f"'{task.name}'" for task in state.tasks),
            },
        )
        msg = await state.mainq.get()
        if await handle_msg(msg, state):
            state.mainq.task_done()
        else:
            break

    for name, task in [("timer", timer_task), ("signalfd", signalfd_task)] + [
        (task.name, task.task) for task in state.tasks
    ]:
        if not task.done():
            state.cfg.log.info("Cancelling the %(name)s task", {"name": name})
            task.cancel()

        state.cfg.log.info("Waiting for the %(name)s task", {"name": name})
        try:
            await task
            state.cfg.log.info("The %(name)s task completed", {"name": name})
        except asyncio.CancelledError:
            state.cfg.log.warning("The %(name)s task was cancelled", {"name": name})

    state.cfg.log.info("Looks like we are done here")


def ensure_not_there(cfg: Config, lsock: pathlib.Path) -> None:
    """Make sure a path does not exist on the filesystem."""
    cfg.log.debug("Making sure %(path)s does not exist on the filesystem", {"path": lsock})
    if lsock.is_symlink():
        lsock.unlink(missing_ok=True)
    elif lsock.is_dir():
        subprocess.check_call(["rm", "-rf", "--", lsock])  # noqa: S603,S607
    elif lsock.exists():
        lsock.unlink(missing_ok=True)


@click.command(name="run")
@click.option("--noop", "-N", is_flag=True, help="no-operation mode; display what would be done")
@click.pass_context
def cmd_run(ctx: click.Context, *, noop: bool) -> None:
    """Accept client connections, listen for their messages, report them."""
    cfg: Final = extract_cfg(ctx)

    if noop:
        cfg.log.info(
            "Would listen on %(listen)s and create the %(active)s marker",
            {"listen": cfg.listen_socket, "active": cfg.active_marker},
        )
        return

    ensure_not_there(cfg, cfg.listen_socket)
    ensure_not_there(cfg, cfg.active_marker)

    asyncio.run(do_run(cfg))


@click.command(name="config")
@click.pass_context
def cmd_show_config(ctx: click.Context) -> None:
    """Show the current configuration settings."""
    cfg: Final = extract_cfg(ctx)

    print(  # noqa: T201
        json.dumps(
            {
                "format": {"version": {"major": 0, "minor": 1}},
                "config": {
                    "paths": {
                        "listen": str(cfg.listen_socket),
                        "active": str(cfg.active_marker),
                    },
                },
            },
        ),
    )


@click.group(name="show")
@click.pass_context
def cmd_show(_ctx: click.Context) -> None:
    """Display program parameters as requested."""


cmd_show.add_command(cmd_show_config)


@click.group(name="sp-watchdog-mux")
@click.option(
    "--features",
    is_flag=True,
    is_eager=True,
    callback=util.arg_features,
    help="display program features information and exit",
)
@click.option(
    "--active-marker",
    "-a",
    type=click.Path(file_okay=True, dir_okay=True, resolve_path=False, path_type=pathlib.Path),
    default=WD_ACTIVE_MARKER,
    help="the path to the directory to use as an active clients marker",
)
@click.option("--debug", "-d", is_flag=True, help="debug mode; display diagnostic output")
@click.option(
    "--listen-socket",
    "-l",
    type=click.Path(file_okay=True, dir_okay=True, resolve_path=False, path_type=pathlib.Path),
    default=WD_SOCK_PATH,
    help="the path to the Unix-domain socket to listen on",
)
@click.pass_context
def main(
    ctx: click.Context,
    *,
    active_marker: pathlib.Path,
    debug: bool,
    features: bool,
    listen_socket: pathlib.Path,
) -> None:
    """Do something, or do something else, who cares anyway."""
    if features:
        sys.exit("Internal error: how did we get to main() with features=True?")

    sys.stdout.reconfigure(line_buffering=True)  # type: ignore[union-attr]

    ctx.ensure_object(ConfigHolder)
    log = util.build_logger(debug=debug)
    config = Config(
        active_marker=active_marker,
        listen_socket=listen_socket,
        log=log,
        debug=debug,
        pve_cluster=util.get_pve_cluster(log),
        sp_ourid=int(spconfig.SPConfig().get("SP_OURID")),
    )
    log.info("Loaded configuration: %s", config)
    ctx.obj.cfg = config


main.add_command(cmd_run)
main.add_command(cmd_show)


if __name__ == "__main__":
    main()
