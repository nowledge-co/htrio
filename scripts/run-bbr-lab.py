#!/usr/bin/env python3
"""Collect portable BBR receipts or run isolated Docker/netem experiments."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import subprocess
import sys
import tarfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
PROFILES = {
    "clean": dict(
        delay_ms=20, jitter_ms=0, loss_percent=0, rate_mbit=10, limit_packets=128
    ),
    "long-rtt": dict(
        delay_ms=80, jitter_ms=0, loss_percent=0, rate_mbit=10, limit_packets=256
    ),
    "lossy": dict(
        delay_ms=20, jitter_ms=5, loss_percent=1, rate_mbit=10, limit_packets=128
    ),
    "slow": dict(
        delay_ms=40, jitter_ms=5, loss_percent=0.1, rate_mbit=2, limit_packets=64
    ),
}


def command(args, **kwargs):
    return subprocess.run(
        args, text=True, capture_output=True, check=True, **kwargs
    ).stdout


def revision():
    return command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip()


def write_json(path, value):
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def receipt_run(args, output, argv, environment=None):
    with (
        (output / "receipts.jsonl").open("w") as receipts,
        (output / "client.stderr").open("w") as errors,
    ):
        result = subprocess.run(
            argv,
            env=environment,
            stdout=receipts,
            stderr=errors,
            timeout=3 * args.runs * 121 + 60,
        )
    summary = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/summarize-bbr-lab.py"),
            str(output / "receipts.jsonl"),
        ],
        text=True,
        capture_output=True,
    )
    (output / "summary.jsonl").write_text(summary.stdout, encoding="utf-8")
    (output / "summary.stderr").write_text(summary.stderr, encoding="utf-8")
    records = [
        json.loads(line)
        for line in (output / "receipts.jsonl").read_text().splitlines()
    ]
    expected = [
        (trial + 1, position + 1, ("cubic", "bbr1", "bbr3")[(trial + position) % 3])
        for trial in range(args.runs)
        for position in range(3)
    ]
    actual = [
        (record["trial"], record["position"], record["algorithm"]) for record in records
    ]
    return dict(
        client_exit_code=result.returncode,
        summary_exit_code=summary.returncode,
        expected_attempts=len(expected),
        recorded_attempts=len(records),
        complete=actual == expected,
        successful_attempts=sum(record["status"] == "ok" for record in records),
    )


def archive(output, metadata):
    write_json(output / "manifest.json", metadata)
    path = output.with_name(output.name + ".tar.gz")
    with tarfile.open(path, "w:gz") as bundle:
        bundle.add(output, arcname=output.name)
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name(path.name + ".sha256").write_text(f"{checksum}  {path.name}\n")
    print(f"Results: {output}\nSend back: {path}\nSHA256: {checksum}")


def collect(args, output, metadata):
    binary = Path(args.binary).resolve()
    if not binary.is_file():
        raise ValueError("build bbr_lab first; see docs/BBR_LAB.md, or pass --binary")
    if not os.environ.get("HTRIO_LAB_TOKEN"):
        raise ValueError("set HTRIO_LAB_TOKEN to the server's shared token")
    metadata.update(
        mode="real-network",
        network=args.network,
        url=args.url,
        certificate_sha256=hashlib.sha256(Path(args.ca).read_bytes()).hexdigest(),
        binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
    )
    metadata["measurement"] = receipt_run(
        args,
        output,
        [
            str(binary),
            "run",
            args.url,
            str(Path(args.ca).resolve()),
            args.network,
            str(args.bytes),
            str(args.runs),
        ],
    )
    result = metadata["measurement"]
    return (
        result["client_exit_code"] == 0
        and result["summary_exit_code"] == 0
        and result["complete"]
    )


def docker_exec(container, *argv):
    return command(["docker", "exec", container, *argv], timeout=30)


def configure(container, profile, seed, output):
    docker_exec(
        container,
        "ethtool",
        "-K",
        "eth0",
        "gro",
        "off",
        "gso",
        "off",
        "tso",
        "off",
        "tx-udp-segmentation",
        "off",
    )
    offloads = docker_exec(container, "ethtool", "-k", "eth0")
    (output / f"{container.rsplit('-', 1)[-1]}-offloads.txt").write_text(offloads)
    for feature in (
        "generic-segmentation-offload",
        "generic-receive-offload",
        "tx-udp-segmentation",
    ):
        if not re.search(rf"^{feature}: off(?:\s|$)", offloads, re.MULTILINE):
            raise ValueError(
                f"{container}: {feature} must be disabled for packet-level impairment"
            )
    argv = [
        "tc",
        "qdisc",
        "replace",
        "dev",
        "eth0",
        "root",
        "netem",
        "limit",
        str(profile["limit_packets"]),
        "delay",
        f"{profile['delay_ms']}ms",
    ]
    if profile["jitter_ms"]:
        argv += [f"{profile['jitter_ms']}ms"]
    argv += [
        "rate",
        f"{profile['rate_mbit']}mbit",
        "loss",
        "random",
        f"{profile['loss_percent']}%",
        "seed",
        str(seed),
    ]
    docker_exec(container, *argv)


def snapshot(containers, output, suffix):
    for container in containers:
        role = container.rsplit("-", 1)[-1]
        (output / f"{role}-qdisc-{suffix}.json").write_text(
            docker_exec(container, "tc", "-s", "-j", "qdisc", "show", "dev", "eth0")
        )


def netem(args, output, metadata):
    metadata.update(
        mode="docker-netem",
        profiles=args.profiles,
        seed=args.seed,
        container_limits=dict(cpus=2, memory_mib=512, pids=128),
    )
    metadata["docker"] = json.loads(
        command(["docker", "info", "--format", "{{json .}}"])
    )
    # Record only the environment details needed to interpret measurements.
    metadata["docker"] = {
        key: metadata["docker"][key]
        for key in (
            "ServerVersion",
            "KernelVersion",
            "OperatingSystem",
            "Architecture",
            "NCPU",
            "MemTotal",
            "SystemTime",
        )
    }
    if not args.skip_build:
        print("Building the Linux lab image with two Cargo jobs...", flush=True)
        with (output / "build.log").open("w") as log:
            subprocess.run(
                [
                    "docker",
                    "build",
                    "--build-arg",
                    f"HTRIO_LAB_REVISION={metadata['source_revision']}",
                    "-f",
                    str(ROOT / "tools/bbr-lab/Dockerfile"),
                    "-t",
                    args.image,
                    str(ROOT),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
    metadata["image"] = json.loads(
        command(["docker", "image", "inspect", args.image, "--format", "{{json .}}"])
    )
    metadata["image"] = {
        key: metadata["image"][key]
        for key in ("Id", "RepoDigests", "Os", "Architecture")
    }
    prefix = "htrio-lab-" + uuid.uuid4().hex[:12]
    containers = []
    network_created = False
    environment = dict(os.environ, HTRIO_LAB_TOKEN=secrets.token_hex(24))
    measurements = {}
    metadata["measurements"] = measurements
    try:
        command(["docker", "network", "create", "--internal", prefix])
        network_created = True
        server, client = prefix + "-server", prefix + "-client"
        for name in (server, client):
            command(
                [
                    "docker",
                    "run",
                    "--detach",
                    "--name",
                    name,
                    "--network",
                    prefix,
                    "--network-alias",
                    "bbr-server" if name == server else "bbr-client",
                    "--cap-drop",
                    "ALL",
                    "--cap-add",
                    "NET_ADMIN",
                    "--cap-add",
                    "NET_RAW",
                    "--cpus",
                    "2",
                    "--memory",
                    "512m",
                    "--pids-limit",
                    "128",
                    "--env",
                    "HTRIO_LAB_TOKEN",
                    args.image,
                    "sleep",
                    "infinity",
                ],
                env=environment,
            )
            containers.append(name)
        # The background process has no host port, host mount, or host network access.
        command(
            [
                "docker",
                "exec",
                "--detach",
                server,
                "sh",
                "-c",
                "bbr_lab serve 0.0.0.0:4433 bbr-server /tmp/lab-ca.pem >/tmp/server.log 2>&1",
            ]
        )
        for attempt in range(100):
            ready = subprocess.run(
                ["docker", "exec", server, "test", "-s", "/tmp/lab-ca.pem"],
                capture_output=True,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.1)
        else:
            raise ValueError("lab server did not become ready")
        command(
            ["docker", "cp", server + ":/tmp/lab-ca.pem", str(output / "lab-ca.pem")]
        )
        command(
            ["docker", "cp", str(output / "lab-ca.pem"), client + ":/tmp/lab-ca.pem"]
        )
        for name in args.profiles:
            directory = output / name
            directory.mkdir()
            profile = PROFILES[name]
            write_json(
                directory / "profile.json",
                dict(profile, client_seed=args.seed, server_seed=args.seed + 1),
            )
            print(
                f"Measuring {name}: {args.runs} uploads per algorithm, {args.bytes} bytes each",
                flush=True,
            )
            for offset, container in enumerate((client, server)):
                configure(container, profile, args.seed + offset, directory)
            snapshot(containers, directory, "before")
            ping = docker_exec(
                client, "ping", "-n", "-c", "5", "-i", "0.2", "-W", "3", "bbr-server"
            )
            (directory / "ping.txt").write_text(ping)
            rtt = re.search(r"= ([0-9.]+)/([0-9.]+)/", ping)
            if not rtt or float(rtt[2]) < profile["delay_ms"]:
                raise ValueError(
                    f"{name}: measured RTT does not confirm bidirectional delay"
                )
            result = receipt_run(
                args,
                directory,
                [
                    "docker",
                    "exec",
                    client,
                    "bbr_lab",
                    "run",
                    "https://bbr-server:4433/",
                    "/tmp/lab-ca.pem",
                    "netem-" + name,
                    str(args.bytes),
                    str(args.runs),
                ],
            )
            measurements[name] = result
            snapshot(containers, directory, "after")
            (directory / "container-stats.jsonl").write_text(
                command(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{json .}}",
                        *containers,
                    ]
                )
            )
        return all(
            value["client_exit_code"] == 0
            and value["summary_exit_code"] == 0
            and value["complete"]
            for value in measurements.values()
        )
    finally:
        for container in reversed(containers):
            if container.endswith("-server"):
                log = subprocess.run(
                    ["docker", "exec", container, "cat", "/tmp/server.log"],
                    text=True,
                    capture_output=True,
                )
                (output / "server.log").write_text(log.stdout + log.stderr)
            subprocess.run(["docker", "rm", "--force", container], capture_output=True)
        if network_created:
            subprocess.run(["docker", "network", "rm", prefix], capture_output=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    real = subparsers.add_parser(
        "collect", help="collect receipts on the current real network"
    )
    real.add_argument("url")
    real.add_argument("ca")
    real.add_argument("network")
    real.add_argument("--binary", default=str(ROOT / "target/release/examples/bbr_lab"))
    simulated = subparsers.add_parser(
        "netem", help="build and run an isolated Linux Docker experiment"
    )
    simulated.add_argument("--image", default="htrio-bbr-lab:local")
    simulated.add_argument(
        "--skip-build",
        action="store_true",
        help="reuse a previously built image; its ID is recorded",
    )
    simulated.add_argument(
        "--profiles", nargs="+", choices=PROFILES, default=list(PROFILES)
    )
    simulated.add_argument("--seed", type=int, default=7)
    for child in (real, simulated):
        child.add_argument("--bytes", type=int, default=2 * 1024 * 1024)
        child.add_argument("--runs", type=int, default=3)
        child.add_argument("--output", type=Path)
        child.add_argument(
            "--note",
            default="",
            help="access type, location pair, VPN and competing traffic",
        )
    args = parser.parse_args()
    if not 1 <= args.bytes <= 64 * 1024 * 1024 or not 1 <= args.runs <= 100:
        parser.error("expected 1..64 MiB and 1..100 repetitions per algorithm")
    if args.mode == "netem" and not 1 <= args.seed < 2**32 - 1:
        parser.error("seed must be in 1..4294967294")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or ROOT / "artifacts" / f"bbr-{args.mode}-{stamp}").resolve()
    if output.exists() or output.with_name(output.name + ".tar.gz").exists():
        parser.error("output directory/archive already exists; choose a new --output")
    output.mkdir(parents=True)
    metadata = dict(
        schema=1,
        started_utc=stamp,
        source_revision=revision(),
        source_dirty=bool(command(["git", "-C", str(ROOT), "status", "--porcelain"])),
        platform=platform.platform(),
        arch=platform.machine(),
        bytes=args.bytes,
        runs=args.runs,
        note=args.note,
    )
    success = False
    try:
        success = (collect if args.mode == "collect" else netem)(args, output, metadata)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        metadata["error"] = str(error)
        if isinstance(error, subprocess.CalledProcessError):
            metadata["command_stderr"] = error.stderr
        print(str(error), file=sys.stderr)
    except KeyboardInterrupt:
        metadata["error"] = "interrupted by operator"
    finally:
        metadata["complete"] = success
        metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
        archive(output, metadata)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
