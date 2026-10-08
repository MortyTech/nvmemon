"""
NVMe Utilization Monitor — FastAPI + Prometheus remote write + query proxy.

Environment variables:
  NVME_NODE_NAME                Node name for Prometheus labels (default: system hostname)
  NVME_GRPC_SERVER_URL          Upstream gRPC server URL (empty disables agent mode)
  NVME_GRPC_LISTEN_PORT         gRPC listen port for incoming agents (0 disables server)
  NVME_SAMPLING_INTERVAL       Sampling interval in seconds (default: 5.0)
  NVME_PROFILES_PATH           Path to JSON profiles file (default: /etc/nvme-profiles.json)
  NVME_PROM_REMOTE_WRITE_URL   Prometheus remote write URL (empty disables push)
  NVME_PROM_QUERY_URL          Prometheus HTTP query base URL (default: derived from remote write)
"""

from __future__ import annotations

import asyncio
import glob
import json
import logging
import os
import re
import socket
import struct
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import grpc
import grpc.aio
import httpx
import snappy
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

import nvme_pb2
import nvme_pb2_grpc


# =========================================================================
# Configuration
# =========================================================================

NODE_NAME = os.environ.get("NVME_NODE_NAME", "").strip()
_NODE_NAME_FROM_ENV = bool(NODE_NAME)
if not NODE_NAME:
    NODE_NAME = socket.gethostname()

GRPC_SERVER_URL = os.environ.get("NVME_GRPC_SERVER_URL", "")
GRPC_LISTEN_PORT = int(os.environ.get("NVME_GRPC_LISTEN_PORT", "0"))

AGENT_VERSION = "1.0.0"
REMOTE_STALE_SECONDS_FACTOR = 3
RELOAD_TIMEOUT_SECONDS = 5.0

PUSH_QUEUE_MAX = 64
PUSH_BATCH_MAX = 8
PUSH_RETRY_BUFFER_MAX = 60  # ~5 minutes of cycles at the default 5s interval
SEND_QUEUE_MAX = 64
REDISCOVERY_INTERVAL_SECONDS = 60.0
DEVICE_REMOVAL_MISSING_CYCLES = 12
REMOTE_NODE_TTL_SECONDS = 600.0
AGENT_RETRY_BACKOFF_SECONDS = 30.0

SAMPLING_INTERVAL = float(os.environ.get("NVME_SAMPLING_INTERVAL", "5.0"))
PROFILES_PATH = Path(os.environ.get("NVME_PROFILES_PATH", "/etc/nvme-profiles.json"))
PROM_REMOTE_WRITE_URL = os.environ.get("NVME_PROM_REMOTE_WRITE_URL", "")
PROM_QUERY_URL = os.environ.get("NVME_PROM_QUERY_URL", "")

if not PROM_QUERY_URL and PROM_REMOTE_WRITE_URL:
    _idx = PROM_REMOTE_WRITE_URL.rfind("/api/v1/")
    if _idx > 0:
        PROM_QUERY_URL = PROM_REMOTE_WRITE_URL[:_idx] + "/api/v1/query_range"

SECTOR_SIZE = 512
SYS_BLOCK = Path("/sys/block")

MAX_POINTS_PER_SERIES = 2000
MAX_RANGE_SECONDS = 30 * 24 * 3600

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("nvme")

if not _NODE_NAME_FROM_ENV:
    logger.warning("NVME_NODE_NAME not set, falling back to hostname: %s", NODE_NAME)


# =========================================================================
# Metric catalog for timeseries
# =========================================================================

METRIC_CATALOG = {
    "concurrency_util": {
        "prom_name": "nvme_util_concurrency_ratio",
        "scale": 100.0,
        "unit": "percent",
        "description": "Average queue depth divided by effective optimal queue depth",
    },
    "iops_util": {
        "prom_name": "nvme_util_iops_ratio",
        "scale": 100.0,
        "unit": "percent",
        "description": "Current total IOPS divided by effective max IOPS",
    },
    "bandwidth_util": {
        "prom_name": "nvme_util_bandwidth_ratio",
        "scale": 100.0,
        "unit": "percent",
        "description": "Current bandwidth divided by effective max bandwidth",
    },
    "busy_ratio": {
        "prom_name": "nvme_busy_ratio",
        "scale": 100.0,
        "unit": "percent",
        "description": "Fraction of wall time with at least one I/O in flight",
    },
    "avg_queue_depth": {
        "prom_name": "nvme_queue_depth_avg",
        "scale": 1.0,
        "unit": "count",
        "description": "Average number of I/Os in flight while device was busy",
    },
    "read_latency": {
        "prom_name": "nvme_read_latency_seconds",
        "scale": 1000.0,
        "unit": "ms",
        "description": "Average read latency",
    },
    "write_latency": {
        "prom_name": "nvme_write_latency_seconds",
        "scale": 1000.0,
        "unit": "ms",
        "description": "Average write latency",
    },
    "combined_latency": {
        "prom_name": "nvme_combined_latency_seconds",
        "scale": 1000.0,
        "unit": "ms",
        "description": "Average latency combining reads and writes",
    },
    "read_iops": {
        "prom_name": "nvme_read_iops",
        "scale": 1.0,
        "unit": "iops",
        "description": "Read operations per second",
    },
    "write_iops": {
        "prom_name": "nvme_write_iops",
        "scale": 1.0,
        "unit": "iops",
        "description": "Write operations per second",
    },
    "total_iops": {
        "prom_name": "nvme_total_iops",
        "scale": 1.0,
        "unit": "iops",
        "description": "Total operations per second",
    },
    "read_bandwidth": {
        "prom_name": "nvme_read_bytes_per_second",
        "scale": 1.0 / 1e6,
        "unit": "MB/s",
        "description": "Read bandwidth",
    },
    "write_bandwidth": {
        "prom_name": "nvme_write_bytes_per_second",
        "scale": 1.0 / 1e6,
        "unit": "MB/s",
        "description": "Write bandwidth",
    },
    "saturation_state": {
        "prom_name": "nvme_saturation_state",
        "scale": 1.0,
        "unit": "enum",
        "description": "0=idle,1=light,2=acceptable,3=saturated,4=sick,5=critical",
        "value_labels": {
            0: "idle", 1: "light", 2: "acceptable",
            3: "saturated", 4: "sick", 5: "critical",
        },
    },
    "ios_in_progress": {
        "prom_name": "nvme_ios_in_progress",
        "scale": 1.0,
        "unit": "count",
        "description": "Instantaneous number of I/Os currently in flight",
    },
}

HEATMAP_TIMESERIES_MAP = {
    "write_latency_ms":    {"prom_name": "nvme_write_latency_seconds",     "scale": 1000.0},
    "read_latency_ms":     {"prom_name": "nvme_read_latency_seconds",      "scale": 1000.0},
    "combined_latency_ms": {"prom_name": "nvme_combined_latency_seconds",  "scale": 1000.0},
    "concurrency_util":    {"prom_name": "nvme_util_concurrency_ratio",    "scale": 1.0},
    "iops_util":           {"prom_name": "nvme_util_iops_ratio",           "scale": 1.0},
    "bandwidth_util":      {"prom_name": "nvme_util_bandwidth_ratio",      "scale": 1.0},
    "busy_ratio":          {"prom_name": "nvme_busy_ratio",                "scale": 1.0},
    "flush_time_share":    {"prom_name": "nvme_flush_time_share",          "scale": 1.0},
    "discard_time_share":  {"prom_name": "nvme_discard_time_share",        "scale": 1.0},
    "temperature_celsius": {"prom_name": "nvme_temperature_celsius",       "scale": 1.0},
}

_STEP_RE = re.compile(r"^\d+(ms|s|m|h|d|w)$")
_STEP_MULT = {"s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0, "w": 604800.0}


def _parse_step_seconds(step: str) -> float:
    if step.endswith("ms"):
        return float(step[:-2]) / 1000.0
    unit = step[-1]
    val = float(step[:-1])
    return val * _STEP_MULT.get(unit, 1.0)


# =========================================================================
# Data model
# =========================================================================

RAW_FIELDS = [
    "read_ios", "read_merges", "read_sectors", "read_ticks_ms",
    "write_ios", "write_merges", "write_sectors", "write_ticks_ms",
    "in_flight",
    "io_ticks_ms", "time_in_queue_ms",
    "discard_ios", "discard_merges", "discard_sectors", "discard_ticks_ms",
    "flush_ios", "flush_ticks_ms",
]

COUNTER_FIELDS = [f for f in RAW_FIELDS if f != "in_flight"]

STATE_NAMES = {
    0: "idle", 1: "light", 2: "healthy",
    3: "saturated", 4: "sick", 5: "critical",
}

def _r(value, digits):
    if value is None:
        return None
    return round(float(value), digits)

SEVERITY_NAMES = ["excellent", "acceptable", "warning", "critical"]


def _severity(value: float, thresholds: list) -> str:
    if value < thresholds[0]:
        return SEVERITY_NAMES[0]
    if value < thresholds[1]:
        return SEVERITY_NAMES[1]
    if value < thresholds[2]:
        return SEVERITY_NAMES[2]
    return SEVERITY_NAMES[3]


def _heatmap_value(metric: str, d: DerivedMetrics) -> Optional[float]:
    mapping = {
        "write_latency_ms":    d.write_latency_seconds * 1000.0,
        "read_latency_ms":     d.read_latency_seconds * 1000.0,
        "combined_latency_ms": d.combined_latency_seconds * 1000.0,
        "concurrency_util":    d.concurrency_util_ratio,
        "iops_util":           d.iops_util_ratio,
        "bandwidth_util":      d.bandwidth_util_ratio,
        "busy_ratio":          d.busy_ratio,
        "flush_time_share":    d.flush_time_share,
        "discard_time_share":  d.discard_time_share,
        "temperature_celsius": d.temperature_celsius,
    }
    return mapping.get(metric)


def _heatmap_cell(metric: str, d: DerivedMetrics, profile: dict) -> dict:
    value = _heatmap_value(metric, d)
    thresholds = profile["heatmap_thresholds"][metric]
    severity = _severity(value, thresholds)
    return {
        "value": _r(value, 4),
        "severity": severity,
        "thresholds": thresholds,
    }

@dataclass
class RawSnapshot:
    read_ios: int = 0
    read_merges: int = 0
    read_sectors: int = 0
    read_ticks_ms: int = 0
    write_ios: int = 0
    write_merges: int = 0
    write_sectors: int = 0
    write_ticks_ms: int = 0
    in_flight: int = 0
    io_ticks_ms: int = 0
    time_in_queue_ms: int = 0
    discard_ios: int = 0
    discard_merges: int = 0
    discard_sectors: int = 0
    discard_ticks_ms: int = 0
    flush_ios: int = 0
    flush_ticks_ms: int = 0
    inflight_read: int = 0
    inflight_write: int = 0
    temperature_celsius: float = 0.0


@dataclass
class DerivedMetrics:
    avg_queue_depth: float = 0.0
    concurrency_util_ratio: float = 0.0
    iops_util_ratio: float = 0.0
    bandwidth_util_ratio: float = 0.0
    read_latency_seconds: float = 0.0
    write_latency_seconds: float = 0.0
    combined_latency_seconds: float = 0.0
    read_iops: float = 0.0
    write_iops: float = 0.0
    total_iops: float = 0.0
    read_bytes_per_second: float = 0.0
    write_bytes_per_second: float = 0.0
    busy_ratio: float = 0.0
    saturation_state: int = 0
    effective_max_iops: float = 0.0
    effective_max_bandwidth: float = 0.0
    effective_optimal_qd: float = 0.0
    discard_iops: float = 0.0
    discard_avg_latency_seconds: float = 0.0
    discarded_megabytes_per_second: float = 0.0
    discard_time_share: float = 0.0

    flush_iops: float = 0.0
    flush_avg_latency_seconds: float = 0.0
    flush_time_share: float = 0.0

    inflight_read: int = 0
    inflight_write: int = 0
    read_concurrency_est: float = 0.0
    write_concurrency_est: float = 0.0

    saturation_hint: Optional[str] = None
    temperature_celsius: float = 0.0



@dataclass
class DeviceState:
    name: str
    model: str
    serial: str
    profile_key: str
    previous_snapshot: Optional[RawSnapshot] = None
    previous_timestamp: Optional[float] = None
    last_derived: Optional[DerivedMetrics] = None
    reset_events: dict = field(default_factory=dict)
    last_reset_at: Optional[float] = None
    missing_cycles: int = 0


@dataclass
class GlobalState:
    host: str
    sampling_interval: float
    profiles_path: Path
    profiles_mtime_ns: int
    profiles_loaded_at: float
    profiles: dict
    devices: dict
    started_at: float
    last_cycle_at: float = 0.0
    last_push_at: float = 0.0
    push_failures: int = 0
    push_queue: Optional[asyncio.Queue] = None
    push_buffer: deque = field(
        default_factory=lambda: deque(maxlen=PUSH_RETRY_BUFFER_MAX)
    )
    remote_nodes: dict = field(default_factory=dict)
    agent_send_queue: Optional[asyncio.Queue] = None


@dataclass
class RemoteDevice:
    name: str
    model: str
    serial: str
    profile_key: str
    status: str
    derived: Optional[DerivedMetrics] = None


@dataclass
class RemoteNode:
    name: str
    agent_version: str
    sampling_interval: float
    profiles: dict
    devices: dict = field(default_factory=dict)
    connected_at: float = 0.0
    last_seen: float = 0.0
    stream_alive: bool = False
    send_queue: Optional[asyncio.Queue] = None
    pending_reloads: dict = field(default_factory=dict)


# =========================================================================
# Profiles
# =========================================================================

_REQUIRED_NOMINAL = {
    "random_read_iops", "random_write_iops",
    "seq_read_bps", "seq_write_bps",
}
_REQUIRED_SATURATION = {
    "optimal_qd", "effective_optimal_qd", "thresholds",
}
_REQUIRED_THRESHOLDS = {
    "busy_ratio_idle",
    "concurrency_light",
    "concurrency_saturated",
    "concurrency_min_for_critical",
    "iops_saturated",
    "iops_sick",
    "latency_warning_ms",
    "latency_critical_ms",
}
_REQUIRED_HINT_THRESHOLDS = {
    "flush_share_heavy",
    "discard_share_heavy",
}
_HEATMAP_METRICS = [
    "write_latency_ms",
    "read_latency_ms",
    "combined_latency_ms",
    "concurrency_util",
    "iops_util",
    "bandwidth_util",
    "busy_ratio",
    "flush_time_share",
    "discard_time_share",
    "temperature_celsius",
]
_REQUIRED_HEATMAP = set(_HEATMAP_METRICS)
def _read_profiles_file(path: Path) -> dict:
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or "profiles" not in data:
        raise ValueError("top-level 'profiles' key missing")
    profs = data["profiles"]
    if not isinstance(profs, dict):
        raise ValueError("'profiles' must be an object")
    return profs


def _validate_profiles(profs: dict, devices: dict) -> Optional[str]:
    for key, p in profs.items():
        if not isinstance(p, dict):
            return f"profile '{key}' is not an object"
        if "model_match" not in p or not isinstance(p["model_match"], str):
            return f"profile '{key}' missing/invalid 'model_match'"
        for section in ("nominal", "realistic"):
            if section not in p or not isinstance(p[section], dict):
                return f"profile '{key}.{section}' must be an object"
            miss = _REQUIRED_NOMINAL - set(p[section].keys())
            if miss:
                return f"profile '{key}.{section}' missing {sorted(miss)}"

        if "saturation" not in p or not isinstance(p["saturation"], dict):
            return f"profile '{key}.saturation' must be an object"
        miss = _REQUIRED_SATURATION - set(p["saturation"].keys())
        if miss:
            return f"profile '{key}.saturation' missing {sorted(miss)}"

        th = p["saturation"].get("thresholds")
        if not isinstance(th, dict):
            return f"profile '{key}.saturation.thresholds' must be an object"

        miss = _REQUIRED_THRESHOLDS - set(th.keys())
        if miss:
            return f"profile '{key}.saturation.thresholds' missing {sorted(miss)}"

        hint_th = p["saturation"].get("hint_thresholds")
        if not isinstance(hint_th, dict):
            return f"profile '{key}.saturation.hint_thresholds' must be an object"

        miss = _REQUIRED_HINT_THRESHOLDS - set(hint_th.keys())
        if miss:
            return f"profile '{key}.saturation.hint_thresholds' missing {sorted(miss)}"

        hm = p.get("heatmap_thresholds")
        if not isinstance(hm, dict):
            return f"profile '{key}.heatmap_thresholds' must be an object"
        miss = _REQUIRED_HEATMAP - set(hm.keys())
        if miss:
            return f"profile '{key}.heatmap_thresholds' missing {sorted(miss)}"
        for mname, arr in hm.items():
            if not isinstance(arr, list) or len(arr) != 3:
                return f"profile '{key}.heatmap_thresholds.{mname}' must be a list of 3 numbers"
            if not all(isinstance(x, (int, float)) for x in arr):
                return f"profile '{key}.heatmap_thresholds.{mname}' must contain only numbers"
            if not (arr[0] <= arr[1] <= arr[2]):
                return f"profile '{key}.heatmap_thresholds.{mname}' must be sorted ascending"


    for dev_name, ds in devices.items():
        if ds.profile_key not in profs:
            return f"device '{dev_name}' references missing profile '{ds.profile_key}'"
    return None


def _match_profile(model: str, profs: dict) -> Optional[str]:
    for key, p in profs.items():
        mm = p.get("model_match", "")
        if mm and mm in model:
            return key
    return None


# =========================================================================
# /sys I/O
# =========================================================================

def _discover_devices() -> list[str]:
    try:
        names = [e.name for e in SYS_BLOCK.iterdir() if e.name.startswith("nvme")]
    except OSError:
        return []
    return sorted(names)


def _read_sys_attr(device: str, attr: str) -> str:
    try:
        return (SYS_BLOCK / device / "device" / attr).read_text().strip()
    except OSError:
        return ""

def _read_inflight(device: str) -> tuple[int, int]:
    try:
        parts = (SYS_BLOCK / device / "inflight").read_text().split()
    except OSError:
        return 0, 0
    if len(parts) < 2:
        return 0, 0
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return 0, 0

def _read_temperature(device: str) -> float:
    paths = glob.glob(f"/sys/block/{device}/device/hwmon*/temp1_input")
    if not paths:
        return 0.0
    try:
        raw = Path(paths[0]).read_text().strip()
        return int(raw) / 1000.0
    except (OSError, ValueError):
        return 0.0

def _read_raw_snapshot(device: str) -> Optional[RawSnapshot]:
    try:
        parts = (SYS_BLOCK / device / "stat").read_text().split()
    except OSError:
        return None
    if len(parts) < 17:
        return None
    try:
        vals = [int(x) for x in parts[:17]]
    except ValueError:
        return None
    snap = RawSnapshot(*vals)
    snap.inflight_read, snap.inflight_write = _read_inflight(device)
    snap.temperature_celsius = _read_temperature(device)
    return snap

# =========================================================================
# Derived metric computation
# =========================================================================

def _compute_derived(
    prev: RawSnapshot,
    cur: RawSnapshot,
    dt: float,
    profile: dict,
) -> tuple[DerivedMetrics, list[str]]:
    deltas: dict[str, int] = {}
    reset_fields: list[str] = []
    for f in COUNTER_FIELDS:
        d = getattr(cur, f) - getattr(prev, f)
        if d < 0:
            reset_fields.append(f)
            d = 0
        deltas[f] = d

    read_ios = deltas["read_ios"]
    write_ios = deltas["write_ios"]
    read_bytes = deltas["read_sectors"] * SECTOR_SIZE
    write_bytes = deltas["write_sectors"] * SECTOR_SIZE
    busy_ms = deltas["io_ticks_ms"]
    weighted_ms = deltas["time_in_queue_ms"]
    read_time_ms = deltas["read_ticks_ms"]
    write_time_ms = deltas["write_ticks_ms"]

    avg_qd = (weighted_ms / busy_ms) if busy_ms > 0 else 0.0
    busy_ratio = min(1.0, busy_ms / (dt * 1000.0)) if dt > 0 else 0.0

    read_iops = read_ios / dt if dt > 0 else 0.0
    write_iops = write_ios / dt if dt > 0 else 0.0
    total_iops = read_iops + write_iops

    read_bps = read_bytes / dt if dt > 0 else 0.0
    write_bps = write_bytes / dt if dt > 0 else 0.0

    total_ios = read_ios + write_ios
    read_lat_s = (read_time_ms / read_ios / 1000.0) if read_ios > 0 else 0.0
    write_lat_s = (write_time_ms / write_ios / 1000.0) if write_ios > 0 else 0.0
    combined_lat_s = (
        (read_time_ms + write_time_ms) / total_ios / 1000.0
    ) if total_ios > 0 else 0.0

    discard_ios = deltas["discard_ios"]
    discard_bytes = deltas["discard_sectors"] * SECTOR_SIZE
    discard_time_ms = deltas["discard_ticks_ms"]

    discard_iops = discard_ios / dt if dt > 0 else 0.0
    discarded_mbps = (discard_bytes / dt / 1e6) if dt > 0 else 0.0
    discard_avg_lat = (discard_time_ms / discard_ios / 1000.0) if discard_ios > 0 else 0.0
    discard_share = (discard_time_ms / (dt * 1000.0)) if dt > 0 else 0.0

    flush_ios = deltas["flush_ios"]
    flush_time_ms = deltas["flush_ticks_ms"]

    flush_iops = flush_ios / dt if dt > 0 else 0.0
    flush_avg_lat = (flush_time_ms / flush_ios / 1000.0) if flush_ios > 0 else 0.0
    flush_share = (flush_time_ms / (dt * 1000.0)) if dt > 0 else 0.0

    if total_ios > 0:
        read_conc_est = avg_qd * (read_ios / total_ios)
        write_conc_est = avg_qd * (write_ios / total_ios)
    else:
        read_conc_est = 0.0
        write_conc_est = 0.0

    real = profile["realistic"]
    sat = profile["saturation"]

    if total_iops > 0:
        r_ratio = read_iops / total_iops
        w_ratio = write_iops / total_iops
    else:
        r_ratio = 0.5
        w_ratio = 0.5

    eff_max_iops = r_ratio * real["random_read_iops"] + w_ratio * real["random_write_iops"]
    eff_max_bw = r_ratio * real["seq_read_bps"] + w_ratio * real["seq_write_bps"]

    opt_qd = sat["effective_optimal_qd"]
    conc_util = (avg_qd / opt_qd) if opt_qd > 0 else 0.0
    iops_util = (total_iops / eff_max_iops) if eff_max_iops > 0 else 0.0
    bw_util = ((read_bps + write_bps) / eff_max_bw) if eff_max_bw > 0 else 0.0

    th = sat["thresholds"]
    lat_ms = combined_lat_s * 1000.0

    if busy_ratio < th["busy_ratio_idle"]:
        state = 0
    elif conc_util < th["concurrency_light"] and lat_ms > th["latency_warning_ms"]:
        state = 4
    elif conc_util >= th["concurrency_min_for_critical"] and lat_ms > th["latency_critical_ms"]:
        state = 5
    elif conc_util >= th["concurrency_saturated"] and iops_util < th["iops_sick"]:
        state = 4
    elif conc_util >= th["concurrency_saturated"] and iops_util >= th["iops_saturated"]:
        state = 3
    elif conc_util < th["concurrency_light"]:
        state = 1
    else:
        state = 2

    hint_th = sat["hint_thresholds"]

    hint = None
    if state in (4, 5):
        if flush_share > hint_th["flush_share_heavy"]:
            hint = "flush_heavy"
        elif discard_share > hint_th["discard_share_heavy"]:
            hint = "discard_heavy"
        elif lat_ms > th["latency_critical_ms"]:
            hint = "latency_high"
        elif state == 4:
            hint = "low_throughput_high_latency"

    return DerivedMetrics(
        avg_queue_depth=avg_qd,
        concurrency_util_ratio=conc_util,
        iops_util_ratio=iops_util,
        bandwidth_util_ratio=bw_util,
        read_latency_seconds=read_lat_s,
        write_latency_seconds=write_lat_s,
        combined_latency_seconds=combined_lat_s,
        read_iops=read_iops,
        write_iops=write_iops,
        total_iops=total_iops,
        read_bytes_per_second=read_bps,
        write_bytes_per_second=write_bps,
        busy_ratio=busy_ratio,
        saturation_state=state,
        effective_max_iops=eff_max_iops,
        effective_max_bandwidth=eff_max_bw,
        effective_optimal_qd=opt_qd,
        discard_iops=discard_iops,
        discard_avg_latency_seconds=discard_avg_lat,
        discarded_megabytes_per_second=discarded_mbps,
        discard_time_share=discard_share,
        flush_iops=flush_iops,
        flush_avg_latency_seconds=flush_avg_lat,
        flush_time_share=flush_share,
        inflight_read=cur.inflight_read,
        inflight_write=cur.inflight_write,
        read_concurrency_est=read_conc_est,
        write_concurrency_est=write_conc_est,
        saturation_hint=hint,
        temperature_celsius=cur.temperature_celsius,
    ), reset_fields


def _derived_to_proto(d: DerivedMetrics) -> nvme_pb2.Derived:
    p = nvme_pb2.Derived()
    p.avg_queue_depth = d.avg_queue_depth
    p.concurrency_util_ratio = d.concurrency_util_ratio
    p.iops_util_ratio = d.iops_util_ratio
    p.bandwidth_util_ratio = d.bandwidth_util_ratio
    p.read_latency_seconds = d.read_latency_seconds
    p.write_latency_seconds = d.write_latency_seconds
    p.combined_latency_seconds = d.combined_latency_seconds
    p.read_iops = d.read_iops
    p.write_iops = d.write_iops
    p.total_iops = d.total_iops
    p.read_bytes_per_second = d.read_bytes_per_second
    p.write_bytes_per_second = d.write_bytes_per_second
    p.busy_ratio = d.busy_ratio
    p.saturation_state = d.saturation_state
    p.effective_max_iops = d.effective_max_iops
    p.effective_max_bandwidth = d.effective_max_bandwidth
    p.effective_optimal_qd = d.effective_optimal_qd
    p.discard_iops = d.discard_iops
    p.discard_avg_latency_seconds = d.discard_avg_latency_seconds
    p.discarded_megabytes_per_second = d.discarded_megabytes_per_second
    p.discard_time_share = d.discard_time_share
    p.flush_iops = d.flush_iops
    p.flush_avg_latency_seconds = d.flush_avg_latency_seconds
    p.flush_time_share = d.flush_time_share
    p.inflight_read = d.inflight_read
    p.inflight_write = d.inflight_write
    p.read_concurrency_est = d.read_concurrency_est
    p.write_concurrency_est = d.write_concurrency_est
    p.saturation_hint = d.saturation_hint or ""
    p.temperature_celsius = d.temperature_celsius
    return p


def _proto_to_derived(p: nvme_pb2.Derived) -> DerivedMetrics:
    return DerivedMetrics(
        avg_queue_depth=p.avg_queue_depth,
        concurrency_util_ratio=p.concurrency_util_ratio,
        iops_util_ratio=p.iops_util_ratio,
        bandwidth_util_ratio=p.bandwidth_util_ratio,
        read_latency_seconds=p.read_latency_seconds,
        write_latency_seconds=p.write_latency_seconds,
        combined_latency_seconds=p.combined_latency_seconds,
        read_iops=p.read_iops,
        write_iops=p.write_iops,
        total_iops=p.total_iops,
        read_bytes_per_second=p.read_bytes_per_second,
        write_bytes_per_second=p.write_bytes_per_second,
        busy_ratio=p.busy_ratio,
        saturation_state=p.saturation_state,
        effective_max_iops=p.effective_max_iops,
        effective_max_bandwidth=p.effective_max_bandwidth,
        effective_optimal_qd=p.effective_optimal_qd,
        discard_iops=p.discard_iops,
        discard_avg_latency_seconds=p.discard_avg_latency_seconds,
        discarded_megabytes_per_second=p.discarded_megabytes_per_second,
        discard_time_share=p.discard_time_share,
        flush_iops=p.flush_iops,
        flush_avg_latency_seconds=p.flush_avg_latency_seconds,
        flush_time_share=p.flush_time_share,
        inflight_read=p.inflight_read,
        inflight_write=p.inflight_write,
        read_concurrency_est=p.read_concurrency_est,
        write_concurrency_est=p.write_concurrency_est,
        saturation_hint=p.saturation_hint or None,
        temperature_celsius=p.temperature_celsius,
    )


# =========================================================================
# Prometheus remote write encoder
# =========================================================================

def _varint(n: int) -> bytes:
    if n < 0:
        raise ValueError("varint can't be negative")
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _ld(field: int, data: bytes) -> bytes:
    return _varint((field << 3) | 2) + _varint(len(data)) + data


def _str(field: int, s: str) -> bytes:
    return _ld(field, s.encode("utf-8"))


def _dbl(field: int, v: float) -> bytes:
    return _varint((field << 3) | 1) + struct.pack("<d", v)


def _i64(field: int, v: int) -> bytes:
    return _varint((field << 3) | 0) + _varint(v)


def _label(name: str, value: str) -> bytes:
    return _str(1, name) + _str(2, value)


def _sample(value: float, ts_ms: int) -> bytes:
    return _dbl(1, value) + _i64(2, ts_ms)


def _encode_write_request(series_list) -> bytes:
    body = b""
    for labels, samples in series_list:
        ts_bytes = b""
        for name, value in labels:
            ts_bytes += _ld(1, _label(name, value))
        for value, ts_ms in samples:
            ts_bytes += _ld(2, _sample(value, ts_ms))
        body += _ld(1, ts_bytes)
    return body


async def _push_to_prometheus(client: httpx.AsyncClient, series_list) -> None:
    if not PROM_REMOTE_WRITE_URL or not series_list:
        return
    body = _encode_write_request(series_list)
    compressed = snappy.compress(body)
    headers = {
        "Content-Encoding": "snappy",
        "Content-Type": "application/x-protobuf",
        "X-Prometheus-Remote-Write-Version": "0.1.0",
    }
    r = await client.post(PROM_REMOTE_WRITE_URL, content=compressed, headers=headers)
    r.raise_for_status()


def _build_push_series(
    state: GlobalState,
    device: str,
    raw: RawSnapshot,
    derived: Optional[DerivedMetrics],
    reset_total: int,
    ts_ms: int,
) -> list:
    labels = [("host", state.host), ("device", device)]
    out: list = []

    def add(name: str, value) -> None:
        out.append(([("__name__", name)] + labels, [(float(value), ts_ms)]))

    add("nvme_reads_completed_total", raw.read_ios)
    add("nvme_read_merges_total", raw.read_merges)
    add("nvme_read_bytes_total", raw.read_sectors * SECTOR_SIZE)
    add("nvme_read_time_seconds_total", raw.read_ticks_ms / 1000.0)
    add("nvme_writes_completed_total", raw.write_ios)
    add("nvme_write_merges_total", raw.write_merges)
    add("nvme_written_bytes_total", raw.write_sectors * SECTOR_SIZE)
    add("nvme_write_time_seconds_total", raw.write_ticks_ms / 1000.0)
    add("nvme_io_time_seconds_total", raw.io_ticks_ms / 1000.0)
    add("nvme_time_in_queue_seconds_total", raw.time_in_queue_ms / 1000.0)
    add("nvme_discards_completed_total", raw.discard_ios)
    add("nvme_discard_merges_total", raw.discard_merges)
    add("nvme_discarded_bytes_total", raw.discard_sectors * SECTOR_SIZE)
    add("nvme_discard_time_seconds_total", raw.discard_ticks_ms / 1000.0)
    add("nvme_flushes_completed_total", raw.flush_ios)
    add("nvme_flush_time_seconds_total", raw.flush_ticks_ms / 1000.0)

    add("nvme_ios_in_progress", raw.in_flight)
    add("nvme_counter_resets", reset_total)
    add("nvme_temperature_celsius", raw.temperature_celsius)

    if derived is not None:
        add("nvme_queue_depth_avg", derived.avg_queue_depth)
        add("nvme_util_concurrency_ratio", derived.concurrency_util_ratio)
        add("nvme_util_iops_ratio", derived.iops_util_ratio)
        add("nvme_util_bandwidth_ratio", derived.bandwidth_util_ratio)
        add("nvme_read_latency_seconds", derived.read_latency_seconds)
        add("nvme_write_latency_seconds", derived.write_latency_seconds)
        add("nvme_combined_latency_seconds", derived.combined_latency_seconds)
        add("nvme_read_iops", derived.read_iops)
        add("nvme_write_iops", derived.write_iops)
        add("nvme_total_iops", derived.total_iops)
        add("nvme_read_bytes_per_second", derived.read_bytes_per_second)
        add("nvme_write_bytes_per_second", derived.write_bytes_per_second)
        add("nvme_busy_ratio", derived.busy_ratio)
        add("nvme_saturation_state", float(derived.saturation_state))
        add("nvme_effective_max_iops", derived.effective_max_iops)
        add("nvme_effective_max_bandwidth", derived.effective_max_bandwidth)
        add("nvme_util_optimal_qd", derived.effective_optimal_qd)
        add("nvme_discard_iops", derived.discard_iops)
        add("nvme_discard_avg_latency_seconds", derived.discard_avg_latency_seconds)
        add("nvme_discard_bytes_per_second", derived.discarded_megabytes_per_second * 1e6)
        add("nvme_discard_time_share", derived.discard_time_share)

        add("nvme_flush_iops", derived.flush_iops)
        add("nvme_flush_avg_latency_seconds", derived.flush_avg_latency_seconds)
        add("nvme_flush_time_share", derived.flush_time_share)

        add("nvme_inflight_read", derived.inflight_read)
        add("nvme_inflight_write", derived.inflight_write)
        add("nvme_read_concurrency_est", derived.read_concurrency_est)
        add("nvme_write_concurrency_est", derived.write_concurrency_est)

    return out


# =========================================================================
# Prometheus query proxy
# =========================================================================

async def _prom_query_range(
    client: httpx.AsyncClient,
    query: str,
    start: float,
    end: float,
    step: str,
) -> dict:
    r = await client.get(
        PROM_QUERY_URL,
        params={"query": query, "start": start, "end": end, "step": step},
    )
    if r.status_code >= 400:
        raise HTTPException(
            502,
            f"prometheus query failed ({r.status_code}): {r.text[:200]}",
        )
    return r.json()


def _extract_points(payload: dict, scale: float) -> list:
    result = payload.get("data", {}).get("result", [])
    if not result:
        return []
    points = []
    for ts, val in result[0].get("values", []):
        try:
            points.append([ts, round(float(val) * scale, 4)])
        except (ValueError, TypeError):
            continue
    return points

def _extract_points_raw(payload: dict, scale: float) -> list:
    result = payload.get("data", {}).get("result", [])
    if not result:
        return []
    points = []
    for ts, val in result[0].get("values", []):
        try:
            points.append((float(ts), float(val) * scale))
        except (ValueError, TypeError):
            continue
    return points

# =========================================================================
# Hot-reload profiles
# =========================================================================

def _do_reload(state: GlobalState) -> tuple[bool, Optional[str]]:
    try:
        mtime_ns = state.profiles_path.stat().st_mtime_ns
    except OSError as e:
        return False, f"stat failed: {e}"
    try:
        new_profs = _read_profiles_file(state.profiles_path)
    except Exception as e:
        return False, f"parse error: {e}"
    err = _validate_profiles(new_profs, state.devices)
    if err is not None:
        return False, err
    state.profiles = new_profs
    state.profiles_mtime_ns = mtime_ns
    state.profiles_loaded_at = time.time()
    logger.info("profile reload applied (%d profiles)", len(new_profs))
    return True, None


def _maybe_reload_profiles(state: GlobalState) -> None:
    try:
        mtime_ns = state.profiles_path.stat().st_mtime_ns
    except OSError:
        return
    if mtime_ns == state.profiles_mtime_ns:
        return
    ok, err = _do_reload(state)
    if not ok:
        logger.error("profile reload rejected: %s", err)


# =========================================================================
# Startup
# =========================================================================

def _init_state() -> GlobalState:
    if not PROFILES_PATH.exists():
        raise RuntimeError(f"profiles file not found: {PROFILES_PATH}")

    profs = _read_profiles_file(PROFILES_PATH)
    mtime_ns = PROFILES_PATH.stat().st_mtime_ns

    devices: dict[str, DeviceState] = {}
    for name in _discover_devices():
        model = _read_sys_attr(name, "model")
        serial = _read_sys_attr(name, "serial")
        key = _match_profile(model, profs)
        if key is None:
            logger.warning(
                "device %s (model=%r) has no matching profile — skipping",
                name, model,
            )
            continue
        devices[name] = DeviceState(
            name=name, model=model, serial=serial, profile_key=key,
        )

    err = _validate_profiles(profs, devices)
    if err is not None:
        raise RuntimeError(f"profile validation failed: {err}")

    return GlobalState(
        host=NODE_NAME,
        sampling_interval=SAMPLING_INTERVAL,
        profiles_path=PROFILES_PATH,
        profiles_mtime_ns=mtime_ns,
        profiles_loaded_at=time.time(),
        profiles=profs,
        devices=devices,
        started_at=time.time(),
        push_queue=asyncio.Queue(maxsize=PUSH_QUEUE_MAX),
    )


def _rediscover_devices(state: GlobalState) -> None:
    present = set(_discover_devices())

    for name in sorted(present - set(state.devices)):
        model = _read_sys_attr(name, "model")
        serial = _read_sys_attr(name, "serial")
        key = _match_profile(model, state.profiles)
        if key is None:
            logger.warning(
                "new device %s (model=%r) has no matching profile — skipping",
                name, model,
            )
            continue
        state.devices[name] = DeviceState(
            name=name, model=model, serial=serial, profile_key=key,
        )
        logger.info("device discovered: %s (profile=%s)", name, key)

    for name in [
        n for n, ds in state.devices.items()
        if n not in present and ds.missing_cycles >= DEVICE_REMOVAL_MISSING_CYCLES
    ]:
        del state.devices[name]
        logger.info("device removed: %s", name)


def _queue_put_drop_oldest(q: asyncio.Queue, item) -> None:
    while True:
        try:
            q.put_nowait(item)
            return
        except asyncio.QueueFull:
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass


async def _sampling_loop(state: GlobalState) -> None:
    last_rediscovery = 0.0
    while True:
        cycle_start = time.monotonic()
        try:
            _maybe_reload_profiles(state)

            if time.monotonic() - last_rediscovery >= REDISCOVERY_INTERVAL_SECONDS:
                _rediscover_devices(state)
                last_rediscovery = time.monotonic()

            ts_ms = int(time.time() * 1000)
            push_series: list = []

            for device, ds in state.devices.items():
                cur = _read_raw_snapshot(device)
                if cur is None:
                    ds.missing_cycles += 1
                    continue
                ds.missing_cycles = 0

                now = time.monotonic()
                if ds.previous_snapshot is not None and ds.previous_timestamp is not None:
                    dt = now - ds.previous_timestamp
                    if dt > 0:
                        derived, resets = _compute_derived(
                            ds.previous_snapshot, cur, dt,
                            state.profiles[ds.profile_key],
                        )
                        if resets:
                            for f in resets:
                                ds.reset_events[f] = ds.reset_events.get(f, 0) + 1
                            ds.last_reset_at = time.time()
                            logger.warning(
                                "counter reset on %s (clamped to zero): %s",
                                device, ",".join(resets),
                            )
                        ds.last_derived = derived

                ds.previous_snapshot = cur
                ds.previous_timestamp = now

                reset_total = sum(ds.reset_events.values())
                push_series.extend(
                    _build_push_series(
                        state, device, cur,
                        ds.last_derived, reset_total, ts_ms,
                    )
                )

            if push_series and PROM_REMOTE_WRITE_URL and state.push_queue is not None:
                _queue_put_drop_oldest(state.push_queue, push_series)

            if state.agent_send_queue is not None:
                _queue_put_drop_oldest(
                    state.agent_send_queue, _make_metrics_push(state)
                )

            state.last_cycle_at = time.time()

        except Exception:
            logger.exception("sampling cycle failed")

        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(0.0, state.sampling_interval - elapsed))


async def _push_loop(state: GlobalState) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            series = await state.push_queue.get()
            batch = [series]
            while not state.push_queue.empty() and len(batch) < PUSH_BATCH_MAX:
                batch.append(state.push_queue.get_nowait())

            # buffered failures go first so ordering roughly follows sample time
            to_push = list(state.push_buffer) + batch
            try:
                await _push_to_prometheus(
                    client, [s for entry in to_push for s in entry]
                )
                state.push_buffer.clear()
                state.last_push_at = time.time()
            except Exception as e:
                state.push_failures += 1
                logger.error("prometheus push failed (%d batch(es) buffered): %s",
                             len(to_push), e)
                # deque(maxlen=...) evicts oldest if the buffer overflows
                for entry in to_push:
                    state.push_buffer.append(entry)


async def _remote_node_reaper(state: GlobalState) -> None:
    while True:
        await asyncio.sleep(REMOTE_NODE_TTL_SECONDS / 2)
        now = time.monotonic()
        for name in list(state.remote_nodes):
            rn = state.remote_nodes[name]
            if not rn.stream_alive and (now - rn.last_seen) > REMOTE_NODE_TTL_SECONDS:
                del state.remote_nodes[name]
                logger.info("reaped disconnected remote node: %s", name)


class NvmeAgentServicer(nvme_pb2_grpc.NvmeAgentServicer):
    def __init__(self, state: GlobalState):
        self.state = state

    async def Stream(self, request_iterator, context):
        send_queue: asyncio.Queue = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
        node_name: Optional[str] = None
        accepted = False

        async def reader():
            nonlocal node_name, accepted
            async for msg in request_iterator:
                kind = msg.WhichOneof("payload")
                if kind == "hello":
                    node_name, accepted = self._handle_hello(
                        msg.hello, send_queue, context
                    )
                    if not accepted:
                        # terminate the writer loop once the ack has been yielded,
                        # so a rejected client can't hold the RPC open
                        await send_queue.put(None)
                        return
                elif kind == "metrics" and accepted and node_name:
                    self._handle_metrics(node_name, msg.metrics)
                elif kind == "reload_response" and node_name:
                    self._handle_reload_response(node_name, msg.reload_response)

        reader_task = asyncio.create_task(reader())
        try:
            while True:
                msg = await send_queue.get()
                if msg is None:
                    return
                yield msg
        finally:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass
            if node_name:
                rn = self.state.remote_nodes.get(node_name)
                if rn and rn.stream_alive:
                    rn.stream_alive = False
                    rn.send_queue = None
                    logger.info("agent disconnected: %s", node_name)

    def _handle_hello(self, hello, send_queue, context):
        name = hello.node_name.strip()
        if not name:
            send_queue.put_nowait(nvme_pb2.ServerMessage(
                hello_ack=nvme_pb2.HelloAck(accepted=False, reason="empty node_name")
            ))
            return None, False

        agent_major = hello.agent_version.split(".")[0] if hello.agent_version else ""
        if not agent_major.isdigit() or agent_major != AGENT_VERSION.split(".")[0]:
            send_queue.put_nowait(nvme_pb2.ServerMessage(
                hello_ack=nvme_pb2.HelloAck(
                    accepted=False,
                    reason=(
                        f"agent version '{hello.agent_version}' is incompatible "
                        f"with server version {AGENT_VERSION}"
                    ),
                )
            ))
            return name, False

        if name == self.state.host:
            send_queue.put_nowait(nvme_pb2.ServerMessage(
                hello_ack=nvme_pb2.HelloAck(
                    accepted=False,
                    reason=f"node_name '{name}' collides with this server's own name",
                )
            ))
            return name, False

        try:
            profiles = json.loads(hello.profiles_json) if hello.profiles_json else {}
        except Exception as e:
            send_queue.put_nowait(nvme_pb2.ServerMessage(
                hello_ack=nvme_pb2.HelloAck(
                    accepted=False, reason=f"invalid profiles_json: {e}"
                )
            ))
            return name, False

        err = _validate_profiles(profiles, {})
        if err is not None:
            send_queue.put_nowait(nvme_pb2.ServerMessage(
                hello_ack=nvme_pb2.HelloAck(
                    accepted=False, reason=f"agent profiles rejected: {err}"
                )
            ))
            return name, False

        now = time.monotonic()
        existing = self.state.remote_nodes.get(name)
        if existing and existing.stream_alive:
            stale = (now - existing.last_seen) > (
                existing.sampling_interval * REMOTE_STALE_SECONDS_FACTOR
            )
            if not stale:
                send_queue.put_nowait(nvme_pb2.ServerMessage(
                    hello_ack=nvme_pb2.HelloAck(
                        accepted=False,
                        reason=f"node_name '{name}' is already claimed by an active stream",
                    )
                ))
                logger.warning("rejected duplicate node_name: %s from %s", name, context.peer())
                return name, False
            logger.warning("reclaiming stale node_name: %s", name)

        self.state.remote_nodes[name] = RemoteNode(
            name=name,
            agent_version=hello.agent_version,
            sampling_interval=hello.sampling_interval_seconds or 5.0,
            profiles=profiles,
            connected_at=now,
            last_seen=now,
            stream_alive=True,
            send_queue=send_queue,
        )
        logger.info("agent connected: %s (v%s) from %s", name, hello.agent_version, context.peer())

        send_queue.put_nowait(nvme_pb2.ServerMessage(
            hello_ack=nvme_pb2.HelloAck(accepted=True)
        ))
        return name, True

    def _handle_metrics(self, node_name: str, push: nvme_pb2.MetricsPush):
        rn = self.state.remote_nodes.get(node_name)
        if rn is None:
            return
        rn.last_seen = time.monotonic()
        devices = {}
        for d in push.devices:
            devices[d.name] = RemoteDevice(
                name=d.name,
                model=d.model,
                serial=d.serial,
                profile_key=d.profile_key,
                status=d.status,
                derived=_proto_to_derived(d.derived),
            )
        rn.devices = devices

    def _handle_reload_response(self, node_name: str, resp: nvme_pb2.ReloadResponse):
        rn = self.state.remote_nodes.get(node_name)
        if rn is None:
            return
        fut = rn.pending_reloads.pop(resp.request_id, None)
        if fut and not fut.done():
            fut.set_result({
                "status": resp.status,
                "reason": resp.reason or None,
                "profile_count": resp.profile_count,
            })


def _make_hello(state: GlobalState) -> nvme_pb2.AgentMessage:
    return nvme_pb2.AgentMessage(hello=nvme_pb2.Hello(
        node_name=state.host,
        agent_version=AGENT_VERSION,
        sampling_interval_seconds=state.sampling_interval,
        profiles_json=json.dumps(state.profiles),
    ))


def _make_metrics_push(state: GlobalState) -> nvme_pb2.AgentMessage:
    devices = []
    for name, ds in state.devices.items():
        d = ds.last_derived
        if d is None:
            continue
        devices.append(nvme_pb2.DeviceMetrics(
            name=ds.name,
            model=ds.model,
            serial=ds.serial,
            profile_key=ds.profile_key,
            status="active" if ds.missing_cycles == 0 else "missing",
            derived=_derived_to_proto(d),
        ))
    return nvme_pb2.AgentMessage(metrics=nvme_pb2.MetricsPush(
        timestamp_ms=int(time.time() * 1000),
        devices=devices,
    ))


async def _agent_loop(state: GlobalState):
    while True:
        rejected = False
        try:
            async with grpc.aio.insecure_channel(GRPC_SERVER_URL) as channel:
                stub = nvme_pb2_grpc.NvmeAgentStub(channel)

                send_queue: asyncio.Queue = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
                state.agent_send_queue = send_queue
                await send_queue.put(_make_hello(state))
                logger.info("agent connecting to %s", GRPC_SERVER_URL)

                async def gen():
                    while True:
                        msg = await send_queue.get()
                        if msg is None:
                            return
                        yield msg

                call = stub.Stream(gen())
                async for server_msg in call:
                    kind = server_msg.WhichOneof("payload")
                    if kind == "hello_ack":
                        if not server_msg.hello_ack.accepted:
                            logger.error(
                                "server rejected this agent: %s — retrying in %.0fs",
                                server_msg.hello_ack.reason,
                                AGENT_RETRY_BACKOFF_SECONDS,
                            )
                            rejected = True
                            break
                        logger.info("agent accepted by server")
                    elif kind == "reload_request":
                        req_id = server_msg.reload_request.request_id
                        ok, err = _do_reload(state)
                        resp = nvme_pb2.AgentMessage(reload_response=nvme_pb2.ReloadResponse(
                            request_id=req_id,
                            status="applied" if ok else "rejected",
                            reason=err or "",
                            profile_count=len(state.profiles),
                        ))
                        await send_queue.put(resp)

        except Exception as e:
            logger.error("agent stream error: %s", e)
        finally:
            state.agent_send_queue = None

        await asyncio.sleep(AGENT_RETRY_BACKOFF_SECONDS if rejected else 5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    state = _init_state()
    app.state.g = state
    tasks = []
    tasks.append(asyncio.create_task(_sampling_loop(state)))

    if PROM_REMOTE_WRITE_URL:
        tasks.append(asyncio.create_task(_push_loop(state)))

    tasks.append(asyncio.create_task(_remote_node_reaper(state)))

    grpc_server = None
    if GRPC_LISTEN_PORT > 0:
        grpc_server = grpc.aio.server()
        nvme_pb2_grpc.add_NvmeAgentServicer_to_server(
            NvmeAgentServicer(state), grpc_server
        )
        grpc_server.add_insecure_port(f"[::]:{GRPC_LISTEN_PORT}")
        await grpc_server.start()
        logger.info("gRPC server listening on :%d", GRPC_LISTEN_PORT)

    if GRPC_SERVER_URL:
        tasks.append(asyncio.create_task(_agent_loop(state)))

    logger.info(
        "started: node=%s local_devices=%d interval=%.1fs remote_write=%s grpc_server=%s grpc_agent=%s",
        state.host, len(state.devices), state.sampling_interval,
        "on" if PROM_REMOTE_WRITE_URL else "off",
        "on" if GRPC_LISTEN_PORT > 0 else "off",
        "on" if GRPC_SERVER_URL else "off",
    )
    try:
        yield
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        if grpc_server:
            await grpc_server.stop(grace=1.0)


# =========================================================================
# FastAPI app
# =========================================================================

app = FastAPI(title="NVMe Util", lifespan=lifespan)


def _g(request: Request) -> GlobalState:
    return request.app.state.g


# -------------------------------------------------------------------------
# Node tree endpoints
# -------------------------------------------------------------------------

def _resolve_node(g: GlobalState, node: str):
    if node == g.host:
        return "local", None
    rn = g.remote_nodes.get(node)
    if rn is None:
        return None, f"node '{node}' not found"
    return "remote", rn


def _is_stale(g: GlobalState, rn: RemoteNode) -> bool:
    return (time.monotonic() - rn.last_seen) > (
        rn.sampling_interval * REMOTE_STALE_SECONDS_FACTOR
    )


@app.get("/health")
async def health(request: Request):
    g = _g(request)
    now = time.time()
    cycle_age = (now - g.last_cycle_at) if g.last_cycle_at else None
    push_age = (now - g.last_push_at) if g.last_push_at else None
    ok = (
        g.last_cycle_at > 0
        and cycle_age is not None
        and cycle_age < g.sampling_interval * 3
    )

    remote = {}
    for name, rn in g.remote_nodes.items():
        remote[name] = {
            "status": "stale" if _is_stale(g, rn) else ("live" if rn.stream_alive else "off"),
            "last_seen_seconds_ago": time.monotonic() - rn.last_seen,
            "devices": len(rn.devices),
            "agent_version": rn.agent_version,
        }

    body = {
        "status": "ok" if ok else "degraded",
        "node": g.host,
        "started_at": g.started_at,
        "uptime_seconds": now - g.started_at,
        "sampling_interval_seconds": g.sampling_interval,
        "local_devices": len(g.devices),
        "remote_nodes": remote,
        "last_cycle_at": g.last_cycle_at or None,
        "last_cycle_age_seconds": cycle_age,
        "last_push_at": g.last_push_at or None,
        "last_push_age_seconds": push_age,
        "push_failures_total": g.push_failures,
        "push_buffered_batches": len(g.push_buffer),
        "prometheus_remote_write": bool(PROM_REMOTE_WRITE_URL),
        "prometheus_query": bool(PROM_QUERY_URL),
        "grpc_listen": GRPC_LISTEN_PORT > 0,
        "grpc_agent": bool(GRPC_SERVER_URL),
    }
    if not ok:
        return JSONResponse(body, status_code=503)
    return body


@app.get("/nodes")
async def list_nodes(request: Request):
    g = _g(request)
    out = [{
        "name": g.host,
        "source": "local",
        "status": "active",
        "device_count": len(g.devices),
    }]
    for name, rn in g.remote_nodes.items():
        out.append({
            "name": name,
            "source": "remote",
            "status": "stale" if _is_stale(g, rn) else ("live" if rn.stream_alive else "off"),
            "device_count": len(rn.devices),
            "agent_version": rn.agent_version,
            "last_seen_seconds_ago": time.monotonic() - rn.last_seen,
        })
    return {"self": g.host, "node_count": len(out), "nodes": out}


@app.get("/nodes/{node}/disks")
async def node_disks(request: Request, node: str):
    g = _g(request)
    kind, obj = _resolve_node(g, node)
    if kind is None:
        raise HTTPException(404, obj)
    out = []
    if kind == "local":
        for name, ds in g.devices.items():
            out.append({
                "name": name,
                "model": ds.model,
                "serial": ds.serial,
                "profile_key": ds.profile_key,
                "status": "active" if ds.missing_cycles == 0 else "missing",
                "reset_events_total": sum(ds.reset_events.values()),
                "last_reset_at": ds.last_reset_at,
            })
    else:
        for name, rd in obj.devices.items():
            out.append({
                "name": name,
                "model": rd.model,
                "serial": rd.serial,
                "profile_key": rd.profile_key,
                "status": rd.status,
            })
    return {"node": node, "source": kind, "disk_count": len(out), "disks": out}


@app.get("/nodes/{node}/summary")
async def node_summary(request: Request, node: str):
    g = _g(request)
    kind, obj = _resolve_node(g, node)
    if kind is None:
        raise HTTPException(404, obj)

    out = []
    if kind == "local":
        for name, ds in g.devices.items():
            d = ds.last_derived
            out.append({
                "name": name,
                "model": ds.model,
                "profile_key": ds.profile_key,
                "status": "active" if ds.missing_cycles == 0 else "missing",
                "concurrency_util_percent": _r(d.concurrency_util_ratio * 100.0, 2) if d else None,
                "iops_util_percent": _r(d.iops_util_ratio * 100.0, 2) if d else None,
                "bandwidth_util_percent": _r(d.bandwidth_util_ratio * 100.0, 2) if d else None,
                "combined_latency_ms": _r(d.combined_latency_seconds * 1000.0, 3) if d else None,
                "total_iops": _r(d.total_iops, 1) if d else None,
                "temperature_celsius": _r(d.temperature_celsius, 2) if d else None,
                "saturation_hint": d.saturation_hint if d else None,
                "flush_time_share_percent": _r(d.flush_time_share * 100.0, 2) if d else None,
                "discard_time_share_percent": _r(d.discard_time_share * 100.0, 2) if d else None,
                "saturation_state": d.saturation_state if d else None,
                "saturation_state_name": (
                    STATE_NAMES.get(d.saturation_state, "unknown") if d else None
                ),
            })
    else:
        for name, rd in obj.devices.items():
            d = rd.derived
            out.append({
                "name": name,
                "model": rd.model,
                "profile_key": rd.profile_key,
                "status": rd.status,
                "concurrency_util_percent": _r(d.concurrency_util_ratio * 100.0, 2) if d else None,
                "iops_util_percent": _r(d.iops_util_ratio * 100.0, 2) if d else None,
                "bandwidth_util_percent": _r(d.bandwidth_util_ratio * 100.0, 2) if d else None,
                "combined_latency_ms": _r(d.combined_latency_seconds * 1000.0, 3) if d else None,
                "total_iops": _r(d.total_iops, 1) if d else None,
                "temperature_celsius": _r(d.temperature_celsius, 2) if d else None,
                "saturation_hint": d.saturation_hint if d else None,
                "flush_time_share_percent": _r(d.flush_time_share * 100.0, 2) if d else None,
                "discard_time_share_percent": _r(d.discard_time_share * 100.0, 2) if d else None,
                "saturation_state": d.saturation_state if d else None,
                "saturation_state_name": (
                    STATE_NAMES.get(d.saturation_state, "unknown") if d else None
                ),
            })
    return {
        "node": node,
        "source": kind,
        "disk_count": len(out),
        "sampled_at": time.time() if kind == "local" else time.time() - (time.monotonic() - obj.last_seen),
        "disks": out,
    }


def _derived_to_dict(d: DerivedMetrics) -> dict:
    return {
        "avg_queue_depth": _r(d.avg_queue_depth, 3),
        "concurrency_util_ratio": _r(d.concurrency_util_ratio, 4),
        "concurrency_util_percent": _r(d.concurrency_util_ratio * 100.0, 2),
        "iops_util_ratio": _r(d.iops_util_ratio, 4),
        "iops_util_percent": _r(d.iops_util_ratio * 100.0, 2),
        "bandwidth_util_ratio": _r(d.bandwidth_util_ratio, 4),
        "bandwidth_util_percent": _r(d.bandwidth_util_ratio * 100.0, 2),

        "read_latency_ms": _r(d.read_latency_seconds * 1000.0, 3),
        "write_latency_ms": _r(d.write_latency_seconds * 1000.0, 3),
        "combined_latency_ms": _r(d.combined_latency_seconds * 1000.0, 3),

        "read_iops": _r(d.read_iops, 1),
        "write_iops": _r(d.write_iops, 1),
        "total_iops": _r(d.total_iops, 1),

        "read_megabytes_per_second": _r(d.read_bytes_per_second / 1e6, 2),
        "write_megabytes_per_second": _r(d.write_bytes_per_second / 1e6, 2),

        "busy_ratio": _r(d.busy_ratio, 4),
        "busy_percent": _r(d.busy_ratio * 100.0, 2),

        "discard_iops": _r(d.discard_iops, 2),
        "discard_avg_latency_ms": _r(d.discard_avg_latency_seconds * 1000.0, 3),
        "discarded_megabytes_per_second": _r(d.discarded_megabytes_per_second, 2),
        "discard_time_share_percent": _r(d.discard_time_share * 100.0, 2),

        "flush_iops": _r(d.flush_iops, 2),
        "flush_avg_latency_ms": _r(d.flush_avg_latency_seconds * 1000.0, 3),
        "flush_time_share_percent": _r(d.flush_time_share * 100.0, 2),

        "inflight_read": d.inflight_read,
        "inflight_write": d.inflight_write,
        "read_concurrency_est": _r(d.read_concurrency_est, 3),
        "write_concurrency_est": _r(d.write_concurrency_est, 3),

        "saturation_state": d.saturation_state,
        "saturation_state_name": STATE_NAMES.get(d.saturation_state, "unknown"),
        "saturation_hint": d.saturation_hint,

        "effective_max_iops": _r(d.effective_max_iops, 1),
        "effective_max_bandwidth": int(round(d.effective_max_bandwidth)),
        "effective_optimal_qd": _r(d.effective_optimal_qd, 1),
        "temperature_celsius": _r(d.temperature_celsius, 2),
    }


@app.get("/nodes/{node}/disks/{device}/stat")
async def node_device_stat(
    request: Request,
    node: str,
    device: str,
    include_raw: bool = Query(False),
):
    g = _g(request)
    kind, obj = _resolve_node(g, node)
    if kind is None:
        raise HTTPException(404, obj)

    if kind == "local":
        ds = g.devices.get(device)
        if ds is None:
            raise HTTPException(404, f"device '{device}' not found on node '{node}'")
        response = {
            "node": node,
            "source": "local",
            "identity": {
                "name": ds.name,
                "model": ds.model,
                "serial": ds.serial,
                "profile_key": ds.profile_key,
                "status": "active" if ds.missing_cycles == 0 else "missing",
                "reset_events_total": sum(ds.reset_events.values()),
                "last_reset_at": ds.last_reset_at,
            },
            "derived": _derived_to_dict(ds.last_derived) if ds.last_derived else None,
            "profile": g.profiles.get(ds.profile_key),
            "raw": None,
            "sample_age_seconds": (
                time.monotonic() - ds.previous_timestamp
                if ds.previous_timestamp else None
            ),
            "warning": None,
        }
        if ds.missing_cycles > 0:
            response["warning"] = (
                f"device absent from /sys for {ds.missing_cycles} consecutive cycles"
            )
        if include_raw and ds.previous_snapshot is not None:
            response["raw"] = {f: getattr(ds.previous_snapshot, f) for f in RAW_FIELDS}
        return response

    rd = obj.devices.get(device)
    if rd is None:
        raise HTTPException(404, f"device '{device}' not found on node '{node}'")
    return {
        "node": node,
        "source": "remote",
        "identity": {
            "name": rd.name,
            "model": rd.model,
            "serial": rd.serial,
            "profile_key": rd.profile_key,
            "status": rd.status,
        },
        "derived": _derived_to_dict(rd.derived) if rd.derived else None,
        "profile": obj.profiles.get(rd.profile_key),
        "raw": None,
        "sample_age_seconds": time.monotonic() - obj.last_seen,
        "warning": "stale" if _is_stale(g, obj) else None,
    }


@app.get("/nodes/{node}/heatmap/current")
async def node_heatmap_current(request: Request, node: str):
    g = _g(request)
    kind, obj = _resolve_node(g, node)
    if kind is None:
        raise HTTPException(404, obj)

    devices_out = []
    if kind == "local":
        for name, ds in g.devices.items():
            d = ds.last_derived
            if d is None:
                devices_out.append({"name": name, "model": ds.model, "status": "active", "cells": None})
                continue
            profile = g.profiles[ds.profile_key]
            devices_out.append({
                "name": name,
                "model": ds.model,
                "status": "active" if ds.missing_cycles == 0 else "missing",
                "cells": {m: _heatmap_cell(m, d, profile) for m in _HEATMAP_METRICS},
            })
    else:
        for name, rd in obj.devices.items():
            d = rd.derived
            if d is None:
                devices_out.append({"name": name, "model": rd.model, "status": rd.status, "cells": None})
                continue
            profile = obj.profiles.get(rd.profile_key)
            if profile is None or "heatmap_thresholds" not in profile:
                devices_out.append({
                    "name": name, "model": rd.model, "status": rd.status,
                    "cells": None, "warning": "missing heatmap_thresholds in remote profile",
                })
                continue
            devices_out.append({
                "name": name,
                "model": rd.model,
                "status": rd.status,
                "cells": {m: _heatmap_cell(m, d, profile) for m in _HEATMAP_METRICS},
            })
    return {
        "node": node,
        "source": kind,
        "metrics": _HEATMAP_METRICS,
        "severity_levels": SEVERITY_NAMES,
        "devices": devices_out,
    }


@app.get("/nodes/{node}/disks/{device}/heatmap")
async def node_device_heatmap(
    request: Request,
    node: str,
    device: str,
    metrics: str = Query(default=",".join(_HEATMAP_METRICS)),
    from_ts: float = Query(..., alias="from"),
    to_ts: Optional[float] = Query(None, alias="to"),
    step: Optional[str] = Query(None),
):
    g = _g(request)
    kind, obj = _resolve_node(g, node)
    if kind is None:
        raise HTTPException(404, obj)
    if not PROM_QUERY_URL:
        raise HTTPException(503, "prometheus query endpoint is not configured")

    metric_names = [m.strip() for m in metrics.split(",") if m.strip()]
    unknown = [m for m in metric_names if m not in HEATMAP_TIMESERIES_MAP]
    if unknown:
        raise HTTPException(400, f"unknown heatmap metrics: {unknown}")

    if kind == "local":
        ds = g.devices.get(device)
        if ds is None:
            raise HTTPException(404, f"device '{device}' not found")
        profile = g.profiles[ds.profile_key]
    else:
        rd = obj.devices.get(device)
        if rd is None:
            raise HTTPException(404, f"device '{device}' not found")
        profile = obj.profiles.get(rd.profile_key)
        if profile is None:
            raise HTTPException(404, f"profile '{rd.profile_key}' unknown for node '{node}'")

    heatmap_th = profile["heatmap_thresholds"]

    now = time.time()
    end = to_ts if to_ts is not None else now
    start = from_ts
    if end <= start:
        raise HTTPException(400, "'to' must be greater than 'from'")
    if end - start > MAX_RANGE_SECONDS:
        raise HTTPException(400, f"range too large; max is {MAX_RANGE_SECONDS}s")

    if step is None:
        span = end - start
        step = f"{max(15, int(span / 500))}s"
    elif not _STEP_RE.match(step):
        raise HTTPException(400, f"invalid step format: {step!r}")

    step_seconds = _parse_step_seconds(step)
    estimated_points = int((end - start) / step_seconds) + 1
    if estimated_points > MAX_POINTS_PER_SERIES:
        raise HTTPException(400, f"estimated {estimated_points} points exceeds limit")

    rows_out: dict = {}
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=3.0)) as client:
        for name in metric_names:
            entry = HEATMAP_TIMESERIES_MAP[name]
            query = f'{entry["prom_name"]}{{host="{node}",device="{device}"}}'
            payload = await _prom_query_range(client, query, start, end, step)
            thresholds = heatmap_th[name]
            cells = [
                {"ts": ts, "value": _r(val, 4), "severity": _severity(val, thresholds)}
                for ts, val in _extract_points_raw(payload, entry["scale"])
            ]
            rows_out[name] = {"unit": name, "thresholds": thresholds, "cells": cells}

    return {
        "node": node,
        "device": device,
        "source": kind,
        "from": start,
        "to": end,
        "step": step,
        "severity_levels": SEVERITY_NAMES,
        "rows": rows_out,
    }


@app.get("/nodes/{node}/disks/{device}/timeseries")
async def node_device_timeseries(
    request: Request,
    node: str,
    device: str,
    metrics: str = Query(...),
    from_ts: float = Query(..., alias="from"),
    to_ts: Optional[float] = Query(None, alias="to"),
    step: Optional[str] = Query(None),
):
    g = _g(request)
    kind, obj = _resolve_node(g, node)
    if kind is None:
        raise HTTPException(404, obj)
    if not PROM_QUERY_URL:
        raise HTTPException(503, "prometheus query endpoint is not configured")

    metric_names = [m.strip() for m in metrics.split(",") if m.strip()]
    unknown = [m for m in metric_names if m not in METRIC_CATALOG]
    if unknown:
        raise HTTPException(400, f"unknown metrics: {unknown}")

    now = time.time()
    end = to_ts if to_ts is not None else now
    start = from_ts
    if end <= start:
        raise HTTPException(400, "'to' must be greater than 'from'")
    if end - start > MAX_RANGE_SECONDS:
        raise HTTPException(400, f"range too large; max is {MAX_RANGE_SECONDS}s")

    if step is None:
        span = end - start
        step = f"{max(15, int(span / 500))}s"
    elif not _STEP_RE.match(step):
        raise HTTPException(400, f"invalid step format: {step!r}")

    step_seconds = _parse_step_seconds(step)
    estimated_points = int((end - start) / step_seconds) + 1
    if estimated_points > MAX_POINTS_PER_SERIES:
        raise HTTPException(400, f"estimated {estimated_points} points exceeds limit")

    series_out: dict = {}
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=3.0)) as client:
        for name in metric_names:
            entry = METRIC_CATALOG[name]
            query = f'{entry["prom_name"]}{{host="{node}",device="{device}"}}'
            payload = await _prom_query_range(client, query, start, end, step)
            series_entry = {
                "unit": entry["unit"],
                "description": entry["description"],
                "points": _extract_points(payload, entry["scale"]),
            }
            if "value_labels" in entry:
                series_entry["value_labels"] = entry["value_labels"]
            series_out[name] = series_entry

    return {
        "node": node,
        "device": device,
        "source": kind,
        "from": start,
        "to": end,
        "step": step,
        "series": series_out,
    }


@app.get("/profiles")
async def list_profiles(request: Request):
    g = _g(request)
    profs_out = {}
    for key, p in g.profiles.items():
        entry = dict(p)
        entry["devices"] = [n for n, ds in g.devices.items() if ds.profile_key == key]
        profs_out[key] = entry
    return {
        "node": g.host,
        "profiles_path": str(g.profiles_path),
        "profiles_mtime_ns": g.profiles_mtime_ns,
        "loaded_at": g.profiles_loaded_at,
        "profiles": profs_out,
    }


@app.post("/profiles/reload")
async def reload_profiles_local(request: Request):
    g = _g(request)
    ok, err = _do_reload(g)
    if not ok:
        raise HTTPException(400, detail={"status": "rejected", "reason": err})
    return {
        "node": g.host,
        "status": "applied",
        "loaded_at": g.profiles_loaded_at,
        "profile_count": len(g.profiles),
    }


@app.post("/reload")
async def reload_all(request: Request):
    g = _g(request)
    results: dict = {}

    ok, err = _do_reload(g)
    results[g.host] = {
        "status": "applied" if ok else "rejected",
        "reason": err,
        "profile_count": len(g.profiles) if ok else None,
    }

    for name, rn in g.remote_nodes.items():
        if not rn.stream_alive or rn.send_queue is None:
            results[name] = {"status": "unreachable", "reason": "stream not active"}
            continue
        if _is_stale(g, rn):
            results[name] = {"status": "unreachable", "reason": "stream stale"}
            continue
        req_id = uuid.uuid4().hex
        fut = asyncio.get_running_loop().create_future()
        rn.pending_reloads[req_id] = fut
        try:
            rn.send_queue.put_nowait(nvme_pb2.ServerMessage(
                reload_request=nvme_pb2.ReloadRequest(request_id=req_id)
            ))
        except asyncio.QueueFull:
            rn.pending_reloads.pop(req_id, None)
            results[name] = {"status": "unreachable", "reason": "send queue full"}
            continue
        try:
            resp = await asyncio.wait_for(fut, timeout=RELOAD_TIMEOUT_SECONDS)
            results[name] = resp
        except asyncio.TimeoutError:
            rn.pending_reloads.pop(req_id, None)
            results[name] = {"status": "timeout"}
        except Exception as e:
            rn.pending_reloads.pop(req_id, None)
            results[name] = {"status": "error", "reason": str(e)}

    return {"results": results}


@app.get("/timeseries/metrics")
async def timeseries_metrics():
    return {
        "metrics": {
            name: {
                "unit": entry["unit"],
                "description": entry["description"],
                **({"value_labels": entry["value_labels"]} if "value_labels" in entry else {}),
            }
            for name, entry in METRIC_CATALOG.items()
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
