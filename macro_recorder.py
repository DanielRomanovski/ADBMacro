#!/usr/bin/env python3
"""
BlueStacks Macro Recorder
~~~~~~~~~~~~~~~~~~~~~~~~~
Record and manage keyboard/mouse macros for BlueStacks 5 Android emulator.

Usage:
    python macro_recorder.py

Requirements:
    pip install pynput
"""

from __future__ import annotations

import copy
import json
import subprocess
import struct
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, simpledialog
import tkinter as tk
import tkinter.ttk as ttk

# ── pynput (required for recording only — playback uses ADB) ────────────────
try:
    from pynput import keyboard as _pkbd, mouse as _pmou
    PYNPUT_OK = True
except ImportError:
    PYNPUT_OK = False

# ── Paths & constants ─────────────────────────────────────────────────────────
APP_DIR       = Path(__file__).parent
PROFILES_DIR  = APP_DIR / "profiles"
PROFILES_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_DEVICE = "127.0.0.1:5555"
MOVE_THROTTLE  = 0.016  # ~60 fps mouse-move capture for smooth drag replay


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _key_to_str(key) -> str:
    """Convert a pynput Key object to a readable string."""
    try:
        c = key.char
        return c if c is not None else str(key).replace("Key.", "")
    except AttributeError:
        return str(key).replace("Key.", "")


def collapse_mouse_moves(actions: list[dict]) -> list[dict]:
    """Merge consecutive mouse_move events into a single mouse_path action.

    This dramatically reduces the number of cards shown in the editor while
    keeping full fidelity for playback: each mouse_path stores all original
    points and expands back to individual move frames in preprocess().
    """
    result: list[dict] = []
    i = 0
    while i < len(actions):
        act = actions[i]
        if act.get("type") != "mouse_move":
            result.append(act)
            i += 1
            continue
        # Collect run of consecutive mouse_move events
        pts: list[dict] = []
        while i < len(actions) and actions[i].get("type") == "mouse_move":
            a = actions[i]
            pts.append({"x": a["x"], "y": a["y"],
                        "time": float(a.get("time", 0.0))})
            i += 1
        if len(pts) == 1:
            # Single move — re-emit as mouse_move (not a path)
            result.append({"type": "mouse_move",
                           "x": pts[0]["x"], "y": pts[0]["y"],
                           "time": pts[0]["time"]})
        else:
            result.append({
                "type":     "mouse_path",
                "points":   pts,
                "time":     pts[0]["time"],
                "time_end": pts[-1]["time"],
            })
    return result


def expand_mouse_paths(actions: list[dict]) -> list[dict]:
    """Inverse of collapse_mouse_moves — expand mouse_path back to mouse_moves."""
    result: list[dict] = []
    for act in actions:
        if act.get("type") == "mouse_path":
            for p in act.get("points", []):
                result.append({"type": "mouse_move",
                                "x": p["x"], "y": p["y"],
                                "time": p["time"]})
        else:
            result.append(act)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Profile storage
# ─────────────────────────────────────────────────────────────────────────────

class ProfileManager:
    """Loads, saves, deletes, exports and imports JSON profiles."""

    def __init__(self, directory: Path) -> None:
        self.dir = directory

    def load_all(self) -> list[dict]:
        profiles: list[dict] = []
        for path in sorted(self.dir.glob("*.json")):
            try:
                with open(path, encoding="utf-8") as fh:
                    data = json.load(fh)
                data["_file"] = str(path)
                profiles.append(data)
            except Exception:
                pass
        return profiles

    def save(self, profile: dict) -> None:
        if "_file" not in profile:
            profile["_file"] = str(self.dir / f"{profile['id']}.json")
        path = Path(profile["_file"])
        payload = {k: v for k, v in profile.items() if k != "_file"}
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)

    def delete(self, profile: dict) -> None:
        fpath = profile.get("_file")
        if fpath:
            try:
                Path(fpath).unlink()
            except FileNotFoundError:
                pass

    def export(self, profiles: list[dict], dest: str) -> None:
        data = [{k: v for k, v in p.items() if k != "_file"} for p in profiles]
        with open(dest, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)

    def import_file(self, src: str) -> None:
        with open(src, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            data = [data]
        for p in data:
            if "id" not in p:
                p["id"] = str(uuid.uuid4())
            # Give imported profiles a fresh file path so they don't overwrite
            p.pop("_file", None)
            self.save(p)


# ─────────────────────────────────────────────────────────────────────────────
# Recorder
# ─────────────────────────────────────────────────────────────────────────────

class Recorder:
    """Global keyboard + mouse listener that collects timestamped actions."""

    def __init__(self, on_stop) -> None:
        self._on_stop    = on_stop
        self._recording  = False
        self._actions: list[dict] = []
        self._t0:    float = 0.0
        self._last_move: float = 0.0
        self._kl = None
        self._ml = None
        self._lock = threading.Lock()

    @property
    def recording(self) -> bool:
        return self._recording

    def start(self) -> None:
        if not PYNPUT_OK:
            raise RuntimeError("pynput is not installed.")
        with self._lock:
            self._actions   = []
            self._t0        = time.perf_counter()
            self._last_move = 0.0
            self._recording = True

        self._ml = _pmou.Listener(
            on_move=self._move,
            on_click=self._click,
            on_scroll=self._scroll,
        )
        self._kl = _pkbd.Listener(
            on_press=self._press,
            on_release=self._release,
        )
        self._ml.start()
        self._kl.start()

    def stop(self) -> None:
        with self._lock:
            if not self._recording:
                return
            self._recording = False
            actions = list(self._actions)

        for lst in (self._ml, self._kl):
            if lst is not None:
                try:
                    lst.stop()
                except Exception:
                    pass

        # Fire callback on a daemon thread (must not call tkinter directly)
        threading.Thread(target=self._on_stop, args=(actions,), daemon=True).start()

    # ── private callbacks ─────────────────────────────────────────────────────

    def _ts(self) -> float:
        return round(time.perf_counter() - self._t0, 4)

    def _push(self, action: dict) -> None:
        with self._lock:
            if self._recording:
                self._actions.append(action)

    def _move(self, x: int, y: int) -> None:
        now = time.perf_counter()
        if now - self._last_move < MOVE_THROTTLE:
            return
        self._last_move = now
        self._push({"type": "mouse_move", "x": x, "y": y, "time": self._ts()})

    def _click(self, x: int, y: int, button, pressed: bool) -> None:
        self._push({
            "type":    "mouse_click",
            "x":       x,
            "y":       y,
            "button":  str(button).replace("Button.", ""),
            "pressed": pressed,
            "time":    self._ts(),
        })

    def _scroll(self, x: int, y: int, dx: int, dy: int) -> None:
        self._push({
            "type": "mouse_scroll",
            "x": x, "y": y,
            "dx": dx, "dy": dy,
            "time": self._ts(),
        })

    def _press(self, key) -> None | bool:
        # F8 = stop recording
        if PYNPUT_OK and key == _pkbd.Key.f8:
            threading.Thread(target=self.stop, daemon=True).start()
            return False   # stop keyboard listener
        self._push({"type": "key_press", "key": _key_to_str(key), "time": self._ts()})
        return None

    def _release(self, key) -> None:
        if PYNPUT_OK and key == _pkbd.Key.f8:
            return   # don't record F8 release
        self._push({"type": "key_release", "key": _key_to_str(key), "time": self._ts()})


# ─────────────────────────────────────────────────────────────────────────────
# Android keycode table  (pynput key name → Android keyevent integer)
# ─────────────────────────────────────────────────────────────────────────────

ANDROID_KEYCODES: dict[str, int] = {
    "space": 62, "enter": 66, "backspace": 67, "delete": 67,
    "tab": 61, "escape": 111,
    "up": 19, "down": 20, "left": 21, "right": 22,
    "home": 3, "end": 123, "page_up": 92, "page_down": 93,
    "f1": 131, "f2": 132, "f3": 133, "f4": 134,
    "f5": 135, "f6": 136, "f7": 137, "f8": 138, "f9": 139, "f10": 140,
    "a": 29, "b": 30, "c": 31, "d": 32, "e": 33, "f": 34, "g": 35,
    "h": 36, "i": 37, "j": 38, "k": 39, "l": 40, "m": 41, "n": 42,
    "o": 43, "p": 44, "q": 45, "r": 46, "s": 47, "t": 48, "u": 49,
    "v": 50, "w": 51, "x": 52, "y": 53, "z": 54,
    "0": 7,  "1": 8,  "2": 9,  "3": 10, "4": 11,
    "5": 12, "6": 13, "7": 14, "8": 15, "9": 16,
    "shift": 59, "shift_l": 59, "shift_r": 60,
    "ctrl_l": 113, "ctrl_r": 114,
    "alt_l": 57, "alt_r": 58,
    "caps_lock": 115,
}


def _find_window_rect(title_substr: str) -> tuple | None:
    """Return (left, top, width, height) of the first visible window whose
    title contains *title_substr* (case-insensitive).  Windows-only."""
    import ctypes
    import ctypes.wintypes

    found: list = []

    @ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)
    def _cb(hwnd, _):
        if ctypes.windll.user32.IsWindowVisible(hwnd):
            ln  = ctypes.windll.user32.GetWindowTextLengthW(hwnd)
            buf = ctypes.create_unicode_buffer(ln + 1)
            ctypes.windll.user32.GetWindowTextW(hwnd, buf, ln + 1)
            if title_substr.lower() in buf.value.lower():
                found.append(hwnd)
        return True

    ctypes.windll.user32.EnumWindows(_cb, 0)
    if not found:
        return None

    rect = ctypes.wintypes.RECT()
    ctypes.windll.user32.GetWindowRect(found[0], ctypes.byref(rect))
    w = rect.right  - rect.left
    h = rect.bottom - rect.top
    return (rect.left, rect.top, w, h) if w > 0 and h > 0 else None


def _get_device_size(device: str) -> tuple | None:
    """Query the Android device resolution via 'adb shell wm size'.
    Returns (width, height) or None on failure."""
    import re
    try:
        r = subprocess.run(
            ["adb", "-s", device, "shell", "wm", "size"],
            capture_output=True, text=True, timeout=5)
        m = re.search(r"(\d{3,5})x(\d{3,5})", r.stdout)
        if m:
            return int(m.group(1)), int(m.group(2))
    except Exception:
        pass
    return None


def _get_event_size(adb_device: str) -> int:
    """Return 16 (32-bit Android) or 24 (64-bit Android) bytes per input_event."""
    try:
        r = subprocess.run(
            ["adb", "-s", adb_device, "shell", "getconf", "LONG_BIT"],
            capture_output=True, text=True, timeout=5)
        if "64" in r.stdout:
            return 24
    except Exception:
        pass
    return 16


def _find_touch_device(adb_device: str, fallback_size: tuple) -> tuple:
    """Discover the Android kernel touch-event device and its coordinate range.

    Returns (event_path, x_max, y_max).
    Falls back to (None, dw-1, dh-1) when detection fails; the Player then
    uses 'input tap/swipe' instead of sendevent.

    BlueStacks 5 typical result: (/dev/input/event7, 1279, 719) or similar.
    """
    import re
    dw, dh = fallback_size
    try:
        r = subprocess.run(
            ["adb", "-s", adb_device, "shell", "getevent", "-p"],
            capture_output=True, text=True, timeout=8)
        lines = r.stdout.splitlines()
    except Exception:
        return (None, dw - 1, dh - 1)

    cur_path = None
    cur_xmax: int | None = None
    cur_ymax: int | None = None
    best: tuple | None   = None   # (path, xmax, ymax)

    for line in lines:
        # Device header:  "add device N: /dev/input/eventX"
        m = re.match(r'\s*add device \d+:\s*(/dev/input/event\d+)', line)
        if m:
            # Save previous device if it had both axes
            if cur_path and cur_xmax is not None and cur_ymax is not None:
                best = (cur_path, cur_xmax, cur_ymax)
            cur_path = m.group(1)
            cur_xmax = cur_ymax = None
            continue
        if cur_path is None:
            continue
        # Match ABS_MT_POSITION_X (0x35) or ABS_X (0x00) — getevent shows hex
        # BlueStacks uses codes 0035 / 0036 which are ABS_MT_POSITION_X/Y
        if 'ABS_MT_POSITION_X' in line or '0035' in line:
            m2 = re.search(r'max\s+(\d+)', line)
            if m2:
                cur_xmax = int(m2.group(1))
        if 'ABS_MT_POSITION_Y' in line or '0036' in line:
            m2 = re.search(r'max\s+(\d+)', line)
            if m2:
                cur_ymax = int(m2.group(1))
        if cur_xmax is not None and cur_ymax is not None:
            best = (cur_path, cur_xmax, cur_ymax)

    if best:
        return best
    if cur_path:
        return (cur_path, dw - 1, dh - 1)
    return (None, dw - 1, dh - 1)


# ─────────────────────────────────────────────────────────────────────────────
# Player  (ADB-based — sends input only to the Android emulator)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Player  — binary input_event injection (zero Java overhead)
# ─────────────────────────────────────────────────────────────────────────────
#
# Strategy:
#   A SINGLE  `adb shell cat > /dev/input/eventX`  process stays open
#   for the whole playback session.  We write pre-packed struct bytes
#   directly to its stdin pipe.  The Linux kernel receives them at full
#   speed – no Java VM, no per-call process spawning (~0 ms per event).
#
#   A second persistent shell handles text commands (key events) which
#   are infrequent so their ~300 ms overhead doesn’t matter.
#
#   Host PC mouse / keyboard are never touched.
# ─────────────────────────────────────────────────────────────────────────────

class Player:

    def __init__(self, on_action, on_done) -> None:
        self._on_action = on_action   # (frame_idx, loop_num, total)
        self._on_done   = on_done
        self._stop_evt  = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def playing(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self,
              frames:      list[dict],
              loop:        bool,
              device:      str,
              event_path:  str | None,
              event_size:  int) -> None:
        self._stop_evt.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(frames, loop, device, event_path, event_size),
            daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_evt.set()

    # ── helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _ev(etype: int, code: int, value: int, esize: int) -> bytes:
        """Pack one kernel input_event struct.
        64-bit Android: ts_sec(Q) ts_usec(Q) type(H) code(H) value(i) = 24 B
        32-bit Android: ts_sec(I) ts_usec(I) type(H) code(H) value(i) = 16 B
        Timestamps are zero; the kernel stamps them on receipt.
        """
        if esize == 24:
            return struct.pack("<QQHHi", 0, 0, etype, code, value)
        return struct.pack("<IIHHi", 0, 0, etype, code, value)

    # ── preprocessing ─────────────────────────────────────────────────────────

    @staticmethod
    def preprocess(
        actions:     list[dict],
        window_rect: tuple,         # (left, top, w, h)  screen px recorded
        device_size: tuple,         # (dw, dh)  Android display px
        touch_info:  tuple | None,  # (event_path, x_max, y_max)  or None
        event_size:  int = 24,
    ) -> list[dict]:
        """Build a list of frames ready for _run.

        Frame keys
        ----------
        time     : float  seconds-from-start when this frame should fire
        orig     : int    original action index (treeview highlight)
        touch    : bytes  binary input_event payload   (touch pipe)
        cmd      : str    adb shell text command       (key pipe)
        duration : float  seconds to sleep             (explicit delay)
        is_fdown : bool   True = finger just went down (cleanup tracking)
        is_fup   : bool   True = finger just went up
        """
        wx, wy, ww, wh = window_rect
        dw, dh = device_size
        esize  = event_size
        E      = Player._ev

        # ─ coordinate maps ───────────────────────────────────────────
        if touch_info is not None:
            ep, xm, ym = touch_info

            def to_t(sx, sy):
                """Screen px → touch-device units."""
                rx = max(0.0, min(1.0, (sx - wx) / ww))
                ry = max(0.0, min(1.0, (sy - wy) / wh))
                return int(rx * xm), int(ry * ym)

        def to_d(sx, sy):
            """Screen px → display px (fallback path)."""
            return (
                int(max(0.0, min(1.0, (sx - wx) / ww)) * (dw - 1)),
                int(max(0.0, min(1.0, (sy - wy) / wh)) * (dh - 1)),
            )

        use_bin = touch_info is not None

        # ─ event builders — Protocol A (slot-less multi-touch) ───────
        # BlueStacks "Virtual Touch" exposes codes 0x35/0x36
        # (ABS_MT_POSITION_X / ABS_MT_POSITION_Y) without BTN_TOUCH,
        # ABS_MT_SLOT, or ABS_MT_TRACKING_ID.
        # Protocol A touch sequence:
        #   Finger down / move:
        #     EV_ABS ABS_MT_POSITION_X <x>
        #     EV_ABS ABS_MT_POSITION_Y <y>
        #     EV_SYN SYN_MT_REPORT     0   ← separates fingers
        #     EV_SYN SYN_REPORT        0   ← commits frame
        #   Finger up (all fingers lifted):
        #     EV_SYN SYN_MT_REPORT     0   ← empty = no fingers
        #     EV_SYN SYN_REPORT        0
        #
        # EV_ABS=3, ABS_MT_POSITION_X=0x35=53, ABS_MT_POSITION_Y=0x36=54
        # EV_SYN=0, SYN_REPORT=0, SYN_MT_REPORT=2
        def fdown(tx, ty):
            return (
                E(3, 53, tx, esize) +   # EV_ABS ABS_MT_POSITION_X
                E(3, 54, ty, esize) +   # EV_ABS ABS_MT_POSITION_Y
                E(0,  2,  0, esize) +   # EV_SYN SYN_MT_REPORT
                E(0,  0,  0, esize)     # EV_SYN SYN_REPORT
            )

        def fmove(tx, ty):
            return (
                E(3, 53, tx, esize) +   # EV_ABS ABS_MT_POSITION_X
                E(3, 54, ty, esize) +   # EV_ABS ABS_MT_POSITION_Y
                E(0,  2,  0, esize) +   # EV_SYN SYN_MT_REPORT
                E(0,  0,  0, esize)     # EV_SYN SYN_REPORT
            )

        def fup():
            return (
                E(0,  2,  0, esize) +   # EV_SYN SYN_MT_REPORT (empty = lift)
                E(0,  0,  0, esize)     # EV_SYN SYN_REPORT
            )

        # ─ main loop ──────────────────────────────────────────────────
        #
        # Binary-path tap/drag strategy:
        #   ALL touch goes through the binary dd pipe — no "input tap".
        #   "input tap" spawns a Java process per call (~300 ms each),
        #   causing queuing/drop on rapid taps.
        #
        #   Pure tap: deferred fdown → guaranteed ≥80 ms dwell → fup
        #   Drag:     deferred fdown → fmoves → position-echo → fup
        #
        # Expand any stored mouse_path groups back to individual moves
        # before processing so the rest of this code is uniform.
        actions = expand_mouse_paths(actions)

        TAP_MIN_DWELL = 0.080   # seconds; matches Android ViewConfiguration tap timeout

        frames: list[dict]         = []
        finger_down                = False
        last_tx = last_ty          = 0
        pending_press: dict | None = None   # {ts, orig, tx, ty}
        drag_active                = False
        drag_start: tuple | None   = None   # fallback path
        drag_wpts:  list           = []
        drag_orig:  int            = 0

        for i, act in enumerate(actions):
            t  = act.get("type", "")
            ts = float(act.get("time", 0.0))

            # ─ click / tap / drag ─────────────────────────────────────
            if t == "mouse_click":
                pressed = act.get("pressed", True)
                if use_bin:
                    tx, ty = to_t(act["x"], act["y"])
                    if pressed:
                        pending_press    = {"ts": ts, "orig": i, "tx": tx, "ty": ty}
                        drag_active      = False
                        finger_down      = True
                        last_tx, last_ty = tx, ty
                    else:   # release
                        finger_down = False
                        if drag_active:
                            # ── drag release ──────────────────────────
                            frames.append({"time": ts, "orig": i,
                                           "touch": fup(), "is_fup": True})
                            drag_active   = False
                            pending_press = None
                        elif pending_press is not None:
                            # ── pure tap (no mouse_move in between) ───
                            pp            = pending_press
                            pending_press = None
                            fdown_ts = pp["ts"]
                            fup_ts   = max(ts, fdown_ts + TAP_MIN_DWELL)
                            frames.append({"time": fdown_ts,
                                           "orig": pp["orig"],
                                           "touch": fdown(pp["tx"], pp["ty"]),
                                           "is_fdown": True})
                            frames.append({"time": fup_ts, "orig": i,
                                           "touch": fup(), "is_fup": True})
                else:
                    ax, ay = to_d(act["x"], act["y"])
                    if pressed:
                        drag_start  = (ax, ay, ts)
                        drag_wpts   = [(ax, ay, ts)]
                        drag_orig   = i
                        finger_down = True
                    else:
                        finger_down = False
                        if drag_start is not None:
                            drag_wpts.append((ax, ay, ts))
                            sx0, sy0, st = drag_start
                            total_d = sum(
                                (((drag_wpts[j][0] - drag_wpts[j-1][0]) ** 2
                                  + (drag_wpts[j][1] - drag_wpts[j-1][1]) ** 2) ** 0.5)
                                for j in range(1, len(drag_wpts))
                            )
                            dur_ms  = max(50, int((ts - st) * 1000))
                            shell   = (f"input tap {sx0} {sy0}" if total_d <= 15
                                       else f"input swipe {sx0} {sy0} {ax} {ay} {dur_ms}")
                            frames.append({"time": st, "orig": drag_orig,
                                           "cmd": shell})
                            drag_start = None
                            drag_wpts  = []

            # ─ move (drag waypoint) ───────────────────────────────────
            elif t in ("mouse_move", "mouse_path"):
                # mouse_path is a collapsed group of mouse_move points stored
                # as {"type":"mouse_path","points":[{"x":..,"y":..,"time":..}],
                #     "time":<first>, "time_end":<last>}
                # Expand it the same way as individual mouse_moves.
                move_pts: list[dict]
                if t == "mouse_path":
                    move_pts = act.get("points", [])
                else:
                    move_pts = [{"x": act["x"], "y": act["y"], "time": ts}]

                for mp in move_pts:
                    mp_ts = float(mp.get("time", ts))
                    if use_bin:
                        if finger_down:
                            tx, ty = to_t(mp["x"], mp["y"])
                            last_tx, last_ty = tx, ty
                            if pending_press is not None:
                                pp            = pending_press
                                pending_press = None
                                drag_active   = True
                                frames.append({"time": pp["ts"], "orig": pp["orig"],
                                               "touch": fdown(pp["tx"], pp["ty"]),
                                               "is_fdown": True})
                            frames.append({"time": mp_ts, "orig": i,
                                           "touch": fmove(tx, ty)})
                    else:
                        if drag_start is not None:
                            ax, ay = to_d(mp["x"], mp["y"])
                            if (not drag_wpts
                                    or (drag_wpts[-1][0], drag_wpts[-1][1]) != (ax, ay)):
                                drag_wpts.append((ax, ay, mp_ts))

            # ─ scroll ─────────────────────────────────────────────────
            elif t == "mouse_scroll":
                dy = int(act.get("dy", 0))
                if dy != 0:
                    if use_bin:
                        if finger_down:
                            if drag_active:
                                frames.append({"time": ts - 0.001, "orig": i,
                                               "touch": fup(), "is_fup": True})
                            elif pending_press is not None:
                                pending_press = None
                            finger_down = False
                            drag_active = False
                        tx, ty = to_t(act["x"], act["y"])
                        last_tx, last_ty = tx, ty
                        ym2   = touch_info[2]  # type: ignore[index]
                        ey    = max(0, min(ym2, ty - int(dy * ym2 * 0.30)))
                        mid_y = (ty + ey) // 2
                        frames.extend([
                            {"time": ts,        "orig": i,
                             "touch": fdown(tx, ty), "is_fdown": True},
                            {"time": ts + 0.08, "orig": i,
                             "touch": fmove(tx, mid_y)},
                            {"time": ts + 0.18, "orig": i,
                             "touch": fmove(tx, ey)},
                            {"time": ts + 0.24, "orig": i,
                             "touch": fup(), "is_fup": True},
                        ])
                        last_tx, last_ty = tx, ey
                    else:
                        ax, ay = to_d(act["x"], act["y"])
                        ey     = max(0, min(dh - 1, ay - dy * 350))
                        frames.append({"time": ts, "orig": i,
                                       "cmd": f"input swipe {ax} {ay} {ax} {ey} 200"})

            # ─ key press ──────────────────────────────────────────────
            elif t == "key_press":
                key = act.get("key", "")
                kc  = ANDROID_KEYCODES.get(key.lower())
                if kc is not None:
                    frames.append({"time": ts, "orig": i,
                                    "cmd": f"input keyevent {kc}"})
                elif len(key) == 1 and key.isprintable():
                    safe = key.replace("\\", "\\\\").replace("'", "\\'")
                    frames.append({"time": ts, "orig": i,
                                    "cmd": f"input text '{safe}'"})

            # ─ explicit delay ──────────────────────────────────────────
            elif t == "delay":
                dur = float(act.get("duration", 0.0))
                if dur > 0:
                    frames.append({"time": ts, "orig": i, "duration": dur})

        return frames

    # ── playback thread ──────────────────────────────────────────────────────

    def _run(self,
             frames:     list[dict],
             loop:       bool,
             device:     str,
             event_path: str | None,
             event_size: int) -> None:

        # Raise Windows timer resolution to 1 ms
        high_res = False
        try:
            import ctypes
            ctypes.windll.winmm.timeBeginPeriod(1)
            high_res = True
        except Exception:
            pass

        # ─ open binary touch pipe ─────────────────────────────────────────
        # adb shell -T disables PTY so binary bytes flow without CR/LF mangling.
        # dd bs={event_size} reads exactly one input_event struct at a time and
        # writes it immediately to the kernel device — no stdio buffering.
        # (cat uses a 4096 B fully-buffered stdio buffer, so clicks < 240 B
        # would be held until the buffer filled, causing delayed/batched input.)
        touch_proc = None
        if event_path:
            try:
                touch_proc = subprocess.Popen(
                    ["adb", "-s", device, "shell",
                     f"dd bs={event_size} of={event_path}"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except FileNotFoundError:
                pass

        # ─ open text key-command shell ──────────────────────────────────
        # Used for keyevent / text commands and as full fallback when no
        # binary touch device is available.
        key_proc = None
        has_cmds = any("cmd" in f for f in frames)
        if touch_proc is None or has_cmds:
            try:
                key_proc = subprocess.Popen(
                    ["adb", "-s", device, "shell"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    bufsize=1,
                )
            except FileNotFoundError:
                pass

        if touch_proc is None and key_proc is None:
            self._on_done()
            return

        finger_down = False

        def ensure_touch_proc():
            """Return a live dd process, restarting if the previous one died."""
            nonlocal touch_proc
            if touch_proc is not None and touch_proc.poll() is None:
                return touch_proc          # still alive
            # (Re)start dd
            try:
                touch_proc = subprocess.Popen(
                    ["adb", "-s", device, "shell",
                     f"dd bs={event_size} of={event_path}"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                return touch_proc
            except Exception:
                touch_proc = None
                return None

        try:
            total    = len(frames)
            loop_num = 1
            while True:
                t_start = time.perf_counter()
                for fi, frame in enumerate(frames):
                    if self._stop_evt.is_set():
                        return

                    # Precision deadline wait -----------------------------------
                    # Anchor every frame to the same t_start so overshoot on
                    # frame N automatically shortens frame N+1.  No matter how
                    # long the macro runs, drift cannot accumulate.
                    # Event.wait() handles the bulk sleep; we spin the final
                    # 2 ms for sub-millisecond accuracy without wasting CPU.
                    deadline  = t_start + float(frame.get("time", 0.0))
                    remaining = deadline - time.perf_counter()
                    if remaining > 0.002:
                        if self._stop_evt.wait(remaining - 0.002):
                            return
                    while time.perf_counter() < deadline:   # 2 ms precision spin
                        if self._stop_evt.is_set():
                            return

                    # UI callback AFTER deadline so GUI work does not eat the
                    # sleep budget (its cost is absorbed into the next gap).
                    self._on_action(fi, loop_num, total)

                    # ── dispatch ────────────────────────────────────────
                    touch = frame.get("touch")
                    cmd   = frame.get("cmd")
                    dur   = frame.get("duration")

                    if touch is not None and event_path:
                        proc = ensure_touch_proc()
                        if proc is not None:
                            try:
                                # Patch real wall-clock timestamps into every event.
                                # tv_sec=0 would make Android compute elapsed =
                                # device-uptime (billions of ms), instantly firing
                                # LongPress on every fdown and consuming the event
                                # before onClick (release) fires.
                                now = time.time()
                                if event_size == 24:
                                    ts_bytes = struct.pack("<QQ",
                                        int(now), int((now % 1) * 1_000_000))
                                    ts_len = 16
                                else:
                                    ts_bytes = struct.pack("<II",
                                        int(now), int((now % 1) * 1_000_000))
                                    ts_len = 8
                                patched = bytearray(touch)
                                for j in range(0, len(patched), event_size):
                                    patched[j : j + ts_len] = ts_bytes
                                proc.stdin.write(bytes(patched))  # type: ignore
                                proc.stdin.flush()                # type: ignore
                                if frame.get("is_fdown"):
                                    finger_down = True
                                elif frame.get("is_fup"):
                                    finger_down = False
                            except (BrokenPipeError, OSError):
                                touch_proc = None   # force restart next frame
                    elif cmd is not None and key_proc is not None:
                        try:
                            key_proc.stdin.write(cmd + "\n")  # type: ignore
                            key_proc.stdin.flush()             # type: ignore
                        except (BrokenPipeError, OSError):
                            pass
                    elif dur is not None:
                        self._stop_evt.wait(dur)

                if not loop or self._stop_evt.is_set():
                    break
                loop_num += 1
        finally:
            # Lift stuck finger before closing (Protocol A: empty SYN_MT_REPORT)
            if finger_down and touch_proc is not None:
                try:
                    now = time.time()
                    if event_size == 24:
                        ts_b = struct.pack("<QQ", int(now), int((now % 1) * 1_000_000))
                    else:
                        ts_b = struct.pack("<II", int(now), int((now % 1) * 1_000_000))
                    type_code_val_mt  = struct.pack("<HHi", 0, 2, 0)  # SYN_MT_REPORT
                    type_code_val_syn = struct.pack("<HHi", 0, 0, 0)  # SYN_REPORT
                    payload = ts_b + type_code_val_mt + ts_b + type_code_val_syn
                    touch_proc.stdin.write(payload)  # type: ignore
                    touch_proc.stdin.flush()          # type: ignore
                except Exception:
                    pass

            for proc in (touch_proc, key_proc):
                if proc is None:
                    continue
                try:
                    proc.stdin.close()  # type: ignore
                except Exception:
                    pass
                try:
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

            if high_res:
                try:
                    import ctypes
                    ctypes.windll.winmm.timeEndPeriod(1)
                except Exception:
                    pass

            self._on_done()

# ─────────────────────────────────────────────────────────────────────────────
# Action-edit dialog
# ─────────────────────────────────────────────────────────────────────────────

class ActionEditDialog(tk.Toplevel):
    """Modal dialog for editing a single action."""

    CREATABLE_TYPES = [
        "mouse_click", "mouse_move", "mouse_scroll",
        "key_press", "key_release", "delay",
    ]

    def __init__(self, parent: tk.Misc, action: dict,
                 create_mode: bool = False) -> None:
        super().__init__(parent)
        self.title("Edit Action" if not create_mode else "New Action")
        self.resizable(False, False)
        self.grab_set()
        self.transient(parent)
        self.result: dict | None = None
        self._orig        = action
        self._fields: dict[str, tk.Variable] = {}
        self._create_mode = create_mode
        self._build()
        self.wait_window()

    def _build(self) -> None:
        pad = {"padx": 8, "pady": 4}
        frm = ttk.Frame(self, padding=14)
        frm.grid(sticky="nsew")
        self.columnconfigure(0, weight=1)

        if self._create_mode:
            ttk.Label(frm, text="Action type:",
                      font=("Segoe UI", 9)).grid(
                row=0, column=0, sticky="w", padx=8, pady=(0, 2))
            self._type_var = tk.StringVar(value=self._orig.get("type", "mouse_click"))
            cb = ttk.Combobox(frm, textvariable=self._type_var,
                              values=self.CREATABLE_TYPES,
                              state="readonly", width=18)
            cb.grid(row=0, column=1, sticky="w", padx=8, pady=(0, 2))
            cb.bind("<<ComboboxSelected>>", self._on_type_change)
            self._fields_frm = ttk.Frame(frm)
            self._fields_frm.grid(row=1, column=0, columnspan=2, sticky="nsew")
            self._build_fields(self._fields_frm, pad)
            base_row = 2
        else:
            ttk.Label(frm, text=f"Type: {self._orig['type']}",
                      font=("Segoe UI", 10, "bold")).grid(
                row=0, columnspan=2, sticky="w", pady=(0, 10))
            self._build_fields(frm, pad, start_row=1)
            base_row = 100   # large enough

        sep_row = base_row + len(self._fields) + 1
        ttk.Separator(frm).grid(row=sep_row, columnspan=2,
                                sticky="ew", pady=8)
        btn_frm = ttk.Frame(frm)
        btn_frm.grid(row=sep_row + 1, columnspan=2, sticky="e")
        ttk.Button(btn_frm, text="Save",
                   command=self._save).pack(side="right", padx=4)
        ttk.Button(btn_frm, text="Cancel",
                   command=self.destroy).pack(side="right")

    def _on_type_change(self, _e=None) -> None:
        for w in self._fields_frm.winfo_children():
            w.destroy()
        self._fields.clear()
        t = self._type_var.get()
        defaults = self._defaults_for(t)
        self._orig = {"type": t, **defaults}
        self._build_fields(self._fields_frm, {"padx": 8, "pady": 4})

    @staticmethod
    def _defaults_for(t: str) -> dict:
        if t == "mouse_click":
            return {"x": 0, "y": 0, "button": "left",
                    "pressed": True, "time": 0.0}
        if t == "mouse_move":
            return {"x": 0, "y": 0, "time": 0.0}
        if t == "mouse_scroll":
            return {"x": 0, "y": 0, "dx": 0, "dy": -1, "time": 0.0}
        if t in ("key_press", "key_release"):
            return {"key": "a", "time": 0.0}
        if t == "delay":
            return {"duration": 1.0, "time": 0.0}
        return {"time": 0.0}

    def _build_fields(self, parent, pad, start_row: int = 0) -> None:
        for r, (key, val) in enumerate(self._orig.items(), start=start_row):
            if key == "type":
                continue
            ttk.Label(parent, text=key + ":").grid(
                row=r, column=0, sticky="w", **pad)
            if isinstance(val, bool):
                var: tk.Variable = tk.BooleanVar(value=val)
                widget = ttk.Checkbutton(parent, variable=var)
            else:
                var = tk.StringVar(value=str(val))
                widget = ttk.Entry(parent, textvariable=var, width=28)
            widget.grid(row=r, column=1, sticky="ew", **pad)
            self._fields[key] = var

    def _save(self) -> None:
        t = (self._type_var.get()
             if self._create_mode else self._orig["type"])
        result = {"type": t}
        orig_ref = (self._defaults_for(t)
                    if self._create_mode else self._orig)
        for key, var in self._fields.items():
            raw  = var.get()
            orig = orig_ref.get(key, raw)
            try:
                if isinstance(orig, bool):
                    result[key] = bool(raw)
                elif isinstance(orig, int):
                    result[key] = int(float(raw))
                elif isinstance(orig, float):
                    result[key] = float(raw)
                else:
                    result[key] = raw
            except (ValueError, TypeError):
                result[key] = raw
        self.result = result
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
# DragCardList  — scrollable card list with drag-to-reorder support
# ─────────────────────────────────────────────────────────────────────────────

class DragCardList(tk.Frame):
    """
    A scrollable list of "cards" (tk.Frame) that can be:
      • Clicked to select (single) / Ctrl-/Shift-clicked for multi-select
      • Dragged to reorder
      • Double-clicked to trigger an edit callback

    Public API
    ----------
    set_cards(items)   — rebuild from a list of dicts (action data)
    get_order()        — return the current list of action dicts in display order
    selection()        — return list of currently-selected indices
    select(index)      — programmatically select one card
    clear_playing()    — remove the "playing" highlight from all cards
    set_playing(index) — highlight one card as the currently-playing one
    on_reorder         — callback(new_action_list) fired after drag
    on_edit            — callback(index) fired on double-click
    on_select          — callback(selected_indices) fired on selection change
    """

    _CARD_H     = 54   # px per card
    _PAD        = 3    # gap between cards
    _SEL_BG     = "#cce5ff"
    _PLAY_BG    = "#ffe0a0"
    _CARD_BG    = "#f8f8f8"
    _DRAG_ALPHA = "#d0e8ff"
    _MOVE_FILL  = "#4a90d9"   # colour for path canvas

    def __init__(self, master, on_reorder=None, on_edit=None,
                 on_select=None, **kw):
        super().__init__(master, **kw)
        self.on_reorder = on_reorder
        self.on_edit    = on_edit
        self.on_select  = on_select

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._canvas = tk.Canvas(self, highlightthickness=0, bg="#ececec")
        self._vsb    = ttk.Scrollbar(self, orient="vertical",
                                     command=self._canvas.yview)
        self._canvas.configure(yscrollcommand=self._vsb.set)
        self._canvas.grid(row=0, column=0, sticky="nsew")
        self._vsb.grid(row=0, column=1, sticky="ns")

        self._inner = tk.Frame(self._canvas, bg="#ececec")
        self._win_id = self._canvas.create_window(
            (0, 0), window=self._inner, anchor="nw")

        self._inner.bind("<Configure>", self._on_inner_configure)
        self._canvas.bind("<Configure>", self._on_canvas_configure)
        self._canvas.bind("<MouseWheel>", self._on_mousewheel)

        self._cards:      list[tk.Frame] = []
        self._actions:    list[dict]     = []
        self._selected:   set[int]       = set()
        self._playing_idx: int | None    = None

        # drag state
        self._drag_src:    int | None  = None
        self._drag_ghost:  tk.Frame | None = None
        self._drag_y_off:  int = 0
        self._drag_target: int | None  = None

    # ── public ────────────────────────────────────────────────────────────────

    def set_cards(self, items: list[dict]) -> None:
        self._selected.clear()
        self._playing_idx = None
        for w in self._inner.winfo_children():
            w.destroy()
        self._cards   = []
        self._actions = list(items)
        for i, act in enumerate(self._actions):
            self._cards.append(self._make_card(i, act))
        self._repack()

    def get_order(self) -> list[dict]:
        return list(self._actions)

    def selection(self) -> list[int]:
        return sorted(self._selected)

    def select(self, index: int) -> None:
        self._selected = {index}
        self._refresh_colors()
        if 0 <= index < len(self._cards):
            self._scroll_to(index)
        if self.on_select:
            self.on_select(self.selection())

    def clear_playing(self) -> None:
        self._playing_idx = None
        self._refresh_colors()

    def set_playing(self, index: int) -> None:
        self._playing_idx = index
        self._refresh_colors()
        self._scroll_to(index)

    # ── card factory ─────────────────────────────────────────────────────────

    def _make_card(self, i: int, act: dict) -> tk.Frame:
        frm = tk.Frame(self._inner, bg=self._CARD_BG,
                       relief="solid", bd=1,
                       height=self._CARD_H)
        frm.pack_propagate(False)

        t = act.get("type", "")

        # left colour stripe
        stripe_col = {
            "mouse_click":   "#4a90d9",
            "mouse_scroll":  "#9b59b6",
            "mouse_move":    "#27ae60",
            "mouse_path":    "#27ae60",
            "key_press":     "#e67e22",
            "key_release":   "#e67e22",
            "delay":         "#95a5a6",
        }.get(t, "#aaaaaa")
        tk.Frame(frm, bg=stripe_col, width=6).pack(side="left", fill="y")

        body = tk.Frame(frm, bg=self._CARD_BG)
        body.pack(side="left", fill="both", expand=True, padx=6, pady=4)

        # ── Index badge + type label ──────────────────────────────────
        hdr = tk.Frame(body, bg=self._CARD_BG)
        hdr.pack(fill="x")

        tk.Label(hdr, text=f"#{i + 1}", font=("Segoe UI", 7),
                 fg="#aaaaaa", bg=self._CARD_BG,
                 width=4, anchor="w").pack(side="left")

        type_label = {
            "mouse_click":  "Click",
            "mouse_scroll": "Scroll",
            "mouse_move":   "Move",
            "mouse_path":   "Path  (drag)",
            "key_press":    "Key ▼",
            "key_release":  "Key ▲",
            "delay":        "Delay",
        }.get(t, t)
        tk.Label(hdr, text=type_label, font=("Segoe UI", 9, "bold"),
                 fg="#333333", bg=self._CARD_BG, anchor="w").pack(side="left", padx=4)

        ts = act.get("time", "")
        tk.Label(hdr, text=f"{float(ts):.3f} s" if ts != "" else "",
                 font=("Segoe UI", 7), fg="#888888",
                 bg=self._CARD_BG, anchor="e").pack(side="right", padx=4)

        # ── Detail / minimap ──────────────────────────────────────────
        if t == "mouse_path":
            self._add_path_minimap(body, act)
        else:
            detail = self._fmt_detail(act)
            tk.Label(body, text=detail, font=("Segoe UI", 9),
                     fg="#555555", bg=self._CARD_BG,
                     anchor="w", wraplength=460).pack(fill="x")

        # bind interactions — every child widget must also forward events
        for w in self._iter_descendants(frm):
            w.bind("<Button-1>",        lambda e, idx=i: self._on_click(e, idx))
            w.bind("<Double-Button-1>", lambda e, idx=i: self._on_dbl(e, idx))
            w.bind("<B1-Motion>",       lambda e, idx=i: self._on_drag(e, idx))
            w.bind("<ButtonRelease-1>", lambda e, idx=i: self._on_drop(e, idx))

        frm._action_index = i   # type: ignore[attr-defined]
        return frm

    def _add_path_minimap(self, parent: tk.Frame, act: dict) -> None:
        """Draw a tiny polyline of the recorded path."""
        pts = act.get("points", [])
        if not pts:
            return
        MW, MH = 120, 30
        cv = tk.Canvas(parent, width=MW, height=MH,
                       bg=self._CARD_BG, highlightthickness=0)
        cv.pack(side="left", padx=(0, 6))
        xs = [p["x"] for p in pts]
        ys = [p["y"] for p in pts]
        xmin, xmax = min(xs), max(xs)
        ymin, ymax = min(ys), max(ys)
        dx = max(xmax - xmin, 1)
        dy = max(ymax - ymin, 1)
        margin = 4
        def sc(px, py):
            return (
                margin + int((px - xmin) / dx * (MW - 2 * margin)),
                margin + int((py - ymin) / dy * (MH - 2 * margin)),
            )
        coords: list[int] = []
        for p in pts:
            cx2, cy2 = sc(p["x"], p["y"])
            coords += [cx2, cy2]
        if len(coords) >= 4:
            cv.create_line(*coords, fill=self._MOVE_FILL,
                           width=2, smooth=True)
        # start dot
        sx, sy = sc(pts[0]["x"], pts[0]["y"])
        cv.create_oval(sx-3, sy-3, sx+3, sy+3, fill="#27ae60", outline="")
        # end dot
        ex, ey = sc(pts[-1]["x"], pts[-1]["y"])
        cv.create_oval(ex-3, ey-3, ex+3, ey+3, fill="#e74c3c", outline="")

        n = len(pts)
        tk.Label(parent, text=f"{n} pts\n{float(act.get('time',0)):.3f}→{float(act.get('time_end',0)):.3f}s",
                 font=("Segoe UI", 7), fg="#777777",
                 bg=self._CARD_BG, justify="left").pack(side="left")

    @staticmethod
    def _fmt_detail(act: dict) -> str:
        t = act.get("type", "")
        if t == "mouse_click":
            arrow = "▼ press" if act.get("pressed") else "▲ release"
            return f"x={act.get('x')}, y={act.get('y')}   btn={act.get('button')}  {arrow}"
        if t == "mouse_move":
            return f"x={act.get('x')}, y={act.get('y')}"
        if t == "mouse_scroll":
            return (f"x={act.get('x')}, y={act.get('y')}   "
                    f"dx={act.get('dx')}, dy={act.get('dy')}")
        if t in ("key_press", "key_release"):
            arrow = "▼" if t == "key_press" else "▲"
            return f"key = {act.get('key')}  {arrow}"
        if t == "delay":
            return f"wait  {act.get('duration', 0)} s"
        return str({k: v for k, v in act.items()
                    if k not in ("type", "time")})

    @staticmethod
    def _iter_descendants(widget):
        yield widget
        for child in widget.winfo_children():
            yield from DragCardList._iter_descendants(child)

    # ── interaction ───────────────────────────────────────────────────────────

    def _on_click(self, event, idx: int) -> None:
        if (event.state & 0x0004):   # Ctrl
            if idx in self._selected:
                self._selected.discard(idx)
            else:
                self._selected.add(idx)
        elif (event.state & 0x0001):  # Shift
            if self._selected:
                anchor = min(self._selected)
                lo, hi = min(anchor, idx), max(anchor, idx)
                self._selected = set(range(lo, hi + 1))
            else:
                self._selected = {idx}
        else:
            self._selected = {idx}
        self._refresh_colors()
        if self.on_select:
            self.on_select(self.selection())
        # remember start position for drag detection
        self._drag_src   = idx
        self._drag_y_off = event.y_root - self._cards[idx].winfo_rooty()

    def _on_dbl(self, event, idx: int) -> None:
        self._selected = {idx}
        self._refresh_colors()
        if self.on_edit:
            self.on_edit(idx)

    def _on_drag(self, event, idx: int) -> None:
        if self._drag_src is None:
            return
        # Only start ghost after moving 5px
        gy = event.y_root
        if self._drag_ghost is None:
            if abs(gy - (self._cards[idx].winfo_rooty()
                         + self._drag_y_off)) < 5:
                return
            self._create_ghost(idx)

        # Move ghost
        if self._drag_ghost:
            canvas_y = (gy - self._canvas.winfo_rooty()
                        + self._canvas.yview()[0]
                         * self._inner.winfo_reqheight())
            ghost_y  = canvas_y - self._drag_y_off
            self._drag_ghost.place(
                x=2, y=int(ghost_y),
                width=self._inner.winfo_width() - 4)
            # Calculate drop target
            self._drag_target = self._y_to_index(canvas_y)
            self._refresh_colors()

    def _on_drop(self, event, idx: int) -> None:
        if self._drag_ghost is not None:
            self._drag_ghost.destroy()
            self._drag_ghost = None
            tgt = self._drag_target
            if tgt is not None and tgt != self._drag_src:
                self._do_reorder(self._drag_src, tgt)
        self._drag_src    = None
        self._drag_target = None

    def _create_ghost(self, idx: int) -> None:
        src = self._cards[idx]
        self._drag_ghost = tk.Frame(
            self._inner,
            bg=self._DRAG_ALPHA,
            relief="solid", bd=1,
            height=self._CARD_H)
        self._drag_ghost.place(
            x=2, y=src.winfo_y(),
            width=self._inner.winfo_width() - 4)
        tk.Label(self._drag_ghost,
                 text=f"Moving: #{idx + 1}",
                 bg=self._DRAG_ALPHA,
                 font=("Segoe UI", 9, "italic")).place(
            relx=0.5, rely=0.5, anchor="center")

    def _y_to_index(self, canvas_y: float) -> int:
        step = self._CARD_H + self._PAD
        idx  = int(canvas_y // step)
        return max(0, min(idx, len(self._cards) - 1))

    def _do_reorder(self, src: int, tgt: int) -> None:
        act = self._actions.pop(src)
        self._actions.insert(tgt, act)
        self._selected = {tgt}
        self.set_cards(self._actions)   # full rebuild
        if self.on_reorder:
            self.on_reorder(list(self._actions))

    # ── helpers ───────────────────────────────────────────────────────────────

    def _repack(self) -> None:
        for c in self._cards:
            c.pack(fill="x", padx=4,
                   pady=(self._PAD, 0))

    def _refresh_colors(self) -> None:
        for i, card in enumerate(self._cards):
            if i == self._playing_idx:
                bg = self._PLAY_BG
            elif i in self._selected:
                bg = self._SEL_BG
            elif i == self._drag_target and self._drag_ghost:
                bg = "#e0f0ff"
            else:
                bg = self._CARD_BG
            self._set_bg_recursive(card, bg)

    def _set_bg_recursive(self, widget, bg: str) -> None:
        try:
            widget.config(bg=bg)
        except tk.TclError:
            pass
        for child in widget.winfo_children():
            if isinstance(child, tk.Canvas):
                continue   # don't recolor canvas — it owns its own bg
            self._set_bg_recursive(child, bg)

    def _scroll_to(self, index: int) -> None:
        if not self._cards or index >= len(self._cards):
            return
        inner_h = self._inner.winfo_reqheight()
        if inner_h <= 0:
            return
        step = self._CARD_H + self._PAD
        card_top = index * step
        card_bot = card_top + self._CARD_H
        view_top = self._canvas.yview()[0] * inner_h
        view_bot = view_top + self._canvas.winfo_height()
        if card_top < view_top:
            self._canvas.yview_moveto(card_top / inner_h)
        elif card_bot > view_bot:
            self._canvas.yview_moveto(
                (card_bot - self._canvas.winfo_height()) / inner_h)

    def _on_inner_configure(self, _e=None) -> None:
        self._canvas.configure(
            scrollregion=self._canvas.bbox("all"))

    def _on_canvas_configure(self, e) -> None:
        self._canvas.itemconfig(self._win_id, width=e.width)

    def _on_mousewheel(self, e) -> None:
        self._canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")


# ─────────────────────────────────────────────────────────────────────────────
# Main application
# ─────────────────────────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("BlueStacks Macro Recorder")
        self.geometry("1300x760")
        self.minsize(960, 560)

        self._apply_style()

        self._pm            = ProfileManager(PROFILES_DIR)
        self._profiles: list[dict] = []
        self._cur_profile: dict | None = None

        self._recorder     = Recorder(on_stop=self._recording_stopped)
        self._recording    = False
        self._rec_t0: float = 0.0
        self._timer_cb     = None
        # Continue-recording state
        self._cont_mode:        str  = "append"    # "prepend"|"append"|"after_sel"
        self._cont_insert_idx:  int  = -1          # used in "after_sel" mode

        self._player       = Player(on_action=self._playback_action_cb,
                                    on_done=self._playback_done_cb)
        self._playing          = False
        self._play_loop_var    = tk.BooleanVar(value=False)
        self._play_kl          = None   # F9 stop-listener during playback
        self._play_total       = 0
        self._play_orig_indices: list[int] = []
        self._rec_window_rect: tuple | None = None

        self._adb_connected = False
        self._device_var    = tk.StringVar(value=DEFAULT_DEVICE)
        self._win_title_var = tk.StringVar(value="App Player")
        self._status_var    = tk.StringVar(value="Ready.")

        self._build_ui()
        self._refresh_profiles()
        self.protocol("WM_DELETE_WINDOW", self._quit)

    # ── Styling ───────────────────────────────────────────────────────────────

    def _apply_style(self) -> None:
        s = ttk.Style(self)
        for theme in ("vista", "winnative", "clam", "alt"):
            if theme in s.theme_names():
                s.theme_use(theme)
                break
        s.configure("Rec.TLabel",   foreground="#cc0000", font=("Segoe UI", 10, "bold"))
        s.configure("Green.TLabel", foreground="#009933")
        s.configure("Gray.TLabel",  foreground="#666666")
        s.configure("Play.TLabel",  foreground="#0066cc", font=("Segoe UI", 10, "bold"))

    # ── UI construction ───────────────────────────────────────────────────────

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self._build_top_bar()

        pane = ttk.PanedWindow(self, orient="horizontal")
        pane.grid(row=1, column=0, sticky="nsew", padx=6, pady=4)
        self._build_left(pane)
        self._build_right(pane)

        ttk.Separator(self, orient="horizontal").grid(
            row=2, column=0, sticky="ew")
        self._build_status_bar()

    # ── top bar ───────────────────────────────────────────────────────────────

    def _build_top_bar(self) -> None:
        bar = ttk.Frame(self)
        bar.grid(row=0, column=0, sticky="ew", padx=8, pady=(6, 0))

        ttk.Label(bar, text="Device:").pack(side="left")
        ttk.Entry(bar, textvariable=self._device_var, width=22).pack(
            side="left", padx=(4, 2))

        self._btn_connect = ttk.Button(
            bar, text="Connect ADB", command=self._toggle_adb)
        self._btn_connect.pack(side="left", padx=4)

        self._lbl_adb = ttk.Label(bar, text="● Disconnected", foreground="#cc0000")
        self._lbl_adb.pack(side="left", padx=6)

        ttk.Separator(bar, orient="vertical").pack(
            side="left", fill="y", padx=10, pady=2)

        ttk.Label(bar, text="BS Window Title:").pack(side="left")
        ttk.Entry(bar, textvariable=self._win_title_var, width=16).pack(
            side="left", padx=(4, 8))

        ttk.Button(bar, text="Import Profiles…",
                   command=self._import_profiles).pack(side="left", padx=2)

    # ── left panel ────────────────────────────────────────────────────────────

    def _build_left(self, pane: ttk.PanedWindow) -> None:
        left = ttk.Frame(pane)
        pane.add(left, weight=1)
        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)

        ttk.Label(left, text="Profiles",
                  font=("Segoe UI", 10, "bold")).grid(
            row=0, column=0, sticky="w", padx=6, pady=(6, 2))

        lf = ttk.Frame(left)
        lf.grid(row=1, column=0, sticky="nsew", padx=6)
        lf.columnconfigure(0, weight=1)
        lf.rowconfigure(0, weight=1)

        self._plist = tk.Listbox(
            lf,
            selectmode="extended",
            activestyle="none",
            relief="solid",
            borderwidth=1,
            selectbackground="#0078d4",
            selectforeground="white",
            font=("Segoe UI", 10),
        )
        vsb = ttk.Scrollbar(lf, orient="vertical", command=self._plist.yview)
        self._plist.configure(yscrollcommand=vsb.set)
        self._plist.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        self._plist.bind("<<ListboxSelect>>", self._on_profile_sel)
        self._plist.bind("<Double-Button-1>", self._rename_profile)

        btns = ttk.Frame(left)
        btns.grid(row=2, column=0, sticky="ew", padx=6, pady=4)

        self._btn_del_p = ttk.Button(
            btns, text="Delete", state="disabled", command=self._delete_profiles)
        self._btn_exp_p = ttk.Button(
            btns, text="Export", state="disabled", command=self._export_profiles)
        self._btn_ren_p = ttk.Button(
            btns, text="Rename", state="disabled", command=self._rename_profile)
        for b in (self._btn_del_p, self._btn_exp_p, self._btn_ren_p):
            b.pack(side="left", padx=2)

    # ── right panel ───────────────────────────────────────────────────────────

    def _build_right(self, pane: ttk.PanedWindow) -> None:
        right = ttk.Frame(pane)
        pane.add(right, weight=3)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(2, weight=1)  # actions area expands

        # ─ Recording section ─────────────────────────────────────────────────
        rec = ttk.LabelFrame(right, text="Recording", padding=8)
        rec.grid(row=0, column=0, sticky="ew", padx=4, pady=(0, 2))
        rec.columnconfigure(1, weight=1)

        ttk.Label(rec, text="Profile name:").grid(row=0, column=0, sticky="w")
        self._rec_name = tk.StringVar(value=self._new_name())
        ttk.Entry(rec, textvariable=self._rec_name, width=30).grid(
            row=0, column=1, sticky="ew", padx=6)

        self._lbl_rec_stat = ttk.Label(rec, text="Ready")
        self._lbl_rec_stat.grid(row=0, column=2, padx=8)

        self._lbl_timer = ttk.Label(
            rec, text="00:00.0", font=("Consolas", 13, "bold"))
        self._lbl_timer.grid(row=0, column=3, padx=4)

        self._btn_rec = ttk.Button(
            rec, text="⏺  New Recording", command=self._toggle_recording)
        self._btn_rec.grid(row=0, column=4, padx=4)

        # Continue Recording controls
        self._btn_cont = ttk.Button(
            rec, text="⏺  Continue Recording",
            command=self._toggle_continue_recording,
            state="disabled")
        self._btn_cont.grid(row=0, column=5, padx=4)

        cont_mode_frm = ttk.Frame(rec)
        cont_mode_frm.grid(row=0, column=6, padx=(4, 0))
        ttk.Label(cont_mode_frm, text="at:", foreground="gray").pack(side="left")
        self._cont_mode_var = tk.StringVar(value="append")
        for lbl, val in (("end", "append"), ("start", "prepend"),
                         ("after selected", "after_sel")):
            ttk.Radiobutton(
                cont_mode_frm, text=lbl,
                variable=self._cont_mode_var, value=val,
            ).pack(side="left", padx=2)

        ttk.Label(rec, text="(F8 stop)", foreground="gray").grid(
            row=0, column=7, padx=4)

        # ─ Playback section ───────────────────────────────────────────────────
        play_frm = ttk.LabelFrame(right, text="Playback", padding=8)
        play_frm.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 4))
        play_frm.columnconfigure(3, weight=1)

        self._btn_play = ttk.Button(
            play_frm, text="▶  Play", state="disabled",
            command=self._toggle_playback)
        self._btn_play.grid(row=0, column=0, padx=(0, 8))

        self._chk_loop = ttk.Checkbutton(
            play_frm, text="Loop (infinite)", variable=self._play_loop_var)
        self._chk_loop.grid(row=0, column=1, padx=(0, 12))

        self._lbl_play_stat = ttk.Label(play_frm, text="Idle")
        self._lbl_play_stat.grid(row=0, column=2, padx=(0, 8))

        self._play_progress = ttk.Progressbar(
            play_frm, mode="determinate", length=200)
        self._play_progress.grid(row=0, column=3, sticky="ew", padx=(0, 8))

        self._lbl_play_info = ttk.Label(play_frm, text="", foreground="gray")
        self._lbl_play_info.grid(row=0, column=4, padx=(0, 4))

        ttk.Label(play_frm, text="(F9 stop)", foreground="gray").grid(
            row=0, column=5, padx=4)

        # ─ Actions editor ─────────────────────────────────────────────────────
        self._act_frame = ttk.LabelFrame(
            right, text="Actions  (select a profile)", padding=4)
        self._act_frame.grid(row=2, column=0, sticky="nsew", padx=4)
        self._act_frame.columnconfigure(0, weight=1)
        self._act_frame.rowconfigure(0, weight=1)

        self._card_list = DragCardList(
            self._act_frame,
            on_reorder=self._on_cards_reorder,
            on_edit=self._edit_action_by_index,
            on_select=self._on_card_select,
        )
        self._card_list.grid(row=0, column=0, sticky="nsew")

        # ─ Action toolbar ─────────────────────────────────────────────────────
        atb = ttk.Frame(right)
        atb.grid(row=3, column=0, sticky="ew", padx=4, pady=(2, 4))

        self._abt: dict[str, ttk.Button] = {}
        specs = [
            ("add",       "+ Add Action",  self._add_action),
            ("edit",      "Edit",          self._edit_action),
            ("dup",       "Duplicate",     self._duplicate_action),
            ("del",       "Delete",        self._delete_actions),
            ("up",        "▲ Up",          self._move_up),
            ("down",      "▼ Down",        self._move_down),
            ("delay",     "+ Delay",       self._add_delay),
            ("clear_all", "Clear All",     self._clear_all_actions),
        ]
        for key, label, cmd in specs:
            st = "normal" if key == "add" else "disabled"
            b = ttk.Button(atb, text=label, command=cmd, state=st)
            b.pack(side="left", padx=2)
            self._abt[key] = b

        self._lbl_count = ttk.Label(atb, text="", foreground="gray")
        self._lbl_count.pack(side="right", padx=8)

    def _build_status_bar(self) -> None:
        bar = ttk.Frame(self)
        bar.grid(row=3, column=0, sticky="ew")
        ttk.Label(bar, textvariable=self._status_var,
                  foreground="gray").pack(side="left", padx=8, pady=2)

    # ── Profile management ────────────────────────────────────────────────────

    @staticmethod
    def _new_name() -> str:
        return f"Recording {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    def _refresh_profiles(self) -> None:
        self._profiles = self._pm.load_all()
        self._plist.delete(0, "end")
        for p in self._profiles:
            self._plist.insert("end", p.get("name", "Unnamed"))

    def _on_profile_sel(self, _event=None) -> None:
        sel = self._plist.curselection()
        n   = len(sel)
        self._btn_del_p.config(state="normal" if n else "disabled")
        self._btn_exp_p.config(state="normal" if n else "disabled")
        self._btn_ren_p.config(state="normal" if n == 1 else "disabled")
        if n == 1:
            self._cur_profile = self._profiles[sel[0]]
            self._reload_actions()
            self._set_act_state("normal")
            if not self._playing and not self._recording:
                self._btn_play.config(state="normal")
                self._btn_cont.config(state="normal")
        else:
            self._cur_profile = None
            self._clear_actions_view()
            self._set_act_state("disabled")
            if not self._playing:
                self._btn_play.config(state="disabled")
            self._btn_cont.config(state="disabled")

    def _set_act_state(self, state: str) -> None:
        for key, b in self._abt.items():
            # "add" is always enabled when a profile is loaded
            if key == "add":
                b.config(state="normal" if state == "normal" else "disabled")
            else:
                b.config(state=state)

    def _delete_profiles(self) -> None:
        sel = self._plist.curselection()
        if not sel:
            return
        names = [self._profiles[i].get("name", "Unnamed") for i in sel]
        preview = "\n".join(f"• {n}" for n in names[:10])
        if len(names) > 10:
            preview += f"\n  … and {len(names) - 10} more"
        if not messagebox.askyesno(
                "Confirm Delete",
                f"Permanently delete {len(sel)} profile(s)?\n\n{preview}"):
            return
        for i in sorted(sel, reverse=True):
            self._pm.delete(self._profiles[i])
            del self._profiles[i]
        self._cur_profile = None
        self._clear_actions_view()
        self._plist.delete(0, "end")
        for p in self._profiles:
            self._plist.insert("end", p.get("name", "Unnamed"))
        self._set_status(f"Deleted {len(sel)} profile(s).")

    def _export_profiles(self) -> None:
        sel = self._plist.curselection()
        if not sel:
            return
        chosen = [self._profiles[i] for i in sel]
        dest = filedialog.asksaveasfilename(
            title="Export profiles",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
            initialfile=f"profiles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
        )
        if dest:
            self._pm.export(chosen, dest)
            self._set_status(f"Exported {len(chosen)} profile(s) → {dest}")
            messagebox.showinfo("Export complete",
                f"Exported {len(chosen)} profile(s):\n{dest}")

    def _import_profiles(self) -> None:
        src = filedialog.askopenfilename(
            title="Import profiles",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not src:
            return
        try:
            self._pm.import_file(src)
            self._refresh_profiles()
            self._set_status("Profiles imported successfully.")
        except Exception as exc:
            messagebox.showerror("Import failed", str(exc))

    def _rename_profile(self, _event=None) -> None:
        sel = self._plist.curselection()
        if len(sel) != 1:
            return
        p    = self._profiles[sel[0]]
        name = simpledialog.askstring(
            "Rename Profile", "New profile name:",
            initialvalue=p.get("name", ""), parent=self)
        if name and name.strip():
            p["name"] = name.strip()
            self._pm.save(p)
            self._plist.delete(sel[0])
            self._plist.insert(sel[0], p["name"])
            self._plist.selection_set(sel[0])
            if self._cur_profile is p:
                self._act_frame.config(
                    text=f"Actions  —  {p['name']}")

    # ── Actions view ──────────────────────────────────────────────────────────

    def _reload_actions(self) -> None:
        self._clear_actions_view()
        if not self._cur_profile:
            return
        actions = self._cur_profile.get("actions", [])
        self._card_list.set_cards(actions)
        n = len(actions)
        self._lbl_count.config(text=f"{n} action(s)")
        self._act_frame.config(
            text=(f"Actions  —  {self._cur_profile.get('name', 'Unnamed')} "
                  f"({n} actions)"))

    def _clear_actions_view(self) -> None:
        self._card_list.set_cards([])
        self._lbl_count.config(text="")
        self._act_frame.config(text="Actions  (select a profile)")

    # ── Card callbacks ────────────────────────────────────────────────────────

    def _on_cards_reorder(self, new_actions: list[dict]) -> None:
        if not self._cur_profile:
            return
        self._cur_profile["actions"] = new_actions
        self._pm.save(self._cur_profile)
        n = len(new_actions)
        self._lbl_count.config(text=f"{n} action(s)")

    def _on_card_select(self, indices: list[int]) -> None:
        # Enable/disable toolbar buttons that require a selection
        has_sel = bool(indices)
        single  = len(indices) == 1
        for key in ("edit", "dup", "del", "up", "down", "delay"):
            self._abt[key].config(
                state="normal" if (has_sel and self._cur_profile) else "disabled")

    # ── Action editing ────────────────────────────────────────────────────────

    def _edit_action(self, _event=None) -> None:
        if not self._cur_profile:
            return
        sel = self._card_list.selection()
        if not sel:
            return
        self._edit_action_by_index(sel[0])

    def _edit_action_by_index(self, idx: int) -> None:
        if not self._cur_profile:
            return
        actions = self._cur_profile["actions"]
        if idx >= len(actions):
            return
        dlg = ActionEditDialog(self, actions[idx])
        if dlg.result is not None:
            actions[idx] = dlg.result
            self._pm.save(self._cur_profile)
            self._reload_actions()
            self._card_list.select(idx)

    def _add_action(self) -> None:
        """Insert a brand-new action from scratch."""
        if not self._cur_profile:
            return
        sel = self._card_list.selection()
        acts = self._cur_profile["actions"]
        at = sel[0] + 1 if sel else len(acts)
        prev_t = float(acts[at - 1].get("time", 0.0)) if at > 0 and acts else 0.0
        blank = {"type": "mouse_click", "x": 0, "y": 0,
                 "button": "left", "pressed": True,
                 "time": round(prev_t + 0.1, 4)}
        dlg = ActionEditDialog(self, blank, create_mode=True)
        if dlg.result is not None:
            acts.insert(at, dlg.result)
            self._pm.save(self._cur_profile)
            self._reload_actions()
            self._card_list.select(at)
            self._set_status("Action added.")

    def _delete_actions(self) -> None:
        if not self._cur_profile:
            return
        sel = self._card_list.selection()
        if not sel:
            return
        indices = sorted(sel, reverse=True)
        acts = self._cur_profile["actions"]
        for i in indices:
            if 0 <= i < len(acts):
                del acts[i]
        self._pm.save(self._cur_profile)
        self._reload_actions()
        self._set_status(f"Deleted {len(indices)} action(s).")

    def _move_up(self) -> None:
        self._swap_action(-1)

    def _move_down(self) -> None:
        self._swap_action(+1)

    def _swap_action(self, delta: int) -> None:
        if not self._cur_profile:
            return
        sel = self._card_list.selection()
        if len(sel) != 1:
            return
        idx  = sel[0]
        acts = self._cur_profile["actions"]
        new  = idx + delta
        if not (0 <= new < len(acts)):
            return
        acts[idx], acts[new] = acts[new], acts[idx]
        self._pm.save(self._cur_profile)
        self._reload_actions()
        self._card_list.select(new)

    def _add_delay(self) -> None:
        if not self._cur_profile:
            return
        sel = self._card_list.selection()
        dur = simpledialog.askfloat(
            "Add Delay", "Delay duration (seconds):",
            initialvalue=1.0, minvalue=0.01, maxvalue=300.0, parent=self)
        if dur is None:
            return
        acts = self._cur_profile["actions"]
        at   = sel[0] + 1 if sel else len(acts)
        prev_t = float(acts[at - 1].get("time", 0.0)) if at > 0 and acts else 0.0
        acts.insert(at, {
            "type":     "delay",
            "duration": round(dur, 3),
            "time":     round(prev_t + 0.001, 4),
        })
        self._pm.save(self._cur_profile)
        self._reload_actions()
        self._card_list.select(at)

    def _duplicate_action(self) -> None:
        if not self._cur_profile:
            return
        sel = self._card_list.selection()
        if not sel:
            return
        idx  = sel[0]
        acts = self._cur_profile["actions"]
        dup  = copy.deepcopy(acts[idx])
        acts.insert(idx + 1, dup)
        self._pm.save(self._cur_profile)
        self._reload_actions()
        self._card_list.select(idx + 1)

    def _clear_all_actions(self) -> None:
        if not self._cur_profile:
            return
        if not messagebox.askyesno(
                "Clear All",
                "Remove ALL actions from this profile?\nThis cannot be undone."):
            return
        self._cur_profile["actions"] = []
        self._pm.save(self._cur_profile)
        self._reload_actions()
        m.tk_popup(event.x_root, event.y_root)

    # ── Recording ─────────────────────────────────────────────────────────────

    def _toggle_recording(self) -> None:
        if not self._recording:
            self._start_recording(continue_mode=False)
        else:
            self._recorder.stop()

    def _toggle_continue_recording(self) -> None:
        """Append/prepend/insert new recording into the current profile."""
        if not self._cur_profile:
            return
        if not self._recording:
            self._cont_mode = self._cont_mode_var.get()
            sel = self._card_list.selection()
            self._cont_insert_idx = sel[0] if sel else -1
            self._start_recording(continue_mode=True)
        else:
            self._recorder.stop()

    def _start_recording(self, continue_mode: bool = False) -> None:
        if not PYNPUT_OK:
            messagebox.showerror(
                "Missing dependency",
                "pynput is not installed.\n\nFix it by running:\n\n"
                "    pip install pynput\n\nthen restart the application.")
            return
        self._continue_mode = continue_mode
        title = self._win_title_var.get().strip()
        self._rec_window_rect = _find_window_rect(title) if title else None
        if self._rec_window_rect is None:
            self._set_status(
                "Warning: BlueStacks window not found — "
                "check the 'BS Window Title' field. Coordinates may be wrong at playback.")
        self._recording = True
        self._rec_t0    = time.perf_counter()
        if not continue_mode:
            self._rec_name.set(self._new_name())
        label = "⏹  Stop" if continue_mode else "⏹  Stop Recording"
        (self._btn_cont if continue_mode else self._btn_rec).config(text=label)
        self._btn_play.config(state="disabled")
        self._btn_rec.config(state="disabled" if continue_mode else "normal")
        self._btn_cont.config(state="disabled" if not continue_mode else "normal")
        lbl = "● CONT" if continue_mode else "● REC"
        self._lbl_rec_stat.config(text=lbl, foreground="#cc0000")
        self._tick()
        self._recorder.start()
        self.iconify()

    def _recording_stopped(self, actions: list[dict]) -> None:
        """Called from a helper thread — schedules back to main thread."""
        self.after(0, self._finalize, actions)

    def _finalize(self, actions: list[dict]) -> None:
        """Runs on main thread after recording ends."""
        self._recording = False
        if self._timer_cb:
            self.after_cancel(self._timer_cb)
            self._timer_cb = None
        self._lbl_rec_stat.config(text="Ready", foreground="")
        self._btn_rec.config(text="⏺  New Recording", state="normal")
        if self._cur_profile:
            self._btn_cont.config(state="normal")
            self._btn_play.config(state="normal")
        else:
            self._btn_cont.config(state="disabled")
        self._lbl_timer.config(text="00:00.0")
        self.deiconify()
        self.lift()
        self.focus_force()

        if not actions:
            if getattr(self, "_continue_mode", False):
                return   # nothing added — silently ignore
            messagebox.showinfo("Recording", "No actions were recorded.")
            return

        # Collapse consecutive mouse_move events into mouse_path groups
        collapsed = collapse_mouse_moves(actions)

        if getattr(self, "_continue_mode", False) and self._cur_profile:
            # ── Continue mode: merge into existing profile ──────────────────
            existing = self._cur_profile.get("actions", [])
            mode     = getattr(self, "_cont_mode", "append")
            if mode == "prepend":
                # Re-timestamp new actions so they start before existing ones.
                # Shift existing actions forward by the duration of new recording.
                new_dur = float(collapsed[-1].get("time", 0.0)) if collapsed else 0.0
                for ea in existing:
                    ea["time"] = round(float(ea.get("time", 0.0)) + new_dur + 0.5, 4)
                merged = collapsed + existing
            elif mode == "after_sel":
                insert_at = getattr(self, "_cont_insert_idx", -1)
                if insert_at < 0 or insert_at >= len(existing):
                    insert_at = len(existing)
                # Re-timestamp: offset new actions to follow the action at insert_at
                base_t = float(existing[insert_at].get("time", 0.0)) if existing else 0.0
                shift  = base_t + 0.1
                for na in collapsed:
                    na["time"] = round(float(na.get("time", 0.0)) + shift, 4)
                # Shift remaining existing actions forward too
                new_dur = float(collapsed[-1].get("time", 0.0)) if collapsed else 0.0
                tail_shift = new_dur + 0.1
                for ea in existing[insert_at + 1:]:
                    ea["time"] = round(float(ea.get("time", 0.0)) + tail_shift, 4)
                merged = existing[:insert_at + 1] + collapsed + existing[insert_at + 1:]
            else:  # append
                # Offset new actions so they follow the last existing action
                base_t = float(existing[-1].get("time", 0.0)) if existing else 0.0
                shift  = base_t + 0.5
                for na in collapsed:
                    na["time"] = round(float(na.get("time", 0.0)) + shift, 4)
                merged = existing + collapsed
            self._cur_profile["actions"] = merged
            self._pm.save(self._cur_profile)
            self._reload_actions()
            n = len(collapsed)
            self._set_status(
                f"Added {n} action(s) to '{self._cur_profile.get('name', '')}'.")
        else:
            # ── New profile ──────────────────────────────────────────────────
            name = self._rec_name.get().strip() or self._new_name()
            profile = {
                "id":          str(uuid.uuid4()),
                "name":        name,
                "created":     datetime.now().isoformat(),
                "device":      self._device_var.get(),
                "window_rect": list(self._rec_window_rect) if self._rec_window_rect else None,
                "actions":     collapsed,
            }
            self._pm.save(profile)
            self._profiles.append(profile)
            self._plist.insert("end", name)
            new_idx = len(self._profiles) - 1
            self._plist.selection_clear(0, "end")
            self._plist.selection_set(new_idx)
            self._plist.see(new_idx)
            self._on_profile_sel()
            n = len(collapsed)
            self._set_status(f"Profile '{name}' saved — {n} actions.")
            messagebox.showinfo(
                "Recording saved",
                f"Profile \"{name}\" saved.\n{n} actions recorded.")

    def _tick(self) -> None:
        if not self._recording:
            return
        elapsed = time.perf_counter() - self._rec_t0
        m, s = divmod(elapsed, 60)
        self._lbl_timer.config(text=f"{int(m):02d}:{s:04.1f}")
        self._timer_cb = self.after(100, self._tick)

    # ── Playback ──────────────────────────────────────────────────────────────

    def _toggle_playback(self) -> None:
        if self._playing:
            self._stop_playback()
        else:
            self._start_playback()

    def _start_playback(self) -> None:
        if not self._cur_profile:
            return
        actions = self._cur_profile.get("actions", [])
        if not actions:
            messagebox.showinfo("Playback", "This profile has no actions to play.")
            return

        # ─ Resolve window rect (prefer stored profile value) ──────────────────
        window_rect = self._cur_profile.get("window_rect")
        if not window_rect:
            title = self._win_title_var.get().strip()
            if title:
                window_rect = _find_window_rect(title)
        if not window_rect:
            messagebox.showerror(
                "Playback – window not found",
                "No window position is stored in this profile and the\n"
                "BlueStacks window could not be located automatically.\n\n"
                "Re-record while BlueStacks is open, or check the\n"
                "'BS Window Title' field in the top bar.")
            return

        # ─ Resolve Android device resolution via ADB ─────────────────────
        device      = self._device_var.get().strip()
        device_size = _get_device_size(device)
        if not device_size:
            if not messagebox.askyesno(
                    "Device size unknown",
                    "Could not query the Android resolution via ADB.\n"
                    "Use fallback 1280×720?\n\n"
                    "(Connect ADB first for accurate coordinate mapping.)"):
                return
            device_size = (1280, 720)

        # ─ Detect touch input device + event struct size ───────────────────
        ev_path, ev_xmax, ev_ymax = _find_touch_device(device, device_size)
        touch_info  = (ev_path, ev_xmax, ev_ymax) if ev_path else None
        event_size  = _get_event_size(device)
        if touch_info:
            self._set_status(
                f"Binary injection: {ev_path}  ({ev_xmax}×{ev_ymax})  "
                f"{event_size}B/event")
        else:
            self._set_status(
                "Touch device not found — falling back to input tap/swipe")

        # ─ Build frames ────────────────────────────────────────────────
        frames = Player.preprocess(
            actions, tuple(window_rect), device_size, touch_info, event_size)
        if not frames:
            messagebox.showinfo(
                "Playback",
                "No playable actions found in this profile.\n"
                "(Only clicks, scrolls, key presses and delays are sent.)")
            return

        self._play_total        = len(frames)
        self._play_orig_indices = [f.get("orig", 0) for f in frames]
        loop                    = self._play_loop_var.get()

        # ─ Update UI ─────────────────────────────────────────────────
        self._playing = True
        self._btn_play.config(text="⏹  Stop")
        self._btn_rec.config(state="disabled")
        self._btn_cont.config(state="disabled")
        self._chk_loop.config(state="disabled")
        self._set_act_state("disabled")
        self._lbl_play_stat.config(text="▶ Playing", foreground="#0066cc")
        self._play_progress.config(maximum=max(self._play_total - 1, 1), value=0)
        self._lbl_play_info.config(text=f"0 / {self._play_total}")

        self._start_f9_listener()
        self.iconify()
        self._player.start(frames, loop, device, ev_path, event_size)

    def _stop_playback(self) -> None:
        self._player.stop()
        self._stop_f9_listener()

    def _playback_action_cb(self, cmd_idx: int, loop_num: int, total: int) -> None:
        """Called from player thread — schedule on main thread."""
        self.after(0, self._update_play_ui, cmd_idx, loop_num, total)

    def _update_play_ui(self, cmd_idx: int, loop_num: int, total: int) -> None:
        self._play_progress.config(value=cmd_idx)
        loop_txt = f"  Loop #{loop_num}" if self._play_loop_var.get() else ""
        self._lbl_play_info.config(text=f"{cmd_idx + 1} / {total}{loop_txt}")
        # highlight matching original action card
        if cmd_idx < len(self._play_orig_indices):
            orig = self._play_orig_indices[cmd_idx]
            self._card_list.set_playing(orig)

    def _playback_done_cb(self) -> None:
        """Called from player thread when playback finishes."""
        self.after(0, self._playback_finished)

    def _playback_finished(self) -> None:
        self._playing = False
        self._stop_f9_listener()
        self._btn_play.config(text="▶  Play", state="normal")
        self._btn_rec.config(state="normal")
        if self._cur_profile:
            self._btn_cont.config(state="normal")
        self._chk_loop.config(state="normal")
        self._set_act_state("normal")
        self._lbl_play_stat.config(text="Idle", foreground="")
        self._play_progress.config(value=0)
        self._lbl_play_info.config(text="")
        self._card_list.clear_playing()
        self.deiconify()
        self.lift()
        self._set_status("Playback finished.")

    # ── F9 listener ───────────────────────────────────────────────────────────

    def _start_f9_listener(self) -> None:
        if not PYNPUT_OK:
            return

        def _on_press(key):
            if key == _pkbd.Key.f9:
                self._stop_playback()
                return False  # stop listener

        self._play_kl = _pkbd.Listener(on_press=_on_press)
        self._play_kl.daemon = True
        self._play_kl.start()

    def _stop_f9_listener(self) -> None:
        if self._play_kl is not None:
            try:
                self._play_kl.stop()
            except Exception:
                pass
            self._play_kl = None

    # ── ADB connection ────────────────────────────────────────────────────────

    def _toggle_adb(self) -> None:
        if self._adb_connected:
            self._disconnect_adb()
        else:
            self._connect_adb()

    def _connect_adb(self) -> None:
        dev = self._device_var.get().strip()
        if not dev:
            messagebox.showerror("ADB", "Enter a device address first.")
            return
        self._btn_connect.config(state="disabled", text="Connecting…")
        self.update_idletasks()

        def _run() -> None:
            try:
                r   = subprocess.run(
                    ["adb", "connect", dev],
                    capture_output=True, text=True, timeout=10)
                out = (r.stdout + r.stderr).strip()
                ok  = "connected" in out.lower()
                self.after(0, self._adb_done, ok, dev, out)
            except FileNotFoundError:
                self.after(0, self._adb_done, False, dev,
                    "adb not found.\n\n"
                    "Install Android Platform Tools and add adb to your PATH.\n"
                    "Download: https://developer.android.com/tools/releases/platform-tools")
            except subprocess.TimeoutExpired:
                self.after(0, self._adb_done, False, dev, "Connection timed out.")
            except Exception as exc:
                self.after(0, self._adb_done, False, dev, str(exc))

        threading.Thread(target=_run, daemon=True).start()

    def _adb_done(self, success: bool, dev: str, msg: str) -> None:
        self._btn_connect.config(state="normal")
        if success:
            self._adb_connected = True
            self._btn_connect.config(text="Disconnect ADB")
            self._lbl_adb.config(text=f"● {dev}", foreground="#009933")
            self._set_status(f"ADB connected: {dev}")
        else:
            self._adb_connected = False
            self._btn_connect.config(text="Connect ADB")
            self._lbl_adb.config(text="● Disconnected", foreground="#cc0000")
            messagebox.showerror("ADB connection failed", msg)

    def _disconnect_adb(self) -> None:
        dev = self._device_var.get().strip()
        try:
            subprocess.run(["adb", "disconnect", dev],
                           capture_output=True, timeout=5)
        except Exception:
            pass
        self._adb_connected = False
        self._btn_connect.config(text="Connect ADB")
        self._lbl_adb.config(text="● Disconnected", foreground="#cc0000")
        self._set_status("ADB disconnected.")

    # ── Misc ──────────────────────────────────────────────────────────────────

    def _set_status(self, msg: str) -> None:
        self._status_var.set(msg)

    def _quit(self) -> None:
        if self._playing:
            self._stop_playback()
        if self._recording:
            if messagebox.askyesno(
                    "Quit", "A recording is in progress. Stop and quit?"):
                self._recorder.stop()
            else:
                return
        self.destroy()


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.mainloop()
