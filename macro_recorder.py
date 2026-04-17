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
            elif t == "mouse_move":
                if use_bin:
                    if finger_down:
                        tx, ty = to_t(act["x"], act["y"])
                        last_tx, last_ty = tx, ty
                        if pending_press is not None:
                            # First move: commit deferred press as fdown.
                            pp            = pending_press
                            pending_press = None
                            drag_active   = True
                            frames.append({"time": pp["ts"], "orig": pp["orig"],
                                           "touch": fdown(pp["tx"], pp["ty"]),
                                           "is_fdown": True})
                        frames.append({"time": ts, "orig": i,
                                       "touch": fmove(tx, ty)})
                else:
                    if drag_start is not None:
                        ax, ay = to_d(act["x"], act["y"])
                        if (not drag_wpts
                                or (drag_wpts[-1][0], drag_wpts[-1][1]) != (ax, ay)):
                            drag_wpts.append((ax, ay, ts))

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

    def __init__(self, parent: tk.Misc, action: dict) -> None:
        super().__init__(parent)
        self.title("Edit Action")
        self.resizable(False, False)
        self.grab_set()
        self.transient(parent)
        self.result: dict | None = None
        self._orig   = action
        self._fields: dict[str, tk.Variable] = {}
        self._build()
        self.wait_window()

    def _build(self) -> None:
        pad = {"padx": 8, "pady": 4}
        frm = ttk.Frame(self, padding=14)
        frm.grid(sticky="nsew")
        self.columnconfigure(0, weight=1)

        ttk.Label(frm, text=f"Type: {self._orig['type']}",
                  font=("Segoe UI", 10, "bold")).grid(
            row=0, columnspan=2, sticky="w", pady=(0, 10))

        r = 1
        for key, val in self._orig.items():
            if key == "type":
                continue
            ttk.Label(frm, text=key + ":").grid(row=r, column=0, sticky="w", **pad)
            if isinstance(val, bool):
                var: tk.Variable = tk.BooleanVar(value=val)
                widget = ttk.Checkbutton(frm, variable=var)
            else:
                var = tk.StringVar(value=str(val))
                widget = ttk.Entry(frm, textvariable=var, width=28)
            widget.grid(row=r, column=1, sticky="ew", **pad)
            self._fields[key] = var
            r += 1

        ttk.Separator(frm).grid(row=r, columnspan=2, sticky="ew", pady=8)
        r += 1

        btn_frm = ttk.Frame(frm)
        btn_frm.grid(row=r, columnspan=2, sticky="e")
        ttk.Button(btn_frm, text="Save",   command=self._save).pack(side="right", padx=4)
        ttk.Button(btn_frm, text="Cancel", command=self.destroy).pack(side="right")

    def _save(self) -> None:
        result = {"type": self._orig["type"]}
        for key, var in self._fields.items():
            raw  = var.get()
            orig = self._orig[key]
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
# Main application
# ─────────────────────────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("BlueStacks Macro Recorder")
        self.geometry("1200x720")
        self.minsize(900, 540)

        self._apply_style()

        self._pm            = ProfileManager(PROFILES_DIR)
        self._profiles: list[dict] = []
        self._cur_profile: dict | None = None

        self._recorder     = Recorder(on_stop=self._recording_stopped)
        self._recording    = False
        self._rec_t0: float = 0.0
        self._timer_cb     = None

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
        ttk.Entry(rec, textvariable=self._rec_name, width=36).grid(
            row=0, column=1, sticky="ew", padx=6)

        self._lbl_rec_stat = ttk.Label(rec, text="Ready")
        self._lbl_rec_stat.grid(row=0, column=2, padx=10)

        self._lbl_timer = ttk.Label(
            rec, text="00:00.0", font=("Consolas", 13, "bold"))
        self._lbl_timer.grid(row=0, column=3, padx=6)

        self._btn_rec = ttk.Button(
            rec, text="⏺  Start Recording", command=self._toggle_recording)
        self._btn_rec.grid(row=0, column=4, padx=6)

        ttk.Label(rec, text="(F8 to stop)", foreground="gray").grid(
            row=0, column=5, padx=4)

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

        ttk.Label(play_frm, text="(F9 to stop)", foreground="gray").grid(
            row=0, column=5, padx=4)

        # ─ Actions editor ─────────────────────────────────────────────────────
        self._act_frame = ttk.LabelFrame(
            right, text="Actions  (select a profile)", padding=4)
        self._act_frame.grid(row=2, column=0, sticky="nsew", padx=4)
        self._act_frame.columnconfigure(0, weight=1)
        self._act_frame.rowconfigure(0, weight=1)

        cols = ("No.", "Type", "Details", "Time (s)")
        self._tree = ttk.Treeview(
            self._act_frame, columns=cols, show="headings", selectmode="extended")
        for c in cols:
            self._tree.heading(c, text=c)
        self._tree.column("No.",      width=54,  stretch=False, anchor="center")
        self._tree.column("Type",     width=125, stretch=False)
        self._tree.column("Details",  width=420)
        self._tree.column("Time (s)", width=88,  stretch=False, anchor="e")

        # tag for highlighting current playback action
        self._tree.tag_configure("playing", background="#cce5ff")

        vsb2 = ttk.Scrollbar(
            self._act_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb2.set)
        self._tree.grid(row=0, column=0, sticky="nsew")
        vsb2.grid(row=0, column=1, sticky="ns")

        self._tree.bind("<Double-Button-1>", self._edit_action)
        self._tree.bind("<Button-3>",        self._ctx_menu)

        # ─ Action toolbar ─────────────────────────────────────────────────────
        atb = ttk.Frame(right)
        atb.grid(row=3, column=0, sticky="ew", padx=4, pady=(2, 4))

        self._abt: dict[str, ttk.Button] = {}
        specs = [
            ("edit",      "Edit",       self._edit_action),
            ("dup",       "Duplicate",  self._duplicate_action),
            ("del",       "Delete",     self._delete_actions),
            ("up",        "▲ Up",       self._move_up),
            ("down",      "▼ Down",     self._move_down),
            ("delay",     "+ Delay",    self._add_delay),
            ("clear_all", "Clear All",  self._clear_all_actions),
        ]
        for key, label, cmd in specs:
            b = ttk.Button(atb, text=label, command=cmd, state="disabled")
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
        else:
            self._cur_profile = None
            self._clear_actions_view()
            self._set_act_state("disabled")
            if not self._playing:
                self._btn_play.config(state="disabled")

    def _set_act_state(self, state: str) -> None:
        for b in self._abt.values():
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
        for i, act in enumerate(actions):
            self._insert_row(i, act)
        n = len(actions)
        self._lbl_count.config(text=f"{n} action(s)")
        self._act_frame.config(
            text=(f"Actions  —  {self._cur_profile.get('name', 'Unnamed')} "
                  f" ({n} actions)"))

    def _insert_row(self, i: int, act: dict) -> None:
        self._tree.insert(
            "", "end", iid=str(i),
            values=(i + 1, act.get("type", ""), self._fmt(act), act.get("time", "")))

    @staticmethod
    def _fmt(act: dict) -> str:
        t = act.get("type", "")
        if t == "mouse_move":
            return f"x={act['x']}, y={act['y']}"
        if t == "mouse_click":
            arrow = "▼" if act.get("pressed") else "▲"
            return f"x={act['x']}, y={act['y']},  {act.get('button')} {arrow}"
        if t == "mouse_scroll":
            return (f"x={act['x']}, y={act['y']},  "
                    f"dx={act.get('dx')}, dy={act.get('dy')}")
        if t in ("key_press", "key_release"):
            arrow = "▼" if t == "key_press" else "▲"
            return f"key={act.get('key')}  {arrow}"
        if t == "delay":
            return f"wait  {act.get('duration', 0)} s"
        return str({k: v for k, v in act.items() if k not in ("type", "time")})

    def _clear_actions_view(self) -> None:
        for it in self._tree.get_children():
            self._tree.delete(it)
        self._lbl_count.config(text="")
        self._act_frame.config(text="Actions  (select a profile)")

    # ── Action editing ────────────────────────────────────────────────────────

    def _edit_action(self, _event=None) -> None:
        if not self._cur_profile:
            return
        sel = self._tree.selection()
        if not sel:
            return
        idx     = int(sel[0])
        actions = self._cur_profile["actions"]
        if idx >= len(actions):
            return
        dlg = ActionEditDialog(self, actions[idx])
        if dlg.result is not None:
            actions[idx] = dlg.result
            self._pm.save(self._cur_profile)
            self._reload_actions()
            if str(idx) in self._tree.get_children():
                self._tree.selection_set(str(idx))
                self._tree.see(str(idx))

    def _delete_actions(self) -> None:
        if not self._cur_profile:
            return
        sel = self._tree.selection()
        if not sel:
            return
        indices = sorted((int(s) for s in sel), reverse=True)
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
        sel = self._tree.selection()
        if len(sel) != 1:
            return
        idx  = int(sel[0])
        acts = self._cur_profile["actions"]
        new  = idx + delta
        if not (0 <= new < len(acts)):
            return
        acts[idx], acts[new] = acts[new], acts[idx]
        self._pm.save(self._cur_profile)
        self._reload_actions()
        if str(new) in self._tree.get_children():
            self._tree.selection_set(str(new))
            self._tree.see(str(new))

    def _add_delay(self) -> None:
        if not self._cur_profile:
            return
        sel = self._tree.selection()
        dur = simpledialog.askfloat(
            "Add Delay", "Delay duration (seconds):",
            initialvalue=1.0, minvalue=0.01, maxvalue=300.0, parent=self)
        if dur is None:
            return
        acts = self._cur_profile["actions"]
        at   = int(sel[0]) + 1 if sel else len(acts)
        prev_t = acts[at - 1]["time"] if at > 0 and acts else 0.0
        acts.insert(at, {
            "type":     "delay",
            "duration": round(dur, 3),
            "time":     round(prev_t + 0.001, 4),
        })
        self._pm.save(self._cur_profile)
        self._reload_actions()

    def _duplicate_action(self) -> None:
        if not self._cur_profile:
            return
        sel = self._tree.selection()
        if not sel:
            return
        idx  = int(sel[0])
        acts = self._cur_profile["actions"]
        dup  = copy.deepcopy(acts[idx])
        acts.insert(idx + 1, dup)
        self._pm.save(self._cur_profile)
        self._reload_actions()

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

    def _ctx_menu(self, event) -> None:
        row = self._tree.identify_row(event.y)
        if not row:
            return
        self._tree.selection_set(row)
        m = tk.Menu(self, tearoff=0)
        m.add_command(label="Edit",            command=self._edit_action)
        m.add_command(label="Duplicate",       command=self._duplicate_action)
        m.add_command(label="Delete",          command=self._delete_actions)
        m.add_separator()
        m.add_command(label="Move Up",         command=self._move_up)
        m.add_command(label="Move Down",       command=self._move_down)
        m.add_separator()
        m.add_command(label="Add Delay After", command=self._add_delay)
        m.tk_popup(event.x_root, event.y_root)

    # ── Recording ─────────────────────────────────────────────────────────────

    def _toggle_recording(self) -> None:
        if not self._recording:
            self._start_recording()
        else:
            self._recorder.stop()

    def _start_recording(self) -> None:
        if not PYNPUT_OK:
            messagebox.showerror(
                "Missing dependency",
                "pynput is not installed.\n\nFix it by running:\n\n"
                "    pip install pynput\n\nthen restart the application.")
            return
        # Snapshot BlueStacks window position for ADB coordinate mapping
        title = self._win_title_var.get().strip()
        self._rec_window_rect = _find_window_rect(title) if title else None
        if self._rec_window_rect is None:
            self._set_status(
                "Warning: BlueStacks window not found — "
                "check the 'BS Window Title' field. Coordinates may be wrong at playback.")
        self._recording = True
        self._rec_t0    = time.perf_counter()
        self._rec_name.set(self._new_name())
        self._lbl_rec_stat.config(text="● REC", foreground="#cc0000")
        self._btn_rec.config(text="⏹  Stop Recording")
        self._btn_play.config(state="disabled")   # can't play while recording
        self._tick()
        self._recorder.start()
        self.iconify()   # minimise so user can interact with BlueStacks freely

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
        self._btn_rec.config(text="⏺  Start Recording")
        self._lbl_timer.config(text="00:00.0")
        # re-enable play if a profile is already selected
        if self._cur_profile:
            self._btn_play.config(state="normal")
        self.deiconify()
        self.lift()
        self.focus_force()

        if not actions:
            messagebox.showinfo("Recording", "No actions were recorded.")
            return

        name = self._rec_name.get().strip() or self._new_name()
        profile = {
            "id":          str(uuid.uuid4()),
            "name":        name,
            "created":     datetime.now().isoformat(),
            "device":      self._device_var.get(),
            "window_rect": list(self._rec_window_rect) if self._rec_window_rect else None,
            "actions":     actions,
        }
        self._pm.save(profile)
        self._profiles.append(profile)
        self._plist.insert("end", name)
        new_idx = len(self._profiles) - 1
        self._plist.selection_clear(0, "end")
        self._plist.selection_set(new_idx)
        self._plist.see(new_idx)
        self._on_profile_sel()
        self._set_status(f"Profile '{name}' saved — {len(actions)} actions.")
        messagebox.showinfo(
            "Recording saved",
            f"Profile \"{name}\" saved.\n{len(actions)} actions recorded.")

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
        # highlight matching original action row in treeview
        for iid in self._tree.get_children():
            tags = list(self._tree.item(iid, "tags"))
            if "playing" in tags:
                tags.remove("playing")
                self._tree.item(iid, tags=tags)
        if cmd_idx < len(self._play_orig_indices):
            orig = self._play_orig_indices[cmd_idx]
            iid  = str(orig)
            if iid in self._tree.get_children():
                self._tree.item(iid, tags=["playing"])
                self._tree.see(iid)

    def _playback_done_cb(self) -> None:
        """Called from player thread when playback finishes."""
        self.after(0, self._playback_finished)

    def _playback_finished(self) -> None:
        self._playing = False
        self._stop_f9_listener()
        self._btn_play.config(text="▶  Play", state="normal")
        self._btn_rec.config(state="normal")
        self._chk_loop.config(state="normal")
        self._set_act_state("normal")
        self._lbl_play_stat.config(text="Idle", foreground="")
        self._play_progress.config(value=0)
        self._lbl_play_info.config(text="")
        # clear highlight
        for iid in self._tree.get_children():
            tags = list(self._tree.item(iid, "tags"))
            if "playing" in tags:
                tags.remove("playing")
                self._tree.item(iid, tags=tags)
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
