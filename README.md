# ADB Macro Recorder

A Python/tkinter GUI tool that records mouse and keyboard input on your PC and replays it inside any Android emulator accessible via ADB with sub-millisecond timing accuracy.

---

## Features

- **Record** mouse clicks, drags, and keyboard presses via `pynput`
- **Playback** uses raw ADB input injection over the BlueStacks Virtual Touch device (Protocol A multitouch events) — no root required beyond the emulator's own ADB interface
- **Precision timing** — every frame is anchored to a single `t_start` epoch; drift cannot accumulate regardless of macro length
- **Profile manager** — save, rename, delete, import, and export macros as JSON
- **Loop control** — play a macro N times (or indefinitely)
- **Live progress** bar and frame counter in the GUI

---

## Requirements

| Dependency | Purpose |
|---|---|
| Python 3.10+ | Runtime |
| `pynput` | Mouse/keyboard capture during recording |
| ADB (in `PATH` or beside the script) | Injecting touch events into BlueStacks |
| BlueStacks 5 | Target emulator (ADB port 5555 by default) |

```
pip install pynput
```

ADB must be able to connect to `127.0.0.1:5555` before playing back.

---

## Quick start

```bash
python macro_recorder.py
```

1. Click **Record** and perform actions in your emulator while the window is focused.
2. Press **Stop** (or the hotkey shown in the UI) to finish recording.
3. Give the profile a name and save it.
4. Select the profile and click **Play**.

---

## How playback works

1. An ADB shell is opened to `/dev/input/event4` (BlueStacks Virtual Touch).
2. Each recorded frame is converted to Protocol A multitouch binary events:
   - `ABS_MT_POSITION_X` / `ABS_MT_POSITION_Y` (type 3, codes 53/54)
   - `SYN_MT_REPORT` (type 0, code 2)
   - `SYN_REPORT` (type 0, code 0)
3. Events are written via `dd` directly to the input device with real `tv_sec`/`tv_usec` timestamps patched in at write time.
4. The playback thread sleeps until each frame's deadline using `Event.wait()` + a 2 ms precision spin, so overshoot on one frame shortens the next — no long-term drift.

---

## File layout

```
cooking fever/
├── macro_recorder.py   # Main application
├── profiles/           # Saved macros (JSON, gitignored)
├── .gitignore
└── README.md
```

---

## Known limitations

- Only tested with BlueStacks 5 on Windows; the ADB device address is hard-coded to `127.0.0.1:5555`.
- Only single-touch (one finger at a time) is recorded and replayed.
- Keyboard events are captured but not yet translated to Android keycodes for injection.
