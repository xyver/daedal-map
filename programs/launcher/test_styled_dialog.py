from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
from styled_dialog import StyledDialog
from theme import THEME


def test_styled_confirmation_is_modal_and_uses_launcher_theme() -> None:
    import tkinter as tk

    try:
        root = tk.Tk()
    except tk.TclError as exc:
        pytest.skip(f"Tk runtime unavailable: {exc}")
    root.withdraw()
    try:
        dialog = StyledDialog(root, "Geometry download details", "Lite release 1.3.0\nSHA-256: " + "a" * 64,
                              kind="confirm", confirm_label="Start download")
        assert dialog.window.cget("bg") == THEME["colors"]["app_bg"]
        assert dialog.window.grab_current() is dialog.window
        root.after(0, dialog.accept)
        assert dialog.show() is True
        cancelled = StyledDialog(root, "Geometry download details", "Cancel this download?", kind="confirm")
        root.after(0, cancelled.close)
        assert cancelled.show() is False
    finally:
        root.destroy()
