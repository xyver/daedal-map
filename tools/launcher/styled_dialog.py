"""Shared, theme-aware modal dialogs for the launcher."""

from __future__ import annotations

import tkinter as tk
from tkinter import BOTH, LEFT, RIGHT, WORD, Y, Button, Frame, Label, Scrollbar, Text, Toplevel

from theme import THEME, app_root_kwargs, button_kwargs, panel_kwargs, scrollbar_kwargs


class StyledDialog:
    """A small modal with selectable details and consistent launcher controls."""

    def __init__(self, parent: tk.Misc, title: str, message: str, *, kind: str = "info",
                 confirm_label: str | None = None, cancel_label: str = "Cancel") -> None:
        self.parent = parent
        self.result = False
        self.window = Toplevel(parent)
        self.window.withdraw()
        self.window.title(title)
        self.window.configure(**app_root_kwargs())
        self.window.transient(parent)
        self.window.resizable(False, False)
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.window.bind("<Escape>", lambda _event: self.close())
        self.window.bind("<Return>", lambda _event: self.accept())
        panel = Frame(self.window, **panel_kwargs())
        panel.pack(fill=BOTH, expand=True, padx=14, pady=14)
        accent = THEME["colors"]["danger_active"] if kind == "error" else THEME["colors"]["accent"]
        Label(panel, text="DAEDALMAP  /  " + ("ERROR" if kind == "error" else "REVIEW" if kind == "confirm" else "NOTICE"),
              bg=THEME["colors"]["panel_bg"], fg=accent, font=THEME["fonts"]["mono"]).pack(
                  anchor="w", padx=20, pady=(18, 5))
        Label(panel, text=title, bg=THEME["colors"]["panel_bg"],
              fg=THEME["colors"]["text"], font=THEME["fonts"]["card"],
              wraplength=520, justify=LEFT).pack(anchor="w", padx=20)
        lines = str(message).splitlines() or [""]
        visual_lines = sum(max(1, (len(line) + 62) // 63) for line in lines)
        detail_row = Frame(panel, bg=THEME["colors"]["panel_bg"])
        detail_row.pack(fill="x", padx=20, pady=(13, 15))
        details = Text(detail_row, width=64, height=min(16, max(4, visual_lines + 1)), wrap=WORD,
                       bg=THEME["colors"]["panel_bg"], fg=THEME["colors"]["text"],
                       selectbackground=THEME["colors"]["selection"],
                       selectforeground=THEME["colors"]["text"],
                       font=THEME["fonts"]["body"], relief="flat", bd=0,
                       highlightthickness=0, padx=0, pady=0)
        details.insert("1.0", message)
        details.configure(state="disabled")
        details.pack(side=LEFT, fill="x", expand=True)
        if visual_lines > 16:
            scroll = Scrollbar(detail_row, command=details.yview, **scrollbar_kwargs())
            scroll.pack(side=RIGHT, fill=Y)
            details.configure(yscrollcommand=scroll.set)
        actions = Frame(panel, bg=THEME["colors"]["panel_bg"])
        actions.pack(fill="x", padx=20, pady=(0, 18))
        label = confirm_label or ("Continue" if kind == "confirm" else "Close")
        primary = Button(actions, text=label, command=self.accept, **button_kwargs("accent"))
        primary.pack(side=RIGHT)
        if kind == "confirm":
            Button(actions, text=cancel_label, command=self.close,
                   **button_kwargs("primary")).pack(side=RIGHT, padx=(0, 8))
        self.window.update_idletasks()
        x = parent.winfo_rootx() + max(0, (parent.winfo_width() - self.window.winfo_width()) // 2)
        y = parent.winfo_rooty() + max(0, (parent.winfo_height() - self.window.winfo_height()) // 2)
        self.window.geometry(f"+{x}+{y}")
        self.window.deiconify()
        self.window.grab_set()
        primary.focus_set()

    def accept(self) -> None:
        self.result = True
        self.window.destroy()

    def close(self) -> None:
        self.window.destroy()

    def show(self) -> bool:
        self.parent.wait_window(self.window)
        return self.result


class LauncherMessageBox:
    """Compatible showinfo/showerror/askyesno API for launcher call sites."""

    @staticmethod
    def _parent(parent: tk.Misc | None = None) -> tk.Misc:
        selected = parent or tk._default_root
        if selected is None:
            raise RuntimeError("A launcher window is required before showing a dialog")
        return selected

    def askyesno(self, title: str, message: str, *, parent: tk.Misc | None = None,
                 confirm_label: str | None = None, cancel_label: str = "Cancel") -> bool:
        return StyledDialog(self._parent(parent), title, message, kind="confirm",
                            confirm_label=confirm_label, cancel_label=cancel_label).show()

    def showinfo(self, title: str, message: str, *, parent: tk.Misc | None = None) -> str:
        StyledDialog(self._parent(parent), title, message).show()
        return "ok"

    def showerror(self, title: str, message: str, *, parent: tk.Misc | None = None) -> str:
        StyledDialog(self._parent(parent), title, message, kind="error").show()
        return "ok"


messagebox = LauncherMessageBox()
