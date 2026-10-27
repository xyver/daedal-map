"""Shared visual theme for the local DaedalMap launcher.

Tkinter does not have a CSS file, so this module plays that role:
- central color tokens
- central font tokens
- small widget-style helper functions

Keep this intentionally lightweight so the launcher can evolve over time
without scattering visual values across the UI code.
"""

from __future__ import annotations


THEME = {
    "colors": {
        "app_bg": "#07111a",
        "panel_bg": "#0a1622",
        "panel_alt_bg": "#102538",
        "card_bg": "#0d1d2c",
        "text": "#edf4fb",
        "muted_text": "#a7bfce",
        "accent": "#8cecff",
        "accent_strong": "#63b9ff",
        "accent_active": "#c8e8ff",
        "program_accent": "#ffaaa2",
        "mode_connected_bg": "#153e52",
        "mode_connected_fg": "#9cecff",
        "mode_local_bg": "#503527",
        "mode_local_fg": "#ffd4a6",
        "mode_pending_bg": "#5a4827",
        "mode_pending_fg": "#ffe09a",
        "danger": "#a84c4c",
        "danger_active": "#c36464",
        "input_bg": "#071520",
        "log_bg": "#050e16",
        "border": "#294358",
        "selection": "#173e58",
        "tab_active": "#14334a",
        "tab_inactive": "#0a1824",
    },
    "fonts": {
        "title": ("Segoe UI", 18, "bold"),
        "section": ("Segoe UI", 10, "bold"),
        "card": ("Segoe UI", 12, "bold"),
        "body": ("Segoe UI", 10),
        "button": ("Segoe UI", 10, "bold"),
        "mono": ("Consolas", 9),
    },
    "spacing": {
        "page_pad": 14,
        "section_gap": 12,
        "inner_pad": 12,
        "button_gap": 8,
    },
}


def app_root_kwargs() -> dict:
    return {"bg": THEME["colors"]["app_bg"]}


def panel_kwargs() -> dict:
    return {
        "bg": THEME["colors"]["panel_bg"],
        "bd": 1,
        "highlightthickness": 1,
        "highlightbackground": THEME["colors"]["border"],
        "highlightcolor": THEME["colors"]["border"],
    }


def label_kwargs(role: str = "body") -> dict:
    font = THEME["fonts"]["body"]
    fg = THEME["colors"]["text"]
    if role == "title":
        font = THEME["fonts"]["title"]
    elif role == "section":
        font = THEME["fonts"]["section"]
        fg = THEME["colors"]["muted_text"]
    elif role == "muted":
        fg = THEME["colors"]["muted_text"]
    return {
        "bg": THEME["colors"]["panel_bg"],
        "fg": fg,
        "font": font,
    }


def button_kwargs(variant: str = "primary") -> dict:
    bg = THEME["colors"]["panel_alt_bg"]
    active = THEME["colors"]["accent"]
    fg = THEME["colors"]["text"]
    if variant == "accent":
        bg = THEME["colors"]["accent_strong"]
        active = THEME["colors"]["accent_active"]
        fg = "#07111a"
    elif variant == "danger":
        bg = THEME["colors"]["danger"]
        active = THEME["colors"]["danger_active"]
    return {
        "font": THEME["fonts"]["button"],
        "bg": bg,
        "fg": fg,
        "activebackground": active,
        "activeforeground": fg,
        "relief": "flat",
        "bd": 0,
        "padx": 12,
        "pady": 9,
        "highlightthickness": 1,
        "highlightbackground": THEME["colors"]["border"],
        "cursor": "hand2",
    }


def listbox_kwargs() -> dict:
    return {
        "bg": THEME["colors"]["input_bg"],
        "fg": THEME["colors"]["text"],
        "selectbackground": THEME["colors"]["selection"],
        "selectforeground": THEME["colors"]["text"],
        "highlightthickness": 1,
        "highlightbackground": THEME["colors"]["border"],
        "highlightcolor": THEME["colors"]["accent"],
        "bd": 0,
        "relief": "flat",
        "font": THEME["fonts"]["body"],
    }


def text_kwargs() -> dict:
    return {
        "bg": THEME["colors"]["log_bg"],
        "fg": THEME["colors"]["text"],
        "insertbackground": THEME["colors"]["accent_active"],
        "highlightthickness": 1,
        "highlightbackground": THEME["colors"]["border"],
        "highlightcolor": THEME["colors"]["accent"],
        "bd": 0,
        "relief": "flat",
        "font": THEME["fonts"]["mono"],
        "padx": 10,
        "pady": 10,
    }


def scrollbar_kwargs() -> dict:
    return {
        "bg": THEME["colors"]["panel_alt_bg"],
        "activebackground": THEME["colors"]["accent"],
        "troughcolor": THEME["colors"]["app_bg"],
        "bd": 0,
        "highlightthickness": 0,
        "relief": "flat",
    }


def tab_button_kwargs(active: bool = False) -> dict:
    bg = THEME["colors"]["tab_active"] if active else THEME["colors"]["tab_inactive"]
    fg = THEME["colors"]["text"] if active else THEME["colors"]["muted_text"]
    active_bg = THEME["colors"]["tab_active"]
    return {
        "font": THEME["fonts"]["button"],
        "bg": bg,
        "fg": fg,
        "activebackground": active_bg,
        "activeforeground": THEME["colors"]["text"],
        "relief": "flat",
        "bd": 0,
        "padx": 12,
        "pady": 8,
        "highlightthickness": 0,
        "cursor": "hand2",
    }
