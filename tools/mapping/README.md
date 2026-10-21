# Mapping

Mapping is the base interactive map program. Its implementation currently spans
root `app.py`, `mapmover/`, `static/`, `templates/`, and shared root modules.
This folder is its source entry; moving the runtime here requires preserving
the hosted app and local launch/import paths. The Mapping source export must
include its runtime closure, renderer assets, local dependency profile, and
project reopen check. The optional AI extension is owned separately in
[`tools/ai/`](../ai/README.md).

Current source-run instructions: [repository README](../../README.md#run-it-yourself).
