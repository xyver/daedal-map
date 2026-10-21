# Work on one local tool

DaedalMap keeps its local program source in this repository, while each program
has its own download. You can work on one program without checking out every
source directory. Installed programs share one data folder; a source checkout
does not include data packs.

The five selectable tools have source entries under [`tools/`](../tools/README.md).
Their implementation is moving there in stages:

| Tool | Current source | Small checkout/export today |
|---|---|---|
| Mapping | Root runtime and frontend | Multiple public directories; standalone export pending |
| Network | `network/` | Sparse checkout and independent project recipe below |
| Research MCP | `tools/research-mcp/`, shared `tools/mcp-runtime/`, public reader modules | Public source-run path and local candidate ZIP; publication pending |
| Geometry MCP | `tools/geometry-mcp/`, shared `tools/mcp-runtime/`, public geometry modules | Public source-run path and local candidate ZIP; publication pending |
| AI expansion | Chat/provider code mixed into Mapping | Separate source and artifact pending; requires Mapping |

## Network prototype: available now

The current Network prototype is contained in `network/` for its normal Docker
run. Git's sparse checkout fetches the repository metadata and the files needed
for that directory, rather than the other program trees:

```powershell
git clone --filter=blob:none --sparse https://github.com/xyver/daedal-map.git daedalmap-network
git -C daedalmap-network sparse-checkout set tools network
Set-Location daedalmap-network/network
```

Follow [Network data setup](../network/docs/DATA_SETUP.md) and the
[Network run instructions](../network/README.md). Use a published tag or exact
commit in place of the default branch when reproducing a release. The
`network/legacy/road_access/` helper imports `mapmover`; add `mapmover` to the
sparse checkout if you specifically need that legacy helper:

```powershell
git -C .. sparse-checkout set tools network mapmover
```

To turn the normal Network prototype into an independent project, run this from
the directory containing `daedalmap-network` after the sparse checkout:

```powershell
Copy-Item -LiteralPath .\daedalmap-network\network -Destination .\my-network-tool -Recurse
git -C my-network-tool init
git -C my-network-tool add .
git -C my-network-tool commit -m "Start from DaedalMap Network prototype"
```

Keep the copied `LICENSE` and record the upstream commit in the new project's
README with `git -C daedalmap-network rev-parse HEAD`. This copies
the normal Network prototype only; the legacy helper needs the additional
`mapmover` source mentioned above.

## Mapping and AI: current source layout

Mapping currently uses root modules, `mapmover/`, `static/`, and `templates/`,
so a source checkout needs more than one directory. The simplest development
path today is the [repository setup](../README.md#run-it-yourself). A
tool-specific source export is still needed before we can promise a small
Mapping-only checkout. The Mapping program download, when released, will
contain its required runtime files without the Network or MCP programs. AI is
an optional Mapping expansion with its own source export, dependency profile,
and compatibility range. It cannot yet be checked out or installed separately.

## Research and Geometry MCP

The service-specific MCP source now lives in this public repository. The
Research facade and resources are under [`tools/research-mcp/`](../tools/research-mcp/README.md);
the downloaded-package Geometry backend is under
[`tools/geometry-mcp/`](../tools/geometry-mcp/README.md). They share the local
[`tools/mcp-runtime/`](../tools/mcp-runtime/README.md) server and selected
public runtime modules. Each can run without Mapping. A source checkout for
development can omit Network, Mapping's frontend, and AI:

```powershell
git clone --filter=blob:none --sparse https://github.com/xyver/daedal-map.git daedalmap-mcp
git -C daedalmap-mcp sparse-checkout set tools mapmover
Set-Location daedalmap-mcp
```

That checkout also gets the repository's root shared modules. Install only the
selected profile (`tools/research-mcp/requirements.txt` or
`tools/geometry-mcp/requirements.txt`) and follow that tool's README. The
private release exporter assembles an even smaller dependency-closed ZIP for
each service. Those ZIPs are local candidates; they are not yet published
source-linked downloads. Each ZIP now contains the selected tool, shared
runtime closure, setup guide, `requirements.txt`, `LICENSE`, and `SOURCE.json`.
The source record gives the public HEAD commit and worktree clean state; a
dirty candidate is not an exact source pin.

Before calling either ZIP a forkable public release, finish its source record
and acceptance gate:

- record the exact clean public source commit and reproducible export recipe;
- run an external-client smoke against a selected local `DATA_ROOT`, with real
  Research data or a real extracted Geometry package;
- publish the verified ZIP and manifest at an anonymous public URL.

Then a developer can download **Research MCP source** or **Geometry MCP
source**, extract only that tree, and create their own project repository from
it. The source export and installed program ZIP may share files, but each has
its own purpose and release record.

## Fork versus independent project

Forking `xyver/daedal-map` on GitHub creates a fork of the entire repository;
a sparse checkout only makes the local working copy smaller. To start a
separate project repository for one tool, copy its self-contained source export
into a new directory, retain `LICENSE` and upstream source information, then
initialize that directory as a new Git repository. This is the intended path
for projects built on Research MCP. It must not depend on files left behind in
the monorepo.
