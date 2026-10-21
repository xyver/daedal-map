# Network

Network is a standalone editor and scenario program. Its runnable prototype
currently lives in [`network/`](../../network/README.md), with its own Docker
build and dependencies. This folder is its source entry. Move the program here
after the existing prototype, build context, and data mounts have been checked
against the new path. Mapping's future Network layer adapter must use a
versioned display contract; Mapping does not import Network's editor runtime.

The [single-tool guide](../../docs/WORK_ON_ONE_TOOL.md#network-prototype-available-now)
has a working sparse checkout and independent-project recipe today.
