# App Overview

DaedalMap is a map-first geographic query engine.

The current app combines:
- natural-language query interpretation
- map-first rendering and interaction
- public and maintained data sources
- runtime data access through local files or object storage
- optional account-backed persistence and settings

## Core Mental Model

The app is designed around three user questions:
- where
- what
- when

The runtime turns those into executable map/data operations and renders the result directly in the app.

## Main Surfaces

- `/`
  Main app/map surface
- `/settings`
  Account/workspace settings surface

## Current Runtime Direction

The app can operate in multiple modes:

1. bundled demo data
2. fuller local data tree
3. S3/R2-backed runtime mode

The same runtime code supports these modes through path and storage helpers.

## Data Direction

The long-term data model is moving toward pack-oriented delivery rather than treating the repo itself as the unit of release.

Important ideas:
- packs can exist without being active
- runtimes may differ in what is installed or remotely available
- user/account access can eventually affect what the active catalog includes

## Current Account Direction

Guest use remains important for evaluation and simple exploration.

Logged-in use is where the app is starting to mean more:
- user-scoped persistence
- settings-backed workspace behavior
- future entitlement-aware access

This is still evolving.
