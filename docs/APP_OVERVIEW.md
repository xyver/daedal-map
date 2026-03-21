# App Overview

Read order:
- start with [../README.md](../README.md) for setup
- then [LOCAL_AND_HOSTED.md](LOCAL_AND_HOSTED.md) for runtime mode selection
- use this doc for the higher-level app/runtime model

DaedalMap is a map-first geographic query engine.

The current app combines:
- natural-language query interpretation
- map-first rendering and interaction
- public and maintained data sources
- runtime data access through local files or object storage
- optional account-linked runtime access

## Core Mental Model

The app is designed around three user questions:
- where
- what
- when

The runtime turns those into executable map/data operations and renders the result directly in the app.

## Main Surfaces

- `/`
  Main app/map surface
- `/reference/admin-levels`
  Runtime helper endpoint for map admin-level labels

## Current Runtime Direction

The app can operate in multiple modes:

1. full local data tree
2. S3/R2-backed runtime mode
3. future installable runtime plus pack delivery mode

The same runtime code supports these modes through path and storage helpers.

## Data Direction

The long-term data model is moving toward pack-oriented delivery rather than treating the repo itself as the unit of release.

Important ideas:
- packs can exist without being active
- runtimes may differ in what is installed or remotely available
- user/account access can eventually affect what the active catalog includes

## Current Account Direction

Guest use remains important for evaluation and simple exploration.

Hosted login and account ownership can live on `daedalmap.com`, but this is optional for the public/self-host runtime.

The public app still:
- accepts authenticated runtime requests
- reads user/account context where needed for runtime behavior
- can link users out to a paired account surface when one is configured

In the hosted DaedalMap deployment, the private site owns:
- login/signup
- account overview
- billing/credits
- admin runtime/release visibility

For self-host/local use:
- Supabase is optional
- the local runtime can operate without a hosted account system
- `/settings` acts as a local runtime setup page when hosted auth is not configured

This is still evolving, but the intended split is:
- public repo = self-hostable runtime
- private repo = hosted account, billing, and product convenience layer
