# AI expansion

AI is an optional Mapping expansion, not part of first-open Mapping. The
current chat and provider code is mixed into the map runtime; there is no
separate AI artifact or complete source tree here yet. Extract its interface
and provider implementation only after core Mapping runs without it.

Its source export and program artifact follow the same one-tool contract as
the other folders. Both must declare a compatible Mapping interface/version
range. The launcher installs AI only when that compatible Mapping release is
present or the user accepts installing it. Provider settings belong to local
user settings, outside software updates and source exports.

The source and artifact remain planned until the Mapping interface is stable.
