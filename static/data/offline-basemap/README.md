# Offline world basemap

Explore Map in launcher mode uses these Natural Earth 1:50m GeoJSON files by
default. They provide land and country outlines without requesting remote map
tiles. Together they occupy 4,719,656 bytes in the engine source. Their source
URLs are:

- https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_50m_land.geojson
- https://github.com/nvkelso/natural-earth-vector/blob/master/geojson/ne_50m_admin_0_countries.geojson

When no geometry pack is installed, the local Mapping runtime also uses the
country file as a basic Admin0 display fallback. The fallback gives the Admin
Layers control country shapes and names. It is visual data only: exact
containment, deep administrative levels, and historical geometry still require
their respective downloaded packs.

The Mapping tab can download Natural Earth 1:10m land and country
geometry into the local runtime tile folder (about 23.4 MB from the source
file sizes checked on 2026-10-21). The 1:10m files improve coastline and
border detail; neither option includes roads or place labels. Local Explore Map
uses only these bundled or downloaded map assets during normal use.

Natural Earth states that its vector data is [public domain](https://www.naturalearthdata.com/about/terms-of-use/).
