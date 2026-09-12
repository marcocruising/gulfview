---
title: Infrastructure map
---

[Home](index.md) · [Map](infrastructure-map.md) · [Trade](trade.md) · [Prices](prices.md) · [Crops](crops.md) · [Data](data.md)

# Infrastructure map

![Middle East: plant dots and oil/gas pipeline lines](screenshots/05-infrastructure-map.png)

Sidebar section: **Infrastructure map (GEM assets)**. Default geographic filter is a rough **Middle East** box.

**Dots** are Global Energy Monitor plants — cement, chemicals, iron and steel, iron ore mines. Colour is category. In this view: 6,623 geocoded rows worldwide, **586** after the area filter.

**Lines** are oil/NGL (red) and gas (blue) from downloaded GeoJSON:

- `data/globalenergymonitor/GEM-GOIT-Oil-NGL-Pipelines-2025-03/`
- `data/globalenergymonitor/GEM-GGIT-Gas-Pipelines-2025-11/`

Those files are used because the hosted PostGIS pipeline table is not available. Excel pipeline *points* in `gem_tracker_rows` usually have no coordinates and are skipped.

**Undersea internet cables** (TeleGeography) are a separate toggle, off by default; routes come from the dump tables, not the GeoJSON.
