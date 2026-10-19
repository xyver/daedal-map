from __future__ import annotations

from abc import ABC, abstractmethod
import json
from math import cos, radians
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


EARTH_RADIUS_KM = 6371.0088


def haversine_km(
    origin_lon: float,
    origin_lat: float,
    destination_lons: np.ndarray,
    destination_lats: np.ndarray,
) -> np.ndarray:
    lon1 = radians(origin_lon)
    lat1 = radians(origin_lat)
    lon2 = np.radians(destination_lons)
    lat2 = np.radians(destination_lats)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2.0) ** 2 + cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arcsin(np.sqrt(a))
    return EARTH_RADIUS_KM * c


class TravelTimeProvider(ABC):
    provider_name: str

    @abstractmethod
    def compute_batch(self, origin_row: pd.Series, destinations_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        raise NotImplementedError

    def compute_distance_km(self, origin_row: pd.Series, destinations_df: pd.DataFrame) -> np.ndarray:
        distances_km, _ = self.compute_batch(origin_row, destinations_df)
        return distances_km

    def compute_minutes(self, origin_row: pd.Series, destinations_df: pd.DataFrame) -> np.ndarray:
        _, minutes = self.compute_batch(origin_row, destinations_df)
        return minutes


class AirDistanceTravelTimeProvider(TravelTimeProvider):
    """Debug-only provider until a real router adapter is wired in."""

    provider_name = "air_distance_debug"

    def __init__(self, *, assumed_speed_kph: float = 55.0, detour_factor: float = 1.35) -> None:
        if assumed_speed_kph <= 0:
            raise ValueError("assumed_speed_kph must be > 0")
        if detour_factor <= 0:
            raise ValueError("detour_factor must be > 0")
        self.assumed_speed_kph = float(assumed_speed_kph)
        self.detour_factor = float(detour_factor)

    def compute_batch(self, origin_row: pd.Series, destinations_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        km = haversine_km(
            float(origin_row["representative_lon"]),
            float(origin_row["representative_lat"]),
            destinations_df["destination_lon"].to_numpy(dtype=float),
            destinations_df["destination_lat"].to_numpy(dtype=float),
        ) * self.detour_factor
        minutes = (km / self.assumed_speed_kph) * 60.0
        return km, minutes


class ValhallaMatrixTravelTimeProvider(TravelTimeProvider):
    """Valhalla matrix adapter for one-origin-to-many-destination batches."""

    provider_name = "valhalla_matrix"

    def __init__(
        self,
        *,
        service_url: str,
        costing: str = "auto",
        units: str = "kilometers",
        timeout_seconds: float = 120.0,
        target_batch_size: int = 200,
        max_target_haversine_km: float = 150.0,
        max_target_candidates: int | None = None,
        search_radius_meters: float | None = 5000.0,
        verbose: bool = False,
        costing_options: dict[str, Any] | None = None,
    ) -> None:
        if not service_url:
            raise ValueError("service_url is required for Valhalla provider")
        if target_batch_size <= 0:
            raise ValueError("target_batch_size must be > 0")
        self.service_url = service_url.rstrip("/")
        self.costing = costing
        self.units = units
        self.timeout_seconds = float(timeout_seconds)
        self.target_batch_size = int(target_batch_size)
        self.max_target_haversine_km = float(max_target_haversine_km)
        self.max_target_candidates = int(max_target_candidates) if max_target_candidates else None
        self.search_radius_meters = float(search_radius_meters) if search_radius_meters else None
        self.verbose = bool(verbose)
        self.costing_options = costing_options or {}
        self.last_candidate_count = 0
        self.last_candidate_limited = False

    def _location(self, *, lat: float, lon: float) -> dict[str, float]:
        location = {"lat": float(lat), "lon": float(lon)}
        if self.search_radius_meters is not None:
            location["radius"] = self.search_radius_meters
        return location

    def _build_payload(self, origin_row: pd.Series, targets_df: pd.DataFrame) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "sources": [
                self._location(
                    lat=float(origin_row["representative_lat"]),
                    lon=float(origin_row["representative_lon"]),
                )
            ],
            "targets": [
                self._location(
                    lat=float(row["destination_lat"]),
                    lon=float(row["destination_lon"]),
                )
                for _, row in targets_df.iterrows()
            ],
            "costing": self.costing,
            "units": self.units,
            "verbose": self.verbose,
        }
        if self.costing_options:
            payload["costing_options"] = self.costing_options
        return payload

    def _request_matrix(self, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = f"{self.service_url}/sources_to_targets"
        body = json.dumps(payload).encode("utf-8")
        request = Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            if exc.code == 400:
                raise RuntimeError(f"Valhalla HTTP error {exc.code}: {details}") from exc
            # Some Valhalla deployments document GET with ?json=... more explicitly than POST.
            if exc.code not in (404, 405, 415):
                raise RuntimeError(f"Valhalla HTTP error {exc.code}: {details}") from exc
        except (TimeoutError, OSError, URLError) as exc:
            raise RuntimeError(f"Valhalla connection error: {exc}") from exc

        query = quote(json.dumps(payload, separators=(",", ":")), safe="")
        fallback_url = f"{endpoint}?json={query}"
        try:
            with urlopen(fallback_url, timeout=self.timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Valhalla HTTP error {exc.code}: {details}") from exc
        except (TimeoutError, OSError, URLError) as exc:
            raise RuntimeError(f"Valhalla connection error: {exc}") from exc

    @staticmethod
    def _coerce_concise_arrays(payload: dict[str, Any]) -> tuple[list[float | None], list[float | None]]:
        matrix_payload = payload.get("sources_to_targets")
        if isinstance(matrix_payload, dict):
            durations = matrix_payload.get("durations") or []
            distances = matrix_payload.get("distances") or []
            if durations and isinstance(durations[0], list):
                durations = durations[0]
            if distances and isinstance(distances[0], list):
                distances = distances[0]
            return list(distances), list(durations)

        verbose_rows = payload.get("sources_to_targets") or []
        if isinstance(verbose_rows, list):
            verbose_rows = sorted(verbose_rows, key=lambda row: int(row.get("to_index", 0)))
            return [row.get("distance") for row in verbose_rows], [row.get("time") for row in verbose_rows]
        raise RuntimeError("Unsupported Valhalla matrix response shape")

    def compute_batch(self, origin_row: pd.Series, destinations_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        all_distances = np.full(len(destinations_df), float("inf"), dtype=float)
        all_minutes = np.full(len(destinations_df), float("inf"), dtype=float)
        straight_line_km = haversine_km(
            float(origin_row["representative_lon"]),
            float(origin_row["representative_lat"]),
            destinations_df["destination_lon"].to_numpy(dtype=float),
            destinations_df["destination_lat"].to_numpy(dtype=float),
        )
        candidate_indices = np.flatnonzero(straight_line_km <= self.max_target_haversine_km)
        if len(candidate_indices) == 0 and len(destinations_df) > 0:
            candidate_indices = np.array([int(straight_line_km.argmin())], dtype=int)
        if self.max_target_candidates and len(candidate_indices) > self.max_target_candidates:
            candidate_indices = candidate_indices[
                np.argsort(straight_line_km[candidate_indices])[: self.max_target_candidates]
            ]
            self.last_candidate_limited = True
        else:
            self.last_candidate_limited = False
        self.last_candidate_count = int(len(candidate_indices))

        candidate_df = destinations_df.iloc[candidate_indices].copy()
        def route_batch(batch_indices: np.ndarray, batch_df: pd.DataFrame) -> None:
            payload = self._build_payload(origin_row, batch_df)
            try:
                response_payload = self._request_matrix(payload)
            except RuntimeError as exc:
                if (
                    "No path could be found" in str(exc)
                    or "No suitable edges near location" in str(exc)
                    or "timed out" in str(exc)
                    or "forcibly closed" in str(exc)
                ):
                    if len(batch_df) <= 1:
                        return
                    midpoint = len(batch_df) // 2
                    route_batch(batch_indices[:midpoint], batch_df.iloc[:midpoint].copy())
                    route_batch(batch_indices[midpoint:], batch_df.iloc[midpoint:].copy())
                    return
                raise
            distance_values, minute_values = self._coerce_concise_arrays(response_payload)
            if len(distance_values) != len(batch_df) or len(minute_values) != len(batch_df):
                raise RuntimeError(
                    f"Valhalla matrix response length mismatch: got {len(distance_values)} distances and "
                    f"{len(minute_values)} durations for {len(batch_df)} targets"
                )
            for local_idx, (dist, seconds) in enumerate(zip(distance_values, minute_values)):
                destination_idx = int(batch_indices[local_idx])
                all_distances[destination_idx] = float(dist) if dist is not None else float("inf")
                all_minutes[destination_idx] = (float(seconds) / 60.0) if seconds is not None else float("inf")

        for start in range(0, len(candidate_df), self.target_batch_size):
            batch_indices = candidate_indices[start : start + self.target_batch_size]
            batch_df = candidate_df.iloc[start : start + self.target_batch_size].copy()
            route_batch(batch_indices, batch_df)
        return all_distances, all_minutes
