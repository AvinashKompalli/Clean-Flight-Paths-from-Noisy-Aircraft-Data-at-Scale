"""Reference / expected trajectory construction.

Priority of reference sources (per plan.md section 6.6):
  1. Flight-plan record            (if available)
  2. Great-circle route            (nearest airport inference from endpoints)
  3. Historical corridor           (median path across prior days for same callsign)
  4. Smoothed self-reference       (Kalman output of the same flight)
  5. Synthetic ground truth        (controlled runs only)

This module implements the geometric helpers for 2 and the cross-track math.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

R_EARTH_M = 6_371_000.0

AIRPORTS_CSV = Path(__file__).resolve().parents[2] / "data" / "reference" / "airports.csv"


def great_circle_route(lat1, lon1, lat2, lon2, n_points: int = 100) -> pd.DataFrame:
    """Return `n_points` evenly-spaced (lat, lon) samples along the great circle
    from (lat1, lon1) to (lat2, lon2). Uses spherical slerp of unit vectors."""
    p1 = _latlon_to_unit(lat1, lon1)
    p2 = _latlon_to_unit(lat2, lon2)
    dot = np.clip(np.dot(p1, p2), -1.0, 1.0)
    omega = np.arccos(dot)
    if omega < 1e-9:
        return pd.DataFrame({"lat": [lat1] * n_points, "lon": [lon1] * n_points})
    t = np.linspace(0.0, 1.0, n_points)
    a = np.sin((1 - t) * omega) / np.sin(omega)
    b = np.sin(t * omega)       / np.sin(omega)
    pts = a[:, None] * p1 + b[:, None] * p2
    lat = np.degrees(np.arcsin(pts[:, 2]))
    lon = np.degrees(np.arctan2(pts[:, 1], pts[:, 0]))
    return pd.DataFrame({"lat": lat, "lon": lon})


def _latlon_to_unit(lat, lon):
    lat_r, lon_r = np.radians(lat), np.radians(lon)
    return np.array([
        np.cos(lat_r) * np.cos(lon_r),
        np.cos(lat_r) * np.sin(lon_r),
        np.sin(lat_r),
    ])


def _haversine_m(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R_EARTH_M * np.arcsin(np.sqrt(a))


_airports_cache = None


def load_airports() -> pd.DataFrame:
    global _airports_cache
    if _airports_cache is None:
        if not AIRPORTS_CSV.exists():
            # seed with an empty df; caller must populate the file
            _airports_cache = pd.DataFrame(columns=["icao", "iata", "name", "lat", "lon"])
        else:
            _airports_cache = pd.read_csv(AIRPORTS_CSV)
    return _airports_cache


def nearest_airport(lat: float, lon: float, max_km: float = 50.0):
    """ICAO code of the nearest airport within `max_km`, else None."""
    airports = load_airports()
    if airports.empty:
        return None
    d_m = _haversine_m(lat, lon, airports["lat"].to_numpy(), airports["lon"].to_numpy())
    idx = int(np.argmin(d_m))
    if d_m[idx] / 1000.0 > max_km:
        return None
    return airports["icao"].iloc[idx]


def cross_track_distance_m(point_lat, point_lon, path_lat, path_lon):
    """Perpendicular distance (metres) from point(s) to a piecewise great-circle
    path. Accepts either scalar or array point inputs; return matches input shape.

    Vectorised: computes distances from every point to every path midpoint,
    picks the nearest segment per point, then runs the exact cross-track
    formula over just that single segment per point.
    """
    path_lat = np.asarray(path_lat, dtype=float)
    path_lon = np.asarray(path_lon, dtype=float)
    scalar = np.isscalar(point_lat)
    pts_lat = np.atleast_1d(np.asarray(point_lat, dtype=float))
    pts_lon = np.atleast_1d(np.asarray(point_lon, dtype=float))

    if len(path_lat) < 2:
        return float("nan") if scalar else np.full_like(pts_lat, np.nan)

    seg_lat_mid = (path_lat[:-1] + path_lat[1:]) / 2.0
    seg_lon_mid = (path_lon[:-1] + path_lon[1:]) / 2.0

    # (N_points, N_segments) distance matrix to all midpoints
    d2mid = _haversine_m(
        pts_lat[:, None], pts_lon[:, None],
        seg_lat_mid[None, :], seg_lon_mid[None, :],
    )
    nearest = np.argmin(d2mid, axis=1)   # (N_points,)

    a_lat = path_lat[nearest]
    a_lon = path_lon[nearest]
    b_lat = path_lat[nearest + 1]
    b_lon = path_lon[nearest + 1]
    out = np.abs(_cross_track_arc_m(pts_lat, pts_lon, a_lat, a_lon, b_lat, b_lon))
    return float(out[0]) if scalar else out


def _cross_track_arc_m(pt_lat, pt_lon, a_lat, a_lon, b_lat, b_lon):
    """Vectorised cross-track distance from point(s) to the great-circle arc A->B."""
    d13 = _haversine_m(a_lat, a_lon, pt_lat, pt_lon) / R_EARTH_M
    brg13 = _bearing(a_lat, a_lon, pt_lat, pt_lon)
    brg12 = _bearing(a_lat, a_lon, b_lat, b_lon)
    return np.arcsin(np.sin(d13) * np.sin(brg13 - brg12)) * R_EARTH_M


def _bearing(lat1, lon1, lat2, lon2):
    lat1, lat2 = np.radians(lat1), np.radians(lat2)
    dlon = np.radians(lon2 - lon1)
    y = np.sin(dlon) * np.cos(lat2)
    x = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    return np.arctan2(y, x)


def build_reference_for_flight(flight_df: pd.DataFrame, n_points: int = 100) -> pd.DataFrame:
    """Produce an expected path for one reconstructed flight.

    Fallback chain:
      1. Great-circle from first-point airport to last-point airport.
      2. If no nearby airports, straight great-circle between first/last observed points.
    """
    if flight_df.empty:
        return pd.DataFrame(columns=["lat", "lon"])
    first = flight_df.iloc[0]
    last  = flight_df.iloc[-1]
    dep_apt = nearest_airport(first["lat"], first["lon"])
    arr_apt = nearest_airport(last["lat"], last["lon"])
    airports = load_airports()

    if dep_apt and arr_apt and dep_apt != arr_apt:
        dep = airports.loc[airports["icao"] == dep_apt].iloc[0]
        arr = airports.loc[airports["icao"] == arr_apt].iloc[0]
        return great_circle_route(dep["lat"], dep["lon"], arr["lat"], arr["lon"], n_points)
    return great_circle_route(first["lat"], first["lon"], last["lat"], last["lon"], n_points)
