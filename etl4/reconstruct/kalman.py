"""Method C: Kinematic + smoothing via a 6-state Kalman filter.

State  x_t = [x, y, z, vx, vy, vz]^T   (local ENU, metres and m/s)
Model  constant velocity:  x_{t+1} = F x_t + w,  F = [[I, dt I], [0, I]]
Obs    z_t = H x_t + v,  H = [I, 0]   (position-only)

Residuals larger than RESIDUAL_SIGMA * sqrt(trace(R)) flag deviations
and the smoothed position replaces the observation.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .kinematic import segment_kinematic

R_EARTH_M = 6_371_000.0
RESIDUAL_SIGMA = 3.0
PROCESS_NOISE  = 1.0     # m/s^2  (tune per aircraft class if needed)
MEAS_NOISE_M   = 25.0    # ADS-B position accuracy, metres


def _enu_from_latlon(lat, lon, alt, lat0, lon0, alt0):
    """Local flat-Earth ENU — fine for per-flight scope (< ~500 km)."""
    lat_rad  = np.radians(lat)
    lat0_rad = np.radians(lat0)
    east  = (lon - lon0) * np.cos(lat0_rad) * R_EARTH_M * np.pi / 180.0
    north = (lat - lat0) * R_EARTH_M * np.pi / 180.0
    up    = (alt - alt0) if alt is not None else 0.0
    return east, north, up


def _latlon_from_enu(east, north, lat0, lon0):
    lat0_rad = np.radians(lat0)
    lat = lat0 + np.degrees(north / R_EARTH_M)
    lon = lon0 + np.degrees(east  / (R_EARTH_M * np.cos(lat0_rad)))
    return lat, lon


class KalmanTrajectoryFilter:
    """6-state constant-velocity filter in a local ENU frame."""

    def __init__(self, process_noise=PROCESS_NOISE, meas_noise=MEAS_NOISE_M):
        self.q = process_noise
        self.r = meas_noise
        self.x = None       # state (6,)
        self.P = None       # covariance (6,6)

    def _reset(self, pos_enu):
        self.x = np.array([pos_enu[0], pos_enu[1], pos_enu[2], 0.0, 0.0, 0.0])
        self.P = np.eye(6) * 1e3

    def _F(self, dt):
        F = np.eye(6)
        F[0, 3] = F[1, 4] = F[2, 5] = dt
        return F

    def _Q(self, dt):
        # discrete white-noise acceleration model
        q = self.q ** 2
        dt2, dt3, dt4 = dt * dt, dt ** 3, dt ** 4
        Q = np.zeros((6, 6))
        Q[0, 0] = Q[1, 1] = Q[2, 2] = dt4 / 4 * q
        Q[0, 3] = Q[3, 0] = Q[1, 4] = Q[4, 1] = Q[2, 5] = Q[5, 2] = dt3 / 2 * q
        Q[3, 3] = Q[4, 4] = Q[5, 5] = dt2 * q
        return Q

    def predict(self, dt):
        F = self._F(dt)
        self.x = F @ self.x
        self.P = F @ self.P @ F.T + self._Q(dt)

    def update(self, pos_enu):
        H = np.zeros((3, 6))
        H[0, 0] = H[1, 1] = H[2, 2] = 1.0
        R = np.eye(3) * self.r ** 2
        y = np.array(pos_enu) - H @ self.x
        S = H @ self.P @ H.T + R
        K = self.P @ H.T @ np.linalg.inv(S)
        self.x = self.x + K @ y
        self.P = (np.eye(6) - K @ H) @ self.P
        return float(np.sqrt(y @ y))   # residual magnitude in metres

    def residual_threshold(self):
        return RESIDUAL_SIGMA * self.r * np.sqrt(3)


def smooth_and_segment(df: pd.DataFrame) -> pd.DataFrame:
    """Run Method B segmentation, then within each flight run the Kalman
    filter and append smoothed_lat/smoothed_lon/kalman_residual columns.
    """
    df = segment_kinematic(df)
    if df.empty:
        for c in ("smoothed_lat", "smoothed_lon", "kalman_residual"):
            df[c] = np.nan
        return df

    out = df.copy()
    out["smoothed_lat"] = np.nan
    out["smoothed_lon"] = np.nan
    out["kalman_residual"] = np.nan

    for fid, grp in out.groupby("flight_id", sort=False):
        n_rows = len(grp)
        idx = grp.index

        times = grp["time"].to_numpy(dtype=float)
        lats  = grp["lat"].to_numpy(dtype=float)
        lons  = grp["lon"].to_numpy(dtype=float)
        alts  = grp["baroaltitude"].to_numpy(dtype=float)

        if n_rows < 2:
            out.loc[idx, "smoothed_lat"] = lats
            out.loc[idx, "smoothed_lon"] = lons
            continue

        lat0 = lats[0]
        lon0 = lons[0]
        alt0 = alts[0] if not np.isnan(alts[0]) else 0.0

        kf = KalmanTrajectoryFilter()
        e0, n0, u0 = _enu_from_latlon(lat0, lon0, alt0, lat0, lon0, alt0)
        kf._reset((e0, n0, u0))

        smoothed_lat = np.empty(n_rows)
        smoothed_lon = np.empty(n_rows)
        residuals    = np.full(n_rows, np.nan)
        prev_t       = times[0]

        for i in range(n_rows):
            alt = alts[i] if not np.isnan(alts[i]) else alt0
            e, nn, u = _enu_from_latlon(lats[i], lons[i], alt, lat0, lon0, alt0)
            if i > 0:
                dt = max(times[i] - prev_t, 1e-3)
                kf.predict(dt)
                residuals[i] = kf.update((e, nn, u))
            lat_s, lon_s = _latlon_from_enu(kf.x[0], kf.x[1], lat0, lon0)
            smoothed_lat[i] = lat_s
            smoothed_lon[i] = lon_s
            prev_t = times[i]

        out.loc[idx, "smoothed_lat"]    = smoothed_lat
        out.loc[idx, "smoothed_lon"]    = smoothed_lon
        out.loc[idx, "kalman_residual"] = residuals

    return out
