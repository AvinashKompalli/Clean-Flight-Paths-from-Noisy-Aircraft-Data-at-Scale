"""Download OpenSky data from its S3 bucket OR the Zenodo mirror, and convert
to bronze Parquet.

No Trino. No account. Two public sources are supported:

  s3     The OpenSky `data-samples` bucket (per-hour `.csv.tar` state vectors).
         Browsed via an S3 XML API; URL auto-discovered from the bucket
         browser page.
  zenodo The OpenSky "Crowdsourced air traffic data" record series on Zenodo
         (concept DOI 10.5281/zenodo.3737101). Files are monthly
         `flightlist_YYYYMMDD_YYYYMMDD.csv.gz` — per-flight summaries, not
         1 Hz state vectors. Use for flightlist analyses; the state-vector
         pipeline cannot consume these directly.

You MUST read and accept the LICENSE in the source before you use the data.

Modes:
  --list      List folders and files (S3) or files in the latest Zenodo
              version. Use this first.
  --download  Fetch files. On Zenodo, --latest grabs the newest-dated
              flightlist in the latest version; --bulk-pull grabs EVERY
              file matching --prefix (first-time hydration of ~10 GB).
  --convert   Read the .csv.gz files, write bronze Parquet with an explicit
              schema, and print the volume numbers for the report.
              S3 only: Zenodo flightlists have a different schema.

Zenodo idempotency: a local file is skipped only when its size AND its
checksum match the record manifest. Zenodo returns the checksum as
`<algo>:<hex>` (md5 today, sha256 compatible). Mismatches redownload.

Examples:
  python etl4/screpper.py --list
  python etl4/screpper.py --list --prefix states/2026-10-20/
  python etl4/screpper.py --download --prefix states/2026-10-20/ --hours 0
  python etl4/screpper.py --convert --date 2026-10-20 --hours 0
  python etl4/screpper.py --repo zenodo --list
  python etl4/screpper.py --repo zenodo --download --latest
  python etl4/screpper.py --repo zenodo --download --bulk-pull --prefix flightlist_2022
"""
import argparse
import datetime
import gzip
import hashlib
import re
import sys
import tarfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
_cfg_file = ROOT / "config" / "config.yaml"
CFG = yaml.safe_load(_cfg_file.read_text()) if _cfg_file.exists() else {
    "paths": {"bronze_raw_states": "data/bronze/raw_states"}
}

BROWSER_URL = "https://opensky-network.org/datasets/"
RAW_DIR = ROOT / "data" / "raw" / "opensky_states"
ZENODO_RAW_DIR = ROOT / "data" / "raw" / "opensky_flightlists"
UA = "etl4-iisc-student-project/0.2 (academic use)"
DELAY_S = 1.0          # be polite. Do not remove.
MAX_RETRIES = 3
OPENSKY_FIRST_DATE = "2019-01-01"  # oldest date in the S3 bucket
S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# Zenodo API endpoint + the OpenSky "Crowdsourced air traffic data" concept.
# The concept ID is stable; the version record changes with each monthly
# refresh (v20.10, v21.01, ..., v23.00, ...). We always resolve to the
# newest version at query time.
ZENODO_API = "https://zenodo.org/api"
ZENODO_OPENSKY_FLIGHTLIST_CONCEPT = 3737101

# Explicit schema. CSV carries no types. Inference corrupts four of these:
# icao24 loses leading zeros, squawk is octal text, the flags are text.
DTYPES = {
    "icao24": "string", "callsign": "string", "squawk": "string",
    "lat": "float64", "lon": "float64", "velocity": "float64",
    "heading": "float64", "vertrate": "float64", "baroaltitude": "float64",
    "geoaltitude": "float64", "lastposupdate": "float64",
    "lastcontact": "float64", "time": "float64",
    "onground": "string", "alert": "string", "spi": "string",
}
BOOL_COLS = ["onground", "alert", "spi"]
TRUE_SET = {"true", "True", "TRUE", "1", "t"}


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------
def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def get(s, url, stream=False, params=None, headers=None):
    for attempt in range(MAX_RETRIES):
        try:
            r = s.get(url, stream=stream, params=params, headers=headers,
                      timeout=90)
            if r.status_code in (200, 206):
                return r
            if r.status_code in (403, 404, 416):
                raise RuntimeError(f"HTTP {r.status_code} for {r.url}")
            print(f"  HTTP {r.status_code}, retry {attempt + 1}")
        except requests.RequestException as e:
            print(f"  {type(e).__name__}: {e}, retry {attempt + 1}")
            if attempt == MAX_RETRIES - 1:
                raise
        time.sleep(2 ** attempt)
    raise RuntimeError(f"failed after {MAX_RETRIES} tries: {url}")


# --------------------------------------------------------------------------
# S3 discovery and listing
# --------------------------------------------------------------------------
def discover_bucket(s, page_url=BROWSER_URL):
    """Find the bucket URL inside the bucket-browser page.

    The browser app holds a config object with a bucketUrl field.
    """
    r = get(s, page_url)
    html = r.text
    pats = [
        r'bucketUrl\s*[:=]\s*[\'"]([^\'"]+)[\'"]',
        r'"bucketUrl"\s*:\s*"([^"]+)"',
    ]
    for p in pats:
        m = re.search(p, html)
        if m:
            return m.group(1).rstrip("/")
    # The config may sit in a separate file next to the page.
    for name in ("config.json", "config.js"):
        try:
            r2 = get(s, page_url.rstrip("/") + "/" + name)
            for p in pats:
                m = re.search(p, r2.text)
                if m:
                    return m.group(1).rstrip("/")
        except Exception:
            pass
    raise SystemExit(
        "Could not find bucketUrl in the page.\n"
        "Open the page, view source, search for 'bucketUrl', and pass it:\n"
        "  --bucket-url https://<the value you found>"
    )


def s3_list(s, bucket_url, prefix="", delimiter="/"):
    """List one level of the bucket. Returns (folders, files).

    files is a list of (key, size_bytes). Handles pagination.
    """
    folders, files, token = [], [], None
    while True:
        params = {"list-type": "2", "prefix": prefix, "delimiter": delimiter}
        if token:
            params["continuation-token"] = token
        r = get(s, bucket_url, params=params)
        root = ET.fromstring(r.text)

        for cp in root.findall(f"{S3_NS}CommonPrefixes"):
            p = cp.findtext(f"{S3_NS}Prefix")
            if p:
                folders.append(p)
        for c in root.findall(f"{S3_NS}Contents"):
            k = c.findtext(f"{S3_NS}Key")
            sz = int(c.findtext(f"{S3_NS}Size") or 0)
            if k and k != prefix:
                files.append((k, sz))

        if root.findtext(f"{S3_NS}IsTruncated") == "true":
            token = root.findtext(f"{S3_NS}NextContinuationToken")
            if not token:
                break
        else:
            break
    return folders, files


def key_url(bucket_url, key):
    return f"{bucket_url}/{key}"


def match_hour(key, hour):
    """True if the LAST number in the file name is this hour.

    The last number is the hour. Earlier numbers belong to the date, so a
    plain search would match hour 10 or 20 inside '2026-10-20'.
    """
    stem = key.rsplit("/", 1)[-1].split(".")[0]
    groups = re.findall(r"\d+", stem)
    if not groups:
        return False
    last = groups[-1]
    return len(last) <= 2 and int(last) == hour


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------
def download(s, url, dest, max_mb, quiet=False):
    if dest.exists() and dest.stat().st_size > 0:
        print(f"skip {dest.name}: exists ({dest.stat().st_size / 1e6:.1f} MB)")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    resume_from = tmp.stat().st_size if tmp.exists() else 0
    headers = {"Range": f"bytes={resume_from}-"} if resume_from else None
    r = get(s, url, stream=True, headers=headers)

    if resume_from and r.status_code != 206:
        resume_from = 0
        tmp.unlink(missing_ok=True)

    cl = int(r.headers.get("content-length", 0))
    total = cl + resume_from if r.status_code == 206 else cl
    if max_mb and total and total > max_mb * 1e6:
        raise SystemExit(
            f"{dest.name} is {total / 1e6:.0f} MB, over --max-mb. "
            "Raise the limit on purpose, so you do not fill the disk."
        )
    got = resume_from
    mode = "ab" if resume_from else "wb"
    with open(tmp, mode) as f:
        for chunk in r.iter_content(chunk_size=1 << 20):
            f.write(chunk)
            got += len(chunk)
            if max_mb and got > max_mb * 1e6:
                raise SystemExit(f"stopped at {got / 1e6:.0f} MB, over --max-mb")
            if not quiet:
                print(f"\r  {dest.name}: {got / 1e6:7.1f} MB"
                      + (f"  {100 * got / total:5.1f}%" if total else ""), end="")
    if not quiet:
        print()
    tmp.rename(dest)
    time.sleep(DELAY_S)
    return dest


def list_available_dates(s, bucket):
    """Return sorted list of publicly accessible date strings in states/."""
    folders, _ = s3_list(s, bucket, "states/")
    dates = []
    for f in folders:
        # skip dot-prefix folders (restricted/legacy): states/.2019-01-07/
        m = re.search(r"states/(\d{4}-\d{2}-\d{2})/", f)
        if m:
            dates.append(m.group(1))
    return sorted(dates)


def download_date(s, bucket, date_str, hours, max_mb, dry_run=False):
    """Download CSV tar files for one date. Returns (n_files, total_bytes).

    Actual bucket layout: states/YYYY-MM-DD/HH/states_YYYY-MM-DD-HH.csv.tar
    Uses a flat (no-delimiter) listing to find all files in one API call.
    """
    prefix = f"states/{date_str}/"
    # delimiter="" → recursive, returns every key under the prefix
    _, all_files = s3_list(s, bucket, prefix, delimiter="")
    hour_set = set(hours)
    want = []
    for k, sz in all_files:
        if not k.endswith('.csv.tar'):
            continue
        # key: states/YYYY-MM-DD/HH/states_YYYY-MM-DD-HH.csv.tar
        parts = k.split('/')
        if len(parts) >= 3:
            try:
                if int(parts[-2]) in hour_set:
                    want.append((k, sz))
            except ValueError:
                pass
    if not want:
        print(f"[{date_str}] no files in bucket -- skipping")
        return 0, 0
    total_bytes = sum(sz for _, sz in want)
    print(f"[{date_str}] {len(want)} file(s)  {total_bytes / 1e6:.0f} MB")
    if dry_run:
        for k, sz in want:
            print(f"  DRY-RUN  {sz / 1e6:7.1f} MB  {key_url(bucket, k)}")
        return len(want), total_bytes
    for k, _ in want:
        dest = RAW_DIR / date_str / k.rsplit("/", 1)[-1]
        print(f"  GET {key_url(bucket, k)}")
        download(s, key_url(bucket, k), dest, max_mb)
    return len(want), total_bytes


# --------------------------------------------------------------------------
# Zenodo (per-flight "flightlist" record series)
# --------------------------------------------------------------------------
def zenodo_latest_version(s, concept_id=ZENODO_OPENSKY_FLIGHTLIST_CONCEPT):
    """Return the Zenodo record metadata for the newest version of a concept.

    Zenodo stores a concept as the lineage and each dated release as its
    own record. The newest is the first hit when sorted by publication
    date descending.
    """
    url = f"{ZENODO_API}/records"
    r = get(s, url, params={
        "q": f"conceptrecid:{concept_id}",
        "all_versions": 1,
        "sort": "mostrecent",
        "size": 1,
    })
    hits = r.json().get("hits", {}).get("hits", [])
    if not hits:
        raise RuntimeError(f"No Zenodo records for concept {concept_id}")
    return hits[0]


def zenodo_list(s, concept_id=ZENODO_OPENSKY_FLIGHTLIST_CONCEPT, prefix=""):
    """List files in the latest version of a Zenodo concept.

    Returns the record metadata and a filtered list of {key, size, url}.
    `prefix` is a filename substring match (not an S3-style path).
    """
    rec = zenodo_latest_version(s, concept_id)
    files = []
    for f in rec.get("files", []):
        key = f.get("key", "")
        if prefix and prefix not in key:
            continue
        files.append({
            "key":      key,
            "size":     int(f.get("size", 0)),
            "url":      f.get("links", {}).get("self", ""),
            "checksum": f.get("checksum", ""),
        })
    # Sort by filename: the per-month flightlists sort by date naturally.
    files.sort(key=lambda d: d["key"])
    return rec, files


_FLIGHTLIST_DATE_RE = re.compile(r"(\d{8})_(\d{8})")


def zenodo_pick_latest_dated(files):
    """Return the flightlist file whose date range ends latest.

    The OpenSky flightlist naming pattern is flightlist_YYYYMMDD_YYYYMMDD.*.
    Picks the file with the maximum second date; falls back to the last
    file in name order if no files match the pattern.
    """
    dated = []
    for f in files:
        m = _FLIGHTLIST_DATE_RE.search(f["key"])
        if m:
            dated.append((m.group(2), f))
    if not dated:
        return files[-1] if files else None
    dated.sort(key=lambda t: t[0])
    return dated[-1][1]


_HASH_CHUNK = 1 << 16  # 65 KiB; small enough to stream very large files.


def _file_hash(path: Path, algo: str) -> str:
    """Return the hex digest of `path` using `algo` (as named by hashlib).

    Streams the file in 65 KiB chunks so a 10 GB input never materialises
    in memory. Returns "" when the algorithm is not available locally;
    the caller falls back to size-only idempotency.
    """
    try:
        h = hashlib.new(algo)
    except ValueError:
        return ""
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def zenodo_download(s, files, out_dir, max_mb, dry_run=False, workers=4):
    """Fetch each file in `files` into out_dir. Idempotent: skip existing.

    Idempotency is size+checksum: a local file is kept only when its byte
    size matches the manifest AND its digest matches the manifest's
    `<algo>:<hex>` entry. A size match with a digest mismatch triggers a
    full redownload (the partial is removed first).

    Parallel across files via a thread pool (workers>1). Each worker uses its
    own HTTP session. Per-file resume is handled inside download().

    Returns (n_files, n_bytes) actually downloaded (0 when dry_run).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    out_dir = Path(out_dir)
    to_fetch = []
    for f in files:
        size_mb = f["size"] / 1e6
        if dry_run:
            print(f"  DRY-RUN  {size_mb:8.1f} MB  {f['url']}")
            continue
        dest = out_dir / f["key"]
        if dest.exists() and dest.stat().st_size == f["size"]:
            checksum = f.get("checksum", "")
            if checksum and ":" in checksum:
                algo, expected = checksum.split(":", 1)
                algo = algo.lower()
                actual = _file_hash(dest, algo)
                if not actual:
                    print(f"  SKIP     {size_mb:8.1f} MB  {dest.name} "
                          f"(size match; {algo} unavailable)")
                    continue
                if actual == expected:
                    print(f"  HASH OK  {size_mb:8.1f} MB  {dest.name} ({algo})")
                    continue
                print(f"  REDO     {size_mb:8.1f} MB  {dest.name} "
                      f"({algo} mismatch: want {expected[:10]}..., got {actual[:10]}...)")
                dest.unlink()
            else:
                print(f"  SKIP     {size_mb:8.1f} MB  {dest.name} "
                      f"(size match; no manifest checksum)")
                continue
        to_fetch.append((f, dest))

    if dry_run or not to_fetch:
        return 0, 0

    print(f"  queue    {len(to_fetch)} file(s), workers={workers}", flush=True)

    def _one(entry):
        f, dest = entry
        tmp = dest.with_suffix(dest.suffix + ".part")
        pre = tmp.stat().st_size if tmp.exists() else 0
        tag = "RESUME" if pre else "GET   "
        print(f"  {tag}   {f['size']/1e6:8.1f} MB  {dest.name}"
              + (f"  from {pre/1e6:.1f} MB" if pre else ""), flush=True)
        local_s = session()
        t0 = time.time()
        download(local_s, f["url"], dest, max_mb, quiet=True)
        dt = time.time() - t0
        new_bytes = f["size"] - pre
        rate = new_bytes / 1e6 / max(dt, 1e-6)
        print(f"  done     {f['size']/1e6:8.1f} MB  {dest.name}  "
              f"({dt:.1f}s, {rate:.1f} MB/s)", flush=True)
        return f["size"]

    total_bytes = 0
    n_out = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_one, e) for e in to_fetch]
        for fut in as_completed(futs):
            total_bytes += fut.result()
            n_out += 1
    return n_out, total_bytes


# --------------------------------------------------------------------------
# Convert
# --------------------------------------------------------------------------
def _csv_from_bytes(raw: bytes) -> pd.DataFrame:
    """Read CSV bytes with the pipeline's explicit DTYPES."""
    import io
    return pd.read_csv(io.BytesIO(raw), dtype=DTYPES, low_memory=False)


def _maybe_gunzip(raw: bytes, name: str) -> bytes:
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def _is_csv_member(name: str) -> bool:
    return name.endswith((".csv", ".csv.gz"))


def read_states_csv(path):
    """Read a state-vector CSV from any of the common wrappers:

        *.csv              plain CSV
        *.csv.gz           gzipped CSV
        *.csv.bz2          bzip2'd CSV   (pandas handles natively)
        *.csv.tar          tar containing one or more .csv or .csv.gz members
        *.csv.zip          zip containing one or more .csv or .csv.gz members
        *.tar / *.tar.gz   tar / tar+gz archive — any CSV members are read

    Multiple CSV members are concatenated. Non-CSV members (LICENSE.txt etc.)
    are ignored.
    """
    import io
    import zipfile
    name = path.name.lower()
    frames = []

    # 1. Tarballs (plain or gzipped at the outer level)
    if name.endswith((".tar", ".csv.tar", ".tar.gz", ".tgz")):
        mode = "r:gz" if name.endswith((".tar.gz", ".tgz")) else "r"
        with tarfile.open(path, mode) as tf:
            members = [m for m in tf.getmembers() if _is_csv_member(m.name)]
            if not members:
                raise ValueError(f"no CSV member inside {path}")
            for m in members:
                with tf.extractfile(m) as f:
                    frames.append(_csv_from_bytes(_maybe_gunzip(f.read(), m.name)))

    # 2. ZIP archives
    elif name.endswith((".zip", ".csv.zip")):
        with zipfile.ZipFile(path) as zf:
            inner = [n for n in zf.namelist() if _is_csv_member(n)]
            if not inner:
                raise ValueError(f"no CSV member inside {path}")
            for n in inner:
                with zf.open(n) as f:
                    frames.append(_csv_from_bytes(_maybe_gunzip(f.read(), n)))

    # 3. Single-file formats pandas understands directly
    else:
        if name.endswith(".csv.gz") or name.endswith(".gz"):
            compression = "gzip"
        elif name.endswith(".csv.bz2") or name.endswith(".bz2"):
            compression = "bz2"
        elif name.endswith(".csv"):
            compression = None
        else:
            raise ValueError(f"unsupported file extension: {path}")
        frames.append(pd.read_csv(path, dtype=DTYPES,
                                  compression=compression, low_memory=False))

    df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]

    missing = set(DTYPES) - set(df.columns)
    if missing:
        print(f"  WARNING: columns not found: {sorted(missing)}")
    for c in BOOL_COLS:
        if c in df.columns:
            df[c] = df[c].isin(TRUE_SET)
    if "time" in df.columns:
        df["time"] = df["time"].astype("int64")
    return df


def convert(date, hours, out_root):
    day_dir = RAW_DIR / date
    if not day_dir.exists():
        sys.exit(f"nothing downloaded at {day_dir}")
    rows_total = bytes_gz = bytes_pq = 0
    icaos, done = set(), 0

    # Any file whose extension is supported by read_states_csv.
    SUPPORTED = ("*.csv", "*.csv.gz", "*.csv.bz2",
                 "*.csv.tar", "*.csv.zip",
                 "*.tar", "*.tar.gz", "*.tgz", "*.zip")

    for h in hours:
        candidates = []
        for pattern in SUPPORTED:
            candidates += [p for p in day_dir.glob(pattern) if match_hour(p.name, h)]
        srcs = sorted(set(candidates))
        if not srcs:
            print(f"skip hour {h}: no file")
            continue
        out = Path(out_root) / f"date={date}" / f"hour={h}" / "part-0.parquet"
        if out.exists():
            print(f"skip hour {h}: parquet exists")
            continue

        df = pd.concat([read_states_csv(p) for p in srcs], ignore_index=True)
        # hour-of-day is encoded in the partition path `hour={h}/`, not stored
        # in the parquet (prevents pyarrow Hive-discovery schema conflicts with
        # the partition dictionary type).
        if "hour" in df.columns:
            df = df.drop(columns=["hour"])
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out, index=False, compression="snappy")

        gz = sum(p.stat().st_size for p in srcs)
        pq = out.stat().st_size
        raw = 0
        for p in srcs:
            with gzip.open(p, "rb") as f:
                raw += sum(len(c) for c in iter(lambda: f.read(1 << 22), b""))
        icaos.update(df["icao24"].dropna().unique())
        rows_total += len(df)
        bytes_gz += gz
        bytes_pq += pq
        done += 1
        print(f"hour {h:2d}: rows={len(df):>11,}  icao24={df['icao24'].nunique():>6,}  "
              f"gz={gz / 1e6:7.1f} MB  csv={raw / 1e6:8.1f} MB  pq={pq / 1e6:7.1f} MB")

    if done:
        print("\n--- totals for the report ---")
        print(f"hours converted  {done}")
        print(f"rows             {rows_total:,}")
        print(f"unique icao24    {len(icaos):,}")
        print(f"gzip             {bytes_gz / 1e9:.3f} GB")
        print(f"parquet          {bytes_pq / 1e9:.3f} GB")
        print(f"projected 24 h   {bytes_pq / 1e9 / done * 24:.2f} GB parquet, "
              f"{rows_total / done * 24:,.0f} rows")


# --------------------------------------------------------------------------
def date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end inclusive."""
    d = datetime.date.fromisoformat(start)
    e = datetime.date.fromisoformat(end)
    while d <= e:
        yield str(d)
        d += datetime.timedelta(days=1)


def parse_hours(spec):
    if spec in ("all", "*"):
        return list(range(24))
    out = []
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-")
            out += list(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


def main():
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--list", action="store_true")
    mode.add_argument("--download", action="store_true")
    mode.add_argument("--convert", action="store_true")

    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    p.add_argument("--repo", default="s3", choices=["s3", "zenodo"],
                   help="data source: s3 (default) or zenodo")
    p.add_argument("--bucket-url", help="skip discovery and use this S3 endpoint")
    p.add_argument("--prefix", default="",
                   help="S3 path prefix (s3) or filename substring (zenodo)")
    p.add_argument("--date", help="YYYY-MM-DD, for single-day --convert or --download")
    p.add_argument("--start-date", default=OPENSKY_FIRST_DATE,
                   help=f"first date in bulk range (default: {OPENSKY_FIRST_DATE})")
    p.add_argument("--end-date", default=yesterday,
                   help=f"last date in bulk range (default: yesterday, currently {yesterday})")
    p.add_argument("--hours", default="all", help="'0', '0-23', '0,6,12' or 'all'")
    p.add_argument("--max-mb", type=float, default=2000,
                   help="refuse a single file over this size. 0 = no limit.")
    p.add_argument("--dry-run", action="store_true",
                   help="print what would be downloaded without fetching anything")
    p.add_argument("--out", default=str(ROOT / CFG["paths"]["bronze_raw_states"]))
    # Zenodo-only knobs
    p.add_argument("--zenodo-concept", type=int,
                   default=ZENODO_OPENSKY_FLIGHTLIST_CONCEPT,
                   help=f"Zenodo concept ID (default: {ZENODO_OPENSKY_FLIGHTLIST_CONCEPT}, "
                        f"OpenSky flightlists)")
    zsel = p.add_mutually_exclusive_group()
    zsel.add_argument("--latest", action="store_true",
                      help="Zenodo: download only the newest-dated flightlist file")
    zsel.add_argument("--bulk-pull", action="store_true",
                      help="Zenodo: download EVERY file matching --prefix "
                           "(first-time hydration; the default behaviour but opt-in)")
    p.add_argument("--workers", type=int, default=4,
                   help="Zenodo: parallel download workers (default: 4)")
    a = p.parse_args()

    hours = parse_hours(a.hours)

    # ---- convert ----
    if a.convert:
        if a.repo == "zenodo":
            sys.exit("convert is for OpenSky S3 state vectors; "
                     "Zenodo flightlists use a different schema.")
        if a.date:
            convert(a.date, hours, a.out)
        else:
            dates = list(date_range(a.start_date, a.end_date))
            print(f"Converting {len(dates)} day(s): {a.start_date} to {a.end_date}")
            for d in dates:
                convert(d, hours, a.out)
        return

    s = session()

    # ---- Zenodo list / download ----
    if a.repo == "zenodo":
        rec, files = zenodo_list(s, a.zenodo_concept, prefix=a.prefix)
        version = rec.get("metadata", {}).get("version", "?")
        pub     = rec.get("metadata", {}).get("publication_date", "?")
        title   = rec.get("metadata", {}).get("title", "?")
        doi     = rec.get("doi", "?")
        print(f"zenodo concept: {a.zenodo_concept}  record: {rec.get('id')}")
        print(f"version: {version}  published: {pub}")
        print(f"title:   {title}")
        print(f"doi:     {doi}\n")

        if a.list:
            print(f"files ({len(files)}) matching prefix {a.prefix!r}:")
            for f in files:
                print(f"   {f['size'] / 1e6:9.1f} MB  {f['key']}")
            if not files:
                print("  nothing matched. Try without --prefix, or widen it.")
            return

        # download
        if a.latest:
            picked = zenodo_pick_latest_dated(files)
            if not picked:
                sys.exit("no files to pick from")
            files = [picked]
            print(f"--latest resolved to: {picked['key']}")
        elif a.bulk_pull:
            print(f"--bulk-pull: {len(files)} file(s) matching prefix {a.prefix!r}")

        if not files:
            sys.exit("no files matched. Widen --prefix, or drop --latest/--bulk-pull.")

        # "version" from Zenodo may or may not already start with "v". Normalise.
        ver_str = version if str(version).startswith("v") else f"v{version}"
        out_dir = ZENODO_RAW_DIR / ver_str
        total_mb = sum(f["size"] for f in files) / 1e6
        print(f"\n{len(files)} file(s), {total_mb:.1f} MB -> {out_dir}")
        if a.dry_run:
            print("DRY-RUN: listing only, no files will be fetched\n")
        nf, nb = zenodo_download(s, files, out_dir, a.max_mb,
                                 dry_run=a.dry_run, workers=a.workers)
        label = "DRY-RUN total" if a.dry_run else "Done"
        print(f"\n{label}: {nf} file(s) fetched, {nb / 1e6:.1f} MB")
        return

    bucket = a.bucket_url.rstrip("/") if a.bucket_url else discover_bucket(s)
    print(f"bucket: {bucket}\n")

    # ---- list ----
    if a.list:
        folders, files = s3_list(s, bucket, a.prefix)
        print(f"prefix: {a.prefix!r}")
        print(f"\nfolders ({len(folders)}):")
        for f in folders:
            print("  ", f)
        print(f"\nfiles ({len(files)}):")
        for k, sz in files:
            print(f"   {sz / 1e6:9.1f} MB  {k}")
        if not folders and not files:
            print("  nothing here. Check the prefix.")
        print("\nNext: --list a folder above, or --download with that prefix.")
        return

    # ---- download ----
    if a.date:
        # Single specific date
        nf, nb = download_date(s, bucket, a.date, hours, a.max_mb, dry_run=a.dry_run)
        if not a.dry_run:
            print(f"\nsaved to {RAW_DIR / a.date}")
    elif a.prefix:
        # Legacy raw-prefix mode (backward compatible)
        _, files = s3_list(s, bucket, a.prefix)
        date = (re.search(r"(\d{4}-\d{2}-\d{2})", a.prefix) or [None, "unknown"])[1]
        want = [(k, sz) for k, sz in files if any(match_hour(k, h) for h in hours)]
        if not want:
            print(f"no files matched hours {hours}. Files present:")
            for k, sz in files:
                print(f"   {sz / 1e6:9.1f} MB  {k}")
            sys.exit("fix --hours, or use --date / --start-date instead.")
        print(f"{len(want)} file(s), {sum(sz for _, sz in want) / 1e6:.1f} MB\n")
        for k, sz in want:
            if a.dry_run:
                print(f"DRY-RUN  {sz / 1e6:7.1f} MB  {key_url(bucket, k)}")
            else:
                dest = RAW_DIR / date / k.rsplit("/", 1)[-1]
                print(f"GET {key_url(bucket, k)}")
                download(s, key_url(bucket, k), dest, a.max_mb)
        if not a.dry_run:
            print(f"\nsaved to {RAW_DIR / date}")
    else:
        # Bulk mode: discover all available dates then filter by requested range
        print("Discovering available dates from bucket...")
        all_dates = list_available_dates(s, bucket)
        dates = [d for d in all_dates if a.start_date <= d <= a.end_date]
        if not dates:
            sys.exit(f"No available dates between {a.start_date} and {a.end_date}. "
                     f"Bucket has {len(all_dates)} date(s): {all_dates[0]} to {all_dates[-1]}")
        print(f"Bulk download: {len(dates)} available day(s)  "
              f"{dates[0]} to {dates[-1]}  hours={a.hours}")
        if a.dry_run:
            print("DRY-RUN: listing only, no files will be fetched\n")
        total_files = total_bytes = 0
        for d in dates:
            nf, nb = download_date(s, bucket, d, hours, a.max_mb, dry_run=a.dry_run)
            total_files += nf
            total_bytes += nb
        label = "DRY-RUN total" if a.dry_run else "Done"
        print(f"\n{label}: {total_files:,} files  {total_bytes / 1e9:.2f} GB")


if __name__ == "__main__":
    main()