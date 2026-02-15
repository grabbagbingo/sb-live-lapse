#!/usr/bin/env python3
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


STATIONS = ["KC6OYN", "SE068", "SE234", "MTIC1", "MPWC1", "421SE", "SE053", "KSBA"]
STATION_NAMES = {
    "KC6OYN": "La Cumbre",
    "SE068": "VOR",
    "SE234": "AntFarm",
    "MTIC1": "Montecito",
    "MPWC1": "SM Pass",
    "421SE": "Parma",
    "SE053": "Romero Cyn",
    "KSBA": "Airport",
}

RASS_BASE = "https://downloads.psl.noaa.gov/psd2/data/realtime/Radar449/WwTemp/sba/"
MADIS_BASE = "https://madis-data.ncep.noaa.gov/madisPublic/cgi-bin/madisXmlPublicDir"
CWOP_XML_BASE = "http://www.findu.com/cgi-bin/wxxml.cgi"

# Repo-relative outputs so GitHub Actions can run this anywhere.
CHART_PATH = Path("sba_wwtemp_chart.svg")
CHART_METRIC_PATH = Path("sba_wwtemp_chart_metric.svg")
CHART_IMPERIAL_PATH = Path("sba_wwtemp_chart_imperial.svg")
CSV_PATH = Path("madis_recent60_stations.csv")
RASS_TEXT_PATH = Path("sba_latest.01t")
STATE_PATH = Path("station_state.json")
HISTORY_PATH = Path("station_history.json")
SNAPSHOT_DIR = Path("snapshots")

MS_TO_MPH = 2.23694
FT_PER_M = 3.28084
KTS_PER_MPS = 1.94384
PST = timezone(timedelta(hours=-8), name="PST")
HTTP_USER_AGENT = "Mozilla/5.0 (compatible; sb-live-lapse/1.0)"
CWOP_ELEV_M = {
    "KC6OYN": 1201.0,
}
LAST_GOOD_GRACE_MIN = 90.0
DEFAULT_DEPLOYED_STATE_URL = "https://jimmysaurusrex.github.io/sb-live-lapse/station_state.json"
DEFAULT_DEPLOYED_HISTORY_URL = "https://jimmysaurusrex.github.io/sb-live-lapse/station_history.json"
HISTORY_RETENTION_HOURS = 48
RASS_LIST_RETRIES = 3
RASS_FILE_RETRIES = 2
RASS_CANDIDATE_COUNT = 5
FETCH_RETRY_DELAY_SEC = 1.5


def fetch_text(url: str, timeout: int = 25) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def fetch_text_with_retry(url: str, timeout: int, retries: int, delay_sec: float) -> str:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fetch_text(url, timeout=timeout)
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(delay_sec)
    if last_exc is None:
        raise RuntimeError("Fetch failed without exception: %s" % url)
    raise last_exc


def latest_rass_candidates(max_files: int = RASS_CANDIDATE_COUNT) -> Tuple[str, str, List[str]]:
    root_html = fetch_text_with_retry(RASS_BASE, timeout=25, retries=RASS_LIST_RETRIES, delay_sec=FETCH_RETRY_DELAY_SEC)
    years = [int(v) for v in re.findall(r'href="(20\d{2})/"', root_html)]
    if not years:
        raise RuntimeError("No RASS year directories found")

    year = max(years)
    year_url = f"{RASS_BASE}{year}/"
    year_html = fetch_text_with_retry(year_url, timeout=25, retries=RASS_LIST_RETRIES, delay_sec=FETCH_RETRY_DELAY_SEC)
    doys = [int(v) for v in re.findall(r'href="(\d{3})/"', year_html)]
    if not doys:
        raise RuntimeError("No RASS day directories found")

    doy = max(doys)
    day_url = f"{year_url}{doy:03d}/"
    day_html = fetch_text_with_retry(day_url, timeout=25, retries=RASS_LIST_RETRIES, delay_sec=FETCH_RETRY_DELAY_SEC)
    files = re.findall(r'href="(sba\d{5}\.\d{2}t)"', day_html)
    if not files:
        raise RuntimeError("No RASS files found in latest day")

    files_desc = sorted(set(files), reverse=True)
    return str(year), f"{doy:03d}", files_desc[:max_files]


def parse_rass(raw: str) -> Tuple[Optional[str], List[Tuple[int, float]]]:
    lines = raw.splitlines()

    obs_time_utc = None
    for line in lines[:12]:
        parts = line.split()
        if len(parts) >= 6 and all(p.replace(".", "", 1).isdigit() for p in parts[:6]):
            yy, mm, dd, hh, mi, ss = parts[:6]
            if len(yy) <= 2:
                obs_time_utc = f"20{yy}-{mm.zfill(2)}-{dd.zfill(2)}T{hh.zfill(2)}:{mi.zfill(2)}:{ss.zfill(2)}"
                break

    start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("HT"):
            start_idx = i + 1
            break
    if start_idx is None:
        raise RuntimeError("RASS table header not found")

    points: List[Tuple[float, float]] = []
    for line in lines[start_idx:]:
        if not line.strip() or line.strip().startswith("$"):
            break
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            alt_m = float(parts[0]) * 1000.0
            temp_c = float(parts[1])
        except ValueError:
            continue
        if temp_c >= 999999:
            continue
        points.append((alt_m, temp_c))

    if len(points) < 2:
        raise RuntimeError("Not enough valid RASS points")

    points.sort(key=lambda x: x[0])
    min_alt = int(math.ceil(points[0][0] / 100.0) * 100)
    max_alt = int(math.floor(points[-1][0] / 100.0) * 100)

    alt_grid = list(range(min_alt, max_alt + 1, 100))
    out: List[Tuple[int, float]] = []

    j = 0
    for alt in alt_grid:
        while j < len(points) - 2 and points[j + 1][0] < alt:
            j += 1
        a0, t0 = points[j]
        a1, t1 = points[j + 1]
        if a1 == a0:
            t = t0
        else:
            t = t0 + (t1 - t0) * (alt - a0) / (a1 - a0)
        out.append((alt, t))

    return obs_time_utc, out


def load_rass_with_fallback() -> Tuple[str, Optional[str], List[Tuple[int, float]], str]:
    errors: List[str] = []
    try:
        year, doy, candidates = latest_rass_candidates()
        for filename in candidates:
            rass_url = f"{RASS_BASE}{year}/{doy}/{filename}"
            try:
                raw_rass = fetch_text_with_retry(rass_url, timeout=25, retries=RASS_FILE_RETRIES, delay_sec=FETCH_RETRY_DELAY_SEC)
                rass_time_utc, rass_points = parse_rass(raw_rass)
                RASS_TEXT_PATH.write_text(raw_rass)
                return filename, rass_time_utc, rass_points, "live"
            except Exception as exc:
                errors.append(f"{filename}: {exc}")
    except Exception as exc:
        errors.append(f"directory-listing: {exc}")

    if RASS_TEXT_PATH.exists():
        try:
            raw_cached = RASS_TEXT_PATH.read_text()
            rass_time_utc, rass_points = parse_rass(raw_cached)
            return RASS_TEXT_PATH.name, rass_time_utc, rass_points, "cached"
        except Exception as exc:
            errors.append(f"{RASS_TEXT_PATH.name}: {exc}")

    detail = " | ".join(errors[-4:]) if errors else "unknown"
    raise RuntimeError("Unable to load RASS data (%s)" % detail)


def fetch_station(station_id: str) -> Dict:
    params = {
        "time": "0",
        "minbck": "-59",
        "minfwd": "0",
        "recwin": "3",
        "timefilter": "0",
        "dfltrsel": "3",
        "stasel": "1",
        "stanam": station_id,
        "pvdrsel": "0",
        "varsel": "2",
        "qctype": "0",
        "qcsel": "1",
        "xml": "1",
        "csvmiss": "0",
    }
    url = MADIS_BASE + "?" + urllib.parse.urlencode(params)

    out = {
        "id": station_id,
        "name": STATION_NAMES.get(station_id, station_id),
        "elev_m": None,
        "temp_c": None,
        "dew_c": None,
        "temp_ob_time": None,
        "provider": None,
        "wind_dir": None,
        "wind_spd_mps": None,
        "wind_gust_mps": None,
        "wind_ob_time": None,
    }

    try:
        raw = fetch_text(url, timeout=22)
        root = ET.fromstring(raw)
    except Exception:
        return out

    latest: Dict[str, Tuple[str, float, str]] = {}
    any_elev = None

    for rec in root.findall("record"):
        var = rec.attrib.get("var")
        if var not in ("V-T", "V-TD", "V-DD", "V-FF", "V-FFGUST"):
            continue

        ob_time = rec.attrib.get("ObTime")
        val_s = rec.attrib.get("data_value")
        provider = rec.attrib.get("provider", "")
        elev_s = rec.attrib.get("elev")

        if any_elev is None and elev_s:
            try:
                any_elev = float(elev_s)
            except ValueError:
                pass

        if not ob_time or not val_s:
            continue

        try:
            val = float(val_s)
        except ValueError:
            continue

        prev = latest.get(var)
        if prev is None or ob_time > prev[0]:
            latest[var] = (ob_time, val, provider)

    out["elev_m"] = any_elev

    if "V-T" in latest:
        t_ob, t_k, provider = latest["V-T"]
        out["temp_c"] = t_k - 273.15
        out["temp_ob_time"] = t_ob
        out["provider"] = provider
    if "V-TD" in latest:
        _, td_k, _ = latest["V-TD"]
        out["dew_c"] = td_k - 273.15
    if "V-DD" in latest:
        d_ob, d_val, _ = latest["V-DD"]
        out["wind_dir"] = d_val
        out["wind_ob_time"] = d_ob
    if "V-FF" in latest:
        f_ob, f_val, _ = latest["V-FF"]
        out["wind_spd_mps"] = f_val
        if out["wind_ob_time"] is None or f_ob > out["wind_ob_time"]:
            out["wind_ob_time"] = f_ob
    if "V-FFGUST" in latest:
        g_ob, g_val, _ = latest["V-FFGUST"]
        out["wind_gust_mps"] = g_val
        if out["wind_ob_time"] is None or g_ob > out["wind_ob_time"]:
            out["wind_ob_time"] = g_ob

    return out


def parse_float(raw: Optional[str]) -> Optional[float]:
    if raw is None:
        return None
    text = raw.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_iso_utc(iso_time: Optional[str]) -> Optional[datetime]:
    if not iso_time:
        return None
    normalized = iso_time.strip()
    if not normalized:
        return None
    try:
        dt = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def dewpoint_c_from_temp_rh(temp_c: float, rh_pct: float) -> Optional[float]:
    if rh_pct <= 0.0 or rh_pct > 100.0:
        return None
    a = 17.625
    b = 243.04
    gamma = math.log(rh_pct / 100.0) + (a * temp_c) / (b + temp_c)
    return (b * gamma) / (a - gamma)


def blank_station_row(station_id: str) -> Dict:
    return {
        "id": station_id,
        "name": STATION_NAMES.get(station_id, station_id),
        "elev_m": None,
        "temp_c": None,
        "dew_c": None,
        "temp_ob_time": None,
        "provider": None,
        "wind_dir": None,
        "wind_spd_mps": None,
        "wind_gust_mps": None,
        "wind_ob_time": None,
    }


def fetch_station_cwop(station_id: str) -> Dict:
    out = blank_station_row(station_id)
    out["elev_m"] = CWOP_ELEV_M.get(station_id)

    url = CWOP_XML_BASE + "?" + urllib.parse.urlencode({"call": station_id, "last": "2"})
    try:
        raw = fetch_text(url, timeout=18)
        if "<station" not in raw:
            return out
        root = ET.fromstring(raw)
    except Exception:
        return out

    latest_dt: Optional[datetime] = None
    latest_rep: Optional[ET.Element] = None
    for rep in root.findall("weatherReport"):
        ts = (rep.findtext("timeReceived") or "").strip()
        if not ts:
            continue
        try:
            dt_utc = datetime.strptime(ts, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if latest_dt is None or dt_utc > latest_dt:
            latest_dt = dt_utc
            latest_rep = rep

    if latest_dt is None or latest_rep is None:
        return out

    temp_f = parse_float(latest_rep.findtext("temperature"))
    rh_pct = parse_float(latest_rep.findtext("humidity"))
    wind_dir = parse_float(latest_rep.findtext("windDirection"))
    wind_spd_mph = parse_float(latest_rep.findtext("windSpeed"))
    wind_gust_mph = parse_float(latest_rep.findtext("windGust"))

    obs_iso = latest_dt.strftime("%Y-%m-%dT%H:%M")
    if temp_f is not None:
        out["temp_c"] = (temp_f - 32.0) * (5.0 / 9.0)
        out["temp_ob_time"] = obs_iso
    if out["temp_c"] is not None and rh_pct is not None:
        out["dew_c"] = dewpoint_c_from_temp_rh(out["temp_c"], rh_pct)

    if wind_dir is not None:
        out["wind_dir"] = wind_dir
    if wind_spd_mph is not None:
        out["wind_spd_mps"] = wind_spd_mph / MS_TO_MPH
        out["wind_ob_time"] = obs_iso
    if wind_gust_mph is not None:
        out["wind_gust_mps"] = wind_gust_mph / MS_TO_MPH
        out["wind_ob_time"] = obs_iso
    if out["temp_ob_time"] is not None:
        out["provider"] = "CWOP-findU"

    return out


def update_age_and_recency(row: Dict, now_utc: datetime) -> None:
    row["recent"] = False
    row["age_min"] = None
    dt_utc = parse_iso_utc(row.get("temp_ob_time"))
    if dt_utc is not None:
        row["age_min"] = (now_utc - dt_utc).total_seconds() / 60.0
        row["recent"] = row["age_min"] <= 60.0


def should_try_cwop(row: Dict) -> bool:
    return row.get("temp_c") is None or not row.get("recent")


def merge_cwop_if_needed(madis_row: Dict, cwop_row: Dict) -> Dict:
    if cwop_row.get("temp_c") is None:
        return madis_row

    if madis_row.get("temp_c") is not None and madis_row.get("recent"):
        return madis_row

    merged = dict(madis_row)
    for key in ("temp_c", "dew_c", "temp_ob_time", "wind_dir", "wind_spd_mps", "wind_gust_mps", "wind_ob_time"):
        if cwop_row.get(key) is not None:
            merged[key] = cwop_row[key]
    if merged.get("elev_m") is None and cwop_row.get("elev_m") is not None:
        merged["elev_m"] = cwop_row["elev_m"]
    if cwop_row.get("temp_ob_time") is not None:
        merged["provider"] = cwop_row.get("provider") or "CWOP-findU"
    return merged


def state_url_from_env() -> Optional[str]:
    direct = os.getenv("SB_DEPLOYED_STATE_URL")
    if direct:
        return direct
    repo = os.getenv("GITHUB_REPOSITORY", "")
    if "/" in repo:
        owner, name = repo.split("/", 1)
        return f"https://{owner}.github.io/{name}/station_state.json"
    return DEFAULT_DEPLOYED_STATE_URL


def history_url_from_env() -> Optional[str]:
    direct = os.getenv("SB_DEPLOYED_HISTORY_URL")
    if direct:
        return direct
    repo = os.getenv("GITHUB_REPOSITORY", "")
    if "/" in repo:
        owner, name = repo.split("/", 1)
        return f"https://{owner}.github.io/{name}/station_history.json"
    return DEFAULT_DEPLOYED_HISTORY_URL


def normalize_state_row(station_id: str, raw: Dict) -> Dict:
    row = blank_station_row(station_id)
    row["name"] = str(raw.get("name") or row["name"])
    row["provider"] = str(raw["provider"]) if raw.get("provider") else None

    for key in ("elev_m", "temp_c", "dew_c", "wind_dir", "wind_spd_mps", "wind_gust_mps"):
        row[key] = parse_float(str(raw[key])) if raw.get(key) is not None else None

    for key in ("temp_ob_time", "wind_ob_time"):
        value = raw.get(key)
        if value:
            value = str(value).strip()
            row[key] = value if parse_iso_utc(value) is not None else None

    return row


def parse_state_payload(text: str) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    try:
        payload = json.loads(text)
    except Exception:
        return out

    stations_payload = payload.get("stations")
    if isinstance(stations_payload, dict):
        iterable = stations_payload.items()
    elif isinstance(stations_payload, list):
        iterable = ((item.get("id"), item) for item in stations_payload if isinstance(item, dict))
    else:
        return out

    for station_id, raw in iterable:
        if station_id not in STATIONS or not isinstance(raw, dict):
            continue
        out[station_id] = normalize_state_row(station_id, raw)
    return out


def parse_history_payload(text: str) -> List[Dict]:
    try:
        payload = json.loads(text)
    except Exception:
        return []

    snapshots = payload if isinstance(payload, list) else payload.get("snapshots")
    if not isinstance(snapshots, list):
        return []

    out: List[Dict] = []
    for snapshot in snapshots:
        if not isinstance(snapshot, dict):
            continue
        run_at_raw = snapshot.get("run_at") or snapshot.get("generated_at")
        if not isinstance(run_at_raw, str):
            continue
        run_dt = parse_iso_utc(run_at_raw)
        if run_dt is None:
            continue
        normalized = dict(snapshot)
        normalized["run_at"] = run_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        out.append(normalized)
    return out


def load_last_good_state() -> Dict[str, Dict]:
    url = state_url_from_env()
    if not url:
        url = DEFAULT_DEPLOYED_STATE_URL
    try:
        parsed = parse_state_payload(fetch_text(url, timeout=15))
        if parsed:
            return parsed
    except Exception:
        pass

    if STATE_PATH.exists():
        parsed = parse_state_payload(STATE_PATH.read_text())
        if parsed:
            return parsed
    return {}


def load_station_history() -> List[Dict]:
    url = history_url_from_env()
    if not url:
        url = DEFAULT_DEPLOYED_HISTORY_URL
    try:
        parsed = parse_history_payload(fetch_text(url, timeout=15))
        if parsed:
            return parsed
    except Exception:
        pass

    if HISTORY_PATH.exists():
        parsed = parse_history_payload(HISTORY_PATH.read_text())
        if parsed:
            return parsed
    return []


def prune_history_snapshots(snapshots: List[Dict], now_utc: datetime) -> List[Dict]:
    cutoff = now_utc - timedelta(hours=HISTORY_RETENTION_HOURS)
    deduped: Dict[str, Dict] = {}

    for snapshot in snapshots:
        run_at_raw = snapshot.get("run_at")
        if not isinstance(run_at_raw, str):
            continue
        run_dt = parse_iso_utc(run_at_raw)
        if run_dt is None or run_dt < cutoff:
            continue
        run_at = run_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        normalized = dict(snapshot)
        normalized["run_at"] = run_at
        deduped[run_at] = normalized

    return [deduped[key] for key in sorted(deduped.keys())]


def history_chart_paths(snapshots: List[Dict]) -> List[str]:
    paths: List[str] = []
    for snapshot in snapshots:
        charts = snapshot.get("charts")
        if not isinstance(charts, dict):
            continue
        for key in ("metric_svg", "imperial_svg"):
            path = charts.get(key)
            if isinstance(path, str) and path.startswith("snapshots/"):
                paths.append(path)
    return paths


def cleanup_snapshot_files(keep_rel_paths: List[str]) -> None:
    if not SNAPSHOT_DIR.exists():
        return
    keep = set(keep_rel_paths)
    for file_path in SNAPSHOT_DIR.glob("*.svg"):
        rel = file_path.as_posix()
        if rel not in keep:
            try:
                file_path.unlink()
            except OSError:
                pass


def snapshot_to_station_rows(snapshot: Dict) -> List[Dict]:
    stations_map = snapshot.get("stations")
    if not isinstance(stations_map, dict):
        return []

    out: List[Dict] = []
    for station_id in STATIONS:
        raw = stations_map.get(station_id)
        if isinstance(raw, dict):
            out.append(normalize_state_row(station_id, raw))
        else:
            out.append(blank_station_row(station_id))
    return out


def snapshot_to_rass_points(snapshot: Dict) -> List[Tuple[int, float]]:
    rass = snapshot.get("rass")
    if not isinstance(rass, dict):
        return []
    raw_points = rass.get("points_100m_c")
    if not isinstance(raw_points, list):
        return []

    points: List[Tuple[int, float]] = []
    for item in raw_points:
        if not isinstance(item, list) or len(item) < 2:
            continue
        alt_raw, temp_raw = item[0], item[1]
        try:
            alt = int(round(float(alt_raw)))
            temp = float(temp_raw)
        except (TypeError, ValueError):
            continue
        points.append((alt, temp))
    points.sort(key=lambda x: x[0])
    return points


def build_snapshot_svgs(snapshot: Dict) -> Optional[Tuple[str, str]]:
    run_at = parse_iso_utc(snapshot.get("run_at"))
    if run_at is None:
        return None

    rass_points = snapshot_to_rass_points(snapshot)
    if len(rass_points) < 2:
        return None

    stations = snapshot_to_station_rows(snapshot)
    if not stations:
        return None

    for row in stations:
        update_age_and_recency(row, run_at)

    vor = next((r for r in stations if r["id"] == "SE068"), None)
    title_metric = build_lcl_title(vor, altitude_unit="m")
    title_imperial = build_lcl_title(vor, altitude_unit="ft")

    rass_obj = snapshot.get("rass") if isinstance(snapshot.get("rass"), dict) else {}
    rass_hhmm = utc_iso_to_pst_hhmm(rass_obj.get("ob_time_utc")) or "missing"

    metric_rass = convert_rass_points_units(rass_points, unit_system="metric")
    metric_all = convert_station_rows_units(stations, unit_system="metric")
    metric_recent = [r for r in metric_all if r.get("recent") and r.get("temp_c") is not None and r.get("elev_m") is not None]

    imperial_rass = convert_rass_points_units(rass_points, unit_system="imperial")
    imperial_all = convert_station_rows_units(stations, unit_system="imperial")
    imperial_recent = [r for r in imperial_all if r.get("recent") and r.get("temp_c") is not None and r.get("elev_m") is not None]

    metric_svg = draw_svg(
        metric_rass,
        metric_recent,
        metric_all,
        title_metric,
        rass_hhmm,
        temp_unit="C",
        altitude_unit="m",
        temp_suffix="C",
        dalr_label="DALR (9.8 C/km)",
        dalr_rate_per_1000=9.8,
        y_tick_step=200,
    )
    imperial_svg = draw_svg(
        imperial_rass,
        imperial_recent,
        imperial_all,
        title_imperial,
        rass_hhmm,
        temp_unit="F",
        altitude_unit="ft",
        temp_suffix="F",
        dalr_label="DALR (5.4 F/1000 ft)",
        dalr_rate_per_1000=5.4,
        y_tick_step=500,
    )
    return metric_svg, imperial_svg


def age_minutes(iso_time: Optional[str], now_utc: datetime) -> Optional[float]:
    dt = parse_iso_utc(iso_time)
    if dt is None:
        return None
    return (now_utc - dt).total_seconds() / 60.0


def within_grace(iso_time: Optional[str], now_utc: datetime) -> bool:
    age = age_minutes(iso_time, now_utc)
    return age is not None and age <= LAST_GOOD_GRACE_MIN


def apply_last_good_fallback(current_row: Dict, cached_row: Dict, now_utc: datetime) -> Dict:
    merged = dict(current_row)
    used_cache = False

    if merged.get("elev_m") is None and cached_row.get("elev_m") is not None:
        merged["elev_m"] = cached_row["elev_m"]

    temp_cache_ok = within_grace(cached_row.get("temp_ob_time"), now_utc)
    wind_cache_ok = within_grace(cached_row.get("wind_ob_time"), now_utc)

    if merged.get("temp_c") is None and temp_cache_ok and cached_row.get("temp_c") is not None:
        merged["temp_c"] = cached_row["temp_c"]
        merged["temp_ob_time"] = cached_row.get("temp_ob_time")
        used_cache = True
    if merged.get("dew_c") is None and temp_cache_ok and cached_row.get("dew_c") is not None:
        merged["dew_c"] = cached_row["dew_c"]
        used_cache = True
    if merged.get("wind_spd_mps") is None and wind_cache_ok and cached_row.get("wind_spd_mps") is not None:
        merged["wind_spd_mps"] = cached_row["wind_spd_mps"]
        merged["wind_ob_time"] = cached_row.get("wind_ob_time")
        used_cache = True
    if merged.get("wind_gust_mps") is None and wind_cache_ok and cached_row.get("wind_gust_mps") is not None:
        merged["wind_gust_mps"] = cached_row["wind_gust_mps"]
        if merged.get("wind_ob_time") is None:
            merged["wind_ob_time"] = cached_row.get("wind_ob_time")
        used_cache = True
    if merged.get("wind_dir") is None and wind_cache_ok and cached_row.get("wind_dir") is not None:
        merged["wind_dir"] = cached_row["wind_dir"]
        if merged.get("wind_ob_time") is None:
            merged["wind_ob_time"] = cached_row.get("wind_ob_time")
        used_cache = True

    if used_cache:
        base_provider = merged.get("provider") or cached_row.get("provider") or "cache"
        if "(last-good)" not in str(base_provider):
            merged["provider"] = f"{base_provider} (last-good)"
    return merged


def station_payload(row: Dict) -> Dict:
    return {
        "id": row["id"],
        "name": row.get("name"),
        "elev_m": row.get("elev_m"),
        "temp_c": row.get("temp_c"),
        "dew_c": row.get("dew_c"),
        "temp_ob_time": row.get("temp_ob_time"),
        "provider": row.get("provider"),
        "wind_dir": row.get("wind_dir"),
        "wind_spd_mps": row.get("wind_spd_mps"),
        "wind_gust_mps": row.get("wind_gust_mps"),
        "wind_ob_time": row.get("wind_ob_time"),
    }


def write_station_state(stations: List[Dict], now_utc: datetime) -> None:
    state = {
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stations": {},
    }
    for row in stations:
        state["stations"][row["id"]] = station_payload(row)
    STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def write_station_history(
    stations: List[Dict],
    now_utc: datetime,
    rass_filename: str,
    rass_time_utc: Optional[str],
    rass_source: str,
    rass_points: List[Tuple[int, float]],
    metric_svg: str,
    imperial_svg: str,
) -> None:
    def write_snapshot_chart_files(run_at_iso: str, metric_text: str, imperial_text: str) -> Tuple[str, str]:
        run_dt = parse_iso_utc(run_at_iso)
        if run_dt is None:
            raise ValueError("invalid run_at timestamp")
        snapshot_stamp = run_dt.strftime("%Y%m%dT%H%MZ")
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        metric_rel = f"snapshots/{snapshot_stamp}_metric.svg"
        imperial_rel = f"snapshots/{snapshot_stamp}_imperial.svg"
        Path(metric_rel).write_text(metric_text)
        Path(imperial_rel).write_text(imperial_text)
        return metric_rel, imperial_rel

    run_at_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    metric_rel, imperial_rel = write_snapshot_chart_files(run_at_iso, metric_svg, imperial_svg)

    history = load_station_history()
    history.append(
        {
            "run_at": run_at_iso,
            "charts": {
                "metric_svg": metric_rel,
                "imperial_svg": imperial_rel,
            },
            "rass": {
                "file": rass_filename,
                "ob_time_utc": rass_time_utc,
                "points_100m_c": [[int(alt_m), round(temp_c, 3)] for alt_m, temp_c in rass_points],
                "source": rass_source,
            },
            "stations": {row["id"]: station_payload(row) for row in stations},
        }
    )
    history = prune_history_snapshots(history, now_utc)

    for snapshot in history:
        charts = snapshot.get("charts") if isinstance(snapshot.get("charts"), dict) else {}
        metric_existing = charts.get("metric_svg") if isinstance(charts, dict) else None
        imperial_existing = charts.get("imperial_svg") if isinstance(charts, dict) else None
        metric_ok = isinstance(metric_existing, str) and Path(metric_existing).exists()
        imperial_ok = isinstance(imperial_existing, str) and Path(imperial_existing).exists()
        if metric_ok and imperial_ok:
            continue
        rebuilt = build_snapshot_svgs(snapshot)
        if rebuilt is None:
            continue
        metric_rel_built, imperial_rel_built = write_snapshot_chart_files(snapshot["run_at"], rebuilt[0], rebuilt[1])
        snapshot["charts"] = {
            "metric_svg": metric_rel_built,
            "imperial_svg": imperial_rel_built,
        }

    payload = {
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "retention_hours": HISTORY_RETENTION_HOURS,
        "snapshot_count": len(history),
        "snapshots": history,
    }
    HISTORY_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    cleanup_snapshot_files(history_chart_paths(history))


def c_to_f(temp_c: float) -> float:
    return temp_c * 9.0 / 5.0 + 32.0


def m_to_ft(alt_m: float) -> float:
    return alt_m * FT_PER_M


def convert_rass_points_units(points_m_c: List[Tuple[int, float]], unit_system: str) -> List[Tuple[float, float]]:
    if unit_system == "imperial":
        return [(m_to_ft(float(alt_m)), c_to_f(temp_c)) for alt_m, temp_c in points_m_c]
    return [(float(alt_m), temp_c) for alt_m, temp_c in points_m_c]


def convert_station_rows_units(rows: List[Dict], unit_system: str) -> List[Dict]:
    out: List[Dict] = []
    for row in rows:
        converted = dict(row)
        if unit_system == "imperial":
            if converted.get("elev_m") is not None:
                converted["elev_m"] = m_to_ft(converted["elev_m"])
            if converted.get("temp_c") is not None:
                converted["temp_c"] = c_to_f(converted["temp_c"])
            if converted.get("dew_c") is not None:
                converted["dew_c"] = c_to_f(converted["dew_c"])
        out.append(converted)
    return out


def utc_iso_to_pst_hhmm(iso_time: Optional[str]) -> Optional[str]:
    dt_utc = parse_iso_utc(iso_time)
    if dt_utc is None:
        return None
    return dt_utc.astimezone(PST).strftime("%H:%M")


def wind_text_for_row(row: Dict) -> str:
    if row.get("wind_spd_mps") is None:
        return "winds missing"

    direction = "---"
    if row.get("wind_dir") is not None:
        direction = f"{int(round(row['wind_dir'])):03d}"

    speed_mph = int(round(row["wind_spd_mps"] * MS_TO_MPH))
    if row.get("wind_gust_mps") is None:
        return f"winds {direction}, {speed_mph}mph"

    gust_mph = int(round(row["wind_gust_mps"] * MS_TO_MPH))
    return f"winds {direction}, {speed_mph}g{gust_mph}mph"


def wind_barb_svg(x_px: float, y_px: float, wind_dir_deg: Optional[float], wind_spd_mps: Optional[float]) -> List[str]:
    if wind_dir_deg is None or wind_spd_mps is None:
        return []

    # Meteorological barbs use knots and standard 50/10/5 knot increments.
    speed_kt = max(0.0, wind_spd_mps * KTS_PER_MPS)
    barb_speed = int(5 * round(speed_kt / 5.0))

    if barb_speed <= 0:
        return ['<circle class="barb-shaft" cx="%.2f" cy="%.2f" r="2.5" fill="none" />' % (x_px, y_px)]

    rad = math.radians(wind_dir_deg % 360.0)
    ux = math.sin(rad)
    uy = -math.cos(rad)
    nx = -uy
    ny = ux

    shaft_len = 18.0
    tip_x = x_px + shaft_len * ux
    tip_y = y_px + shaft_len * uy

    out = ['<line class="barb-shaft" x1="%.2f" y1="%.2f" x2="%.2f" y2="%.2f" />' % (x_px, y_px, tip_x, tip_y)]

    remaining = barb_speed
    offset = 0.0

    while remaining >= 50:
        p0x = tip_x - ux * offset
        p0y = tip_y - uy * offset
        p1x = tip_x - ux * (offset + 4.0)
        p1y = tip_y - uy * (offset + 4.0)
        p2x = p1x + nx * 7.0 - ux * 2.0
        p2y = p1y + ny * 7.0 - uy * 2.0
        out.append(
            '<polygon class="barb-flag" points="%.2f,%.2f %.2f,%.2f %.2f,%.2f" />'
            % (p0x, p0y, p1x, p1y, p2x, p2y)
        )
        offset += 7.0
        remaining -= 50

    while remaining >= 10:
        bx = tip_x - ux * offset
        by = tip_y - uy * offset
        fx = bx + nx * 7.0 - ux * 2.0
        fy = by + ny * 7.0 - uy * 2.0
        out.append('<line class="barb-feather" x1="%.2f" y1="%.2f" x2="%.2f" y2="%.2f" />' % (bx, by, fx, fy))
        offset += 4.0
        remaining -= 10

    if remaining >= 5:
        bx = tip_x - ux * offset
        by = tip_y - uy * offset
        fx = bx + nx * 4.0 - ux * 1.2
        fy = by + ny * 4.0 - uy * 1.2
        out.append('<line class="barb-feather" x1="%.2f" y1="%.2f" x2="%.2f" y2="%.2f" />' % (bx, by, fx, fy))

    return out


def build_lcl_title(vor_row_metric: Optional[Dict], altitude_unit: str) -> str:
    vor_wind = "winds missing"
    if vor_row_metric is not None:
        vor_wind = wind_text_for_row(vor_row_metric)

    if (
        vor_row_metric is None
        or vor_row_metric.get("elev_m") is None
        or vor_row_metric.get("temp_c") is None
        or vor_row_metric.get("dew_c") is None
    ):
        return f"Estimated LCL @ VOR: missing - {vor_wind}"

    cloud_base_m = vor_row_metric["elev_m"] + 125.0 * (vor_row_metric["temp_c"] - vor_row_metric["dew_c"])
    if altitude_unit == "ft":
        cloud_base_value = int(round(m_to_ft(cloud_base_m)))
        cloud_base_label = "ft"
    else:
        cloud_base_value = int(round(cloud_base_m))
        cloud_base_label = "m"

    time_hhmm = utc_iso_to_pst_hhmm(vor_row_metric.get("temp_ob_time"))
    if time_hhmm:
        return f"Estimated LCL @ VOR: {cloud_base_value} {cloud_base_label} - {vor_wind} ({time_hhmm} PST)"
    return f"Estimated LCL @ VOR: {cloud_base_value} {cloud_base_label} - {vor_wind}"


def dalr_divergence_data(stations_all: List[Dict], dalr_rate_per_1000: float) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    usable = [row for row in stations_all if row.get("elev_m") is not None and row.get("temp_c") is not None]

    for row in stations_all:
        row_id = row.get("id")
        if not row_id:
            continue
        if row.get("elev_m") is None or row.get("temp_c") is None:
            out[row_id] = {"kind": "missing", "items": []}
            continue

        lowers: List[Dict] = []
        for candidate in usable:
            if candidate is row:
                continue
            if candidate["elev_m"] < row["elev_m"]:
                lowers.append(candidate)

        if not lowers:
            out[row_id] = {"kind": "na", "items": []}
            continue

        lowers.sort(key=lambda item: item["elev_m"], reverse=True)
        parts: List[Dict] = []
        for lower in lowers:
            predicted_temp = lower["temp_c"] - dalr_rate_per_1000 * (row["elev_m"] - lower["elev_m"]) / 1000.0
            divergence = row["temp_c"] - predicted_temp
            lower_name = lower.get("name") or lower.get("id") or "lower"
            parts.append({"name": lower_name, "divergence": divergence})

        out[row_id] = {"kind": "values", "items": parts}

    return out


def next_lower_station_dalr_metrics(stations: List[Dict], dalr_rate_per_1000: float) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    usable = [row for row in stations if row.get("elev_m") is not None and row.get("temp_c") is not None]

    for row in stations:
        row_id = row.get("id")
        if not row_id or row.get("elev_m") is None or row.get("temp_c") is None:
            if row_id:
                out[row_id] = {"deviation": None, "time_delta_min": None}
            continue

        lower: Optional[Dict] = None
        for candidate in usable:
            if candidate is row:
                continue
            if candidate["elev_m"] >= row["elev_m"]:
                continue
            if lower is None or candidate["elev_m"] > lower["elev_m"]:
                lower = candidate

        if lower is None:
            out[row_id] = {"deviation": None, "time_delta_min": None}
            continue

        predicted_temp = lower["temp_c"] - dalr_rate_per_1000 * (row["elev_m"] - lower["elev_m"]) / 1000.0
        upper_dt = parse_iso_utc(row.get("temp_ob_time"))
        lower_dt = parse_iso_utc(lower.get("temp_ob_time"))
        time_delta_min: Optional[int] = None
        if upper_dt is not None and lower_dt is not None:
            time_delta_min = int(round((lower_dt - upper_dt).total_seconds() / 60.0))
        out[row_id] = {
            "deviation": row["temp_c"] - predicted_temp,
            "time_delta_min": time_delta_min,
        }

    return out


def rass_gate_dalr_deviations(
    rass_points: List[Tuple[float, float]], dalr_rate_per_1000: float
) -> List[Optional[float]]:
    if not rass_points:
        return []
    out: List[Optional[float]] = [None]
    for i in range(1, len(rass_points)):
        lower_alt, lower_temp = rass_points[i - 1]
        alt, temp = rass_points[i]
        predicted = lower_temp - dalr_rate_per_1000 * (alt - lower_alt) / 1000.0
        out.append(temp - predicted)
    return out


def draw_svg(
    rass_points: List[Tuple[float, float]],
    stations_recent: List[Dict],
    stations_all: List[Dict],
    title_text: str,
    rass_time_hhmm_pst: str,
    temp_unit: str,
    altitude_unit: str,
    temp_suffix: str,
    dalr_label: str,
    dalr_rate_per_1000: float,
    y_tick_step: int,
) -> str:
    width, height = 1180, 760
    margin_left, margin_right, margin_top, margin_bottom = 90, 70, 50, 250
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    anchor_alt, anchor_temp = rass_points[0]

    def dalr_temp(alt_value: float) -> float:
        return anchor_temp - dalr_rate_per_1000 * (alt_value - anchor_alt) / 1000.0

    station_alts = [s["elev_m"] for s in stations_recent if s["elev_m"] is not None]
    y_min = int(math.floor(min([0.0, rass_points[0][0]] + station_alts) / 100.0) * 100) if station_alts else 0
    y_max = int(math.ceil(max([rass_points[-1][0]] + station_alts) / 100.0) * 100) if station_alts else rass_points[-1][0]
    if y_max <= y_min:
        y_max = y_min + 100

    dalr_bottom = dalr_temp(y_min)
    dalr_top = dalr_temp(y_max)

    x_vals = [t for _, t in rass_points] + [dalr_bottom, dalr_top]
    x_vals += [s["temp_c"] for s in stations_recent if s["temp_c"] is not None]
    x_min, x_max = min(x_vals) - 0.5, max(x_vals) + 0.5
    if x_max - x_min < 1.0:
        x_min -= 0.5
        x_max += 0.5

    def x_to_px(temp_c: float) -> float:
        return margin_left + (temp_c - x_min) / (x_max - x_min) * plot_w

    def y_to_px(alt_m: float) -> float:
        return margin_top + (y_max - alt_m) / (y_max - y_min) * plot_h

    obs_path = " ".join(
        (("M" if i == 0 else "L") + "%.2f,%.2f" % (x_to_px(temp), y_to_px(alt)))
        for i, (alt, temp) in enumerate(rass_points)
    )
    dalr_path = "M%.2f,%.2f L%.2f,%.2f" % (
        x_to_px(dalr_bottom),
        y_to_px(y_min),
        x_to_px(dalr_top),
        y_to_px(y_max),
    )

    x_span = x_max - x_min
    x_step = 1 if x_span <= 5 else (2 if x_span <= 10 else 5)
    x_ticks = []
    x_cursor = math.ceil(x_min / x_step) * x_step
    while x_cursor <= x_max:
        x_ticks.append(x_cursor)
        x_cursor += x_step

    y_ticks = []
    y_cursor = int(math.ceil(y_min / float(y_tick_step)) * y_tick_step)
    while y_cursor <= y_max:
        y_ticks.append(y_cursor)
        y_cursor += y_tick_step

    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">' % (width, height, width, height),
        "<style>",
        "  .axis { stroke: #202020; stroke-width: 1; }",
        "  .grid { stroke: #dddddd; stroke-width: 1; }",
        "  .title { font-family: Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 600; fill: #111111; }",
        "  .label { font-family: Helvetica, Arial, sans-serif; font-size: 12px; fill: #222222; }",
        "  .legend-h { font-family: Helvetica, Arial, sans-serif; font-size: 12px; font-weight: 600; fill: #222222; }",
        "  .legend-row { font-family: Helvetica, Arial, sans-serif; font-size: 11px; fill: #333333; }",
        "  .rass { fill: none; stroke: #000000; stroke-width: 2; }",
        "  .dalr { fill: none; stroke: #000000; stroke-width: 1.5; stroke-dasharray: 6 4; stroke-opacity: 0.45; }",
        "  .rass-point { fill: #0077b6; }",
        "  .station { fill: #f4a261; stroke: #8b4c12; stroke-width: 1; }",
        "  .barb-shaft { stroke: #1f2937; stroke-width: 1.3; }",
        "  .barb-feather { stroke: #1f2937; stroke-width: 1.2; }",
        "  .barb-flag { fill: #1f2937; stroke: #1f2937; stroke-width: 1; }",
        "  .station-label { font-family: Helvetica, Arial, sans-serif; font-size: 11px; fill: #444444; }",
        "  .station-dev-pos { font-family: Helvetica, Arial, sans-serif; font-size: 10px; fill: #c62828; }",
        "  .station-dev-neg { font-family: Helvetica, Arial, sans-serif; font-size: 10px; fill: #1565c0; }",
        "  .station-dev-na { font-family: Helvetica, Arial, sans-serif; font-size: 10px; fill: #666666; }",
        "  .rass-dev-pos { font-family: Helvetica, Arial, sans-serif; font-size: 9px; fill: #c62828; }",
        "  .rass-dev-neg { font-family: Helvetica, Arial, sans-serif; font-size: 9px; fill: #1565c0; }",
        "  .rass-dev-na { font-family: Helvetica, Arial, sans-serif; font-size: 9px; fill: #666666; }",
        "</style>",
        '<rect x="0" y="0" width="%d" height="%d" fill="#ffffff" />' % (width, height),
        '<text class="title" x="%d" y="%d">%s</text>' % (margin_left, margin_top - 22, title_text),
    ]

    for y in y_ticks:
        y_px = y_to_px(y)
        lines.append('<line class="grid" x1="%d" y1="%.2f" x2="%d" y2="%.2f" />' % (margin_left, y_px, width - margin_right, y_px))
        lines.append('<text class="label" x="%d" y="%.2f" text-anchor="end">%d</text>' % (margin_left - 8, y_px + 4, y))

    for x in x_ticks:
        x_px = x_to_px(x)
        lines.append('<line class="grid" x1="%.2f" y1="%d" x2="%.2f" y2="%d" />' % (x_px, margin_top, x_px, height - margin_bottom))
        lines.append('<text class="label" x="%.2f" y="%d" text-anchor="middle">%.1f</text>' % (x_px, height - margin_bottom + 18, x))

    lines.append('<line class="axis" x1="%d" y1="%d" x2="%d" y2="%d" />' % (margin_left, margin_top, margin_left, height - margin_bottom))
    lines.append('<line class="axis" x1="%d" y1="%d" x2="%d" y2="%d" />' % (margin_left, height - margin_bottom, width - margin_right, height - margin_bottom))
    lines.append(
        '<text class="label" x="%.2f" y="%d" text-anchor="middle">Temperature (%s)</text>'
        % (margin_left + plot_w / 2.0, height - margin_bottom + 36, temp_unit)
    )
    lines.append(
        '<text class="label" x="26" y="%.2f" text-anchor="middle" transform="rotate(-90 26 %.2f)">Altitude (%s)</text>'
        % (margin_top + plot_h / 2.0, margin_top + plot_h / 2.0, altitude_unit)
    )

    lines.append('<path class="rass" d="%s" />' % obs_path)
    lines.append('<path class="dalr" d="%s" />' % dalr_path)

    rass_dev = rass_gate_dalr_deviations(rass_points, dalr_rate_per_1000)
    for i, (alt, temp) in enumerate(rass_points):
        x_px = x_to_px(temp)
        y_px = y_to_px(alt)
        lines.append('<circle class="rass-point" cx="%.2f" cy="%.2f" r="2" />' % (x_px, y_px))

        dev_val = rass_dev[i] if i < len(rass_dev) else None
        if dev_val is None:
            dev_text = "(n/a)"
            dev_class = "rass-dev-na"
        else:
            dev_text = "(%+.1f)" % dev_val
            if dev_val > 0:
                dev_class = "rass-dev-pos"
            elif dev_val < 0:
                dev_class = "rass-dev-neg"
            else:
                dev_class = "rass-dev-na"

        dev_anchor = "start" if (i % 2 == 0) else "end"
        dev_x = x_px + 6 if dev_anchor == "start" else x_px - 6
        dev_x = max(margin_left + 4, min(width - margin_right - 4, dev_x))
        dev_y = max(margin_top + 8, min(height - margin_bottom - 4, y_px - 3))
        lines.append(
            '<text class="%s" x="%.2f" y="%.2f" text-anchor="%s">%s</text>'
            % (dev_class, dev_x, dev_y, dev_anchor, dev_text)
        )

    station_dev = next_lower_station_dalr_metrics(stations_recent, dalr_rate_per_1000)
    placed_ys: List[float] = []
    for row in stations_recent:
        x_px = x_to_px(row["temp_c"])
        y_px = y_to_px(row["elev_m"])
        lines.append('<rect class="station" x="%.2f" y="%.2f" width="6" height="6" />' % (x_px - 3, y_px - 3))
        for barb_line in wind_barb_svg(x_px, y_px, row.get("wind_dir"), row.get("wind_spd_mps")):
            lines.append(barb_line)

        label_y = y_px
        for _ in range(30):
            if all(abs(label_y - prev) >= 18 for prev in placed_ys):
                break
            label_y += 12
        label_y = max(margin_top + 10, min(height - margin_bottom - 16, label_y))
        placed_ys.append(label_y)

        label = row["name"]
        est_w = 6 * len(label)
        if x_px + est_w + 8 > width - margin_right:
            lines.append('<text class="station-label" x="%.2f" y="%.2f" text-anchor="end">%s</text>' % (x_px - 6, label_y + 4, label))
            dev_anchor = "end"
            dev_x = x_px - 6
        else:
            lines.append('<text class="station-label" x="%.2f" y="%.2f" text-anchor="start">%s</text>' % (x_px + 6, label_y + 4, label))
            dev_anchor = "start"
            dev_x = x_px + 6

        dev_info = station_dev.get(row["id"], {"deviation": None, "time_delta_min": None})
        dev_val = dev_info.get("deviation")
        time_delta_min = dev_info.get("time_delta_min")
        if dev_val is None:
            dev_text = "(n/a)"
            dev_class = "station-dev-na"
            time_text = ""
        else:
            dev_text = "(%+.1f)" % dev_val
            if dev_val > 0:
                dev_class = "station-dev-pos"
            elif dev_val < 0:
                dev_class = "station-dev-neg"
            else:
                dev_class = "station-dev-na"
            time_text = " (%+dmin)" % time_delta_min if isinstance(time_delta_min, int) else " (n/a)"

        dev_y = min(height - margin_bottom - 2, label_y + 14)
        if time_text:
            lines.append(
                '<text class="station-dev-na" x="%.2f" y="%.2f" text-anchor="%s"><tspan class="%s">%s</tspan><tspan fill="#444444">%s</tspan></text>'
                % (dev_x, dev_y, dev_anchor, dev_class, dev_text, time_text)
            )
        else:
            lines.append(
                '<text class="%s" x="%.2f" y="%.2f" text-anchor="%s">%s</text>'
                % (dev_class, dev_x, dev_y, dev_anchor, dev_text)
            )

    legend_x = margin_left
    legend_y = height - margin_bottom + 52
    dalr_x = legend_x + 220
    lines.append('<line class="rass" x1="%d" y1="%d" x2="%d" y2="%d" />' % (legend_x, legend_y, legend_x + 24, legend_y))
    lines.append('<text class="label" x="%d" y="%d">RASS @ %s</text>' % (legend_x + 30, legend_y + 4, rass_time_hhmm_pst))
    lines.append('<line class="dalr" x1="%d" y1="%d" x2="%d" y2="%d" />' % (dalr_x, legend_y, dalr_x + 24, legend_y))
    lines.append('<text class="label" x="%d" y="%d">%s</text>' % (dalr_x + 30, legend_y + 4, dalr_label))

    list_y0 = legend_y + 34
    lines.append('<text class="legend-h" x="%d" y="%d">Stations</text>' % (legend_x, list_y0))
    dalr_info_by_station = dalr_divergence_data(stations_all, dalr_rate_per_1000)

    row_y = list_y0 + 16
    for row in stations_all:
        temp_text = "temp missing"
        if row.get("temp_c") is not None:
            temp_text = "%.1f%s" % (row["temp_c"], temp_suffix)

        obs_time = row.get("wind_ob_time") or row.get("temp_ob_time")
        time_text = utc_iso_to_pst_hhmm(obs_time)
        if time_text:
            prefix = "%s @ %s - %s, %s, DALR dev " % (row["name"], time_text, temp_text, wind_text_for_row(row))
        else:
            prefix = "%s @ missing - %s, winds missing, DALR dev " % (row["name"], temp_text)

        dalr_info = dalr_info_by_station.get(row["id"], {"kind": "missing", "items": []})
        kind = dalr_info.get("kind")
        items = dalr_info.get("items") if isinstance(dalr_info.get("items"), list) else []
        if kind != "values" or not items:
            suffix = "n/a" if kind == "na" else "missing"
            lines.append('<text class="legend-row" x="%d" y="%d">%s%s</text>' % (legend_x, row_y, prefix, suffix))
        else:
            pieces = ['<text class="legend-row" x="%d" y="%d">' % (legend_x, row_y), "<tspan>%s</tspan>" % prefix]
            for i, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                if i > 0:
                    pieces.append("<tspan>; </tspan>")
                divergence = item.get("divergence")
                if not isinstance(divergence, (int, float)):
                    token_text = "%s:missing" % (item.get("name") or "lower")
                    token_color = "#333333"
                else:
                    token_text = "%s:%s%.1f%s" % (
                        item.get("name") or "lower",
                        "+" if divergence >= 0.0 else "",
                        divergence,
                        temp_suffix,
                    )
                    if divergence > 0:
                        token_color = "#c62828"
                    elif divergence < 0:
                        token_color = "#1565c0"
                    else:
                        token_color = "#333333"
                pieces.append('<tspan fill="%s">%s</tspan>' % (token_color, token_text))
            pieces.append("</text>")
            lines.append("".join(pieces))
        row_y += 14

    lines.append("</svg>")
    return "\n".join(lines)


def main() -> None:
    now_utc = datetime.now(timezone.utc)

    filename, rass_time_utc, rass_points, rass_source = load_rass_with_fallback()

    stations: List[Dict] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_station, station_id): station_id for station_id in STATIONS}
        for fut in as_completed(futures):
            stations.append(fut.result())

    order = {station: i for i, station in enumerate(STATIONS)}
    stations.sort(key=lambda r: order[r["id"]])

    for row in stations:
        update_age_and_recency(row, now_utc)

    cwop_targets = [row["id"] for row in stations if should_try_cwop(row)]
    if cwop_targets:
        cwop_rows: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(fetch_station_cwop, station_id): station_id for station_id in cwop_targets}
            for fut in as_completed(futures):
                cwop = fut.result()
                cwop_rows[cwop["id"]] = cwop

        for i, row in enumerate(stations):
            cwop_row = cwop_rows.get(row["id"])
            if cwop_row is not None:
                stations[i] = merge_cwop_if_needed(row, cwop_row)

    last_good = load_last_good_state()
    for i, row in enumerate(stations):
        cached = last_good.get(row["id"])
        if cached is not None:
            stations[i] = apply_last_good_fallback(row, cached, now_utc)

    for row in stations:
        update_age_and_recency(row, now_utc)

    stations_recent = [r for r in stations if r.get("recent") and r.get("temp_c") is not None and r.get("elev_m") is not None]

    rass_hhmm = utc_iso_to_pst_hhmm(rass_time_utc) or "missing"
    vor = next((r for r in stations if r["id"] == "SE068"), None)
    title_metric = build_lcl_title(vor, altitude_unit="m")
    title_imperial = build_lcl_title(vor, altitude_unit="ft")

    metric_rass = convert_rass_points_units(rass_points, unit_system="metric")
    metric_all = convert_station_rows_units(stations, unit_system="metric")
    metric_recent = [r for r in metric_all if r.get("recent") and r.get("temp_c") is not None and r.get("elev_m") is not None]

    imperial_rass = convert_rass_points_units(rass_points, unit_system="imperial")
    imperial_all = convert_station_rows_units(stations, unit_system="imperial")
    imperial_recent = [r for r in imperial_all if r.get("recent") and r.get("temp_c") is not None and r.get("elev_m") is not None]

    svg_metric = draw_svg(
        metric_rass,
        metric_recent,
        metric_all,
        title_metric,
        rass_hhmm,
        temp_unit="C",
        altitude_unit="m",
        temp_suffix="C",
        dalr_label="DALR (9.8 C/km)",
        dalr_rate_per_1000=9.8,
        y_tick_step=200,
    )
    svg_imperial = draw_svg(
        imperial_rass,
        imperial_recent,
        imperial_all,
        title_imperial,
        rass_hhmm,
        temp_unit="F",
        altitude_unit="ft",
        temp_suffix="F",
        dalr_label="DALR (5.4 F/1000 ft)",
        dalr_rate_per_1000=5.4,
        y_tick_step=500,
    )
    CHART_METRIC_PATH.write_text(svg_metric)
    CHART_IMPERIAL_PATH.write_text(svg_imperial)
    CHART_PATH.write_text(svg_metric)

    csv_lines = ["station,name,elev_m,temp_c,ob_time,age_min,provider,recent"]
    for row in stations:
        elev = "" if row.get("elev_m") is None else "%.2f" % row["elev_m"]
        temp = "" if row.get("temp_c") is None else "%.2f" % row["temp_c"]
        ob_time = row.get("temp_ob_time") or ""
        age = "" if row.get("age_min") is None else "%.1f" % row["age_min"]
        provider = row.get("provider") or ("no_temp" if not row.get("temp_ob_time") else "")
        recent = "TRUE" if row.get("recent") else "FALSE"
        csv_lines.append(
            "%s,%s,%s,%s,%s,%s,%s,%s"
            % (row["id"], row["name"], elev, temp, ob_time, age, provider, recent)
        )

    CSV_PATH.write_text("\n".join(csv_lines) + "\n")
    write_station_state(stations, now_utc)
    write_station_history(
        stations,
        now_utc,
        rass_filename=filename,
        rass_time_utc=rass_time_utc,
        rass_source=rass_source,
        rass_points=rass_points,
        metric_svg=svg_metric,
        imperial_svg=svg_imperial,
    )

    print("rass_file=%s" % filename)
    print("rass_source=%s" % rass_source)
    print("rass_time_utc=%s" % (rass_time_utc or "missing"))
    print("title_metric=%s" % title_metric)
    print("title_imperial=%s" % title_imperial)
    print("recent_station_count=%d" % len(stations_recent))
    for row in stations:
        print(
            "%s name=%s provider=%s temp=%s dew=%s wind_time=%s recent=%s"
            % (
                row["id"],
                row["name"],
                row.get("provider") or "none",
                "" if row.get("temp_c") is None else "%.2f" % row["temp_c"],
                "" if row.get("dew_c") is None else "%.2f" % row["dew_c"],
                row.get("wind_ob_time") or "missing",
                "TRUE" if row.get("recent") else "FALSE",
            )
        )


if __name__ == "__main__":
    main()
