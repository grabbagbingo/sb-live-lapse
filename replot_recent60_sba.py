#!/usr/bin/env python3
import math
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


STATIONS = ["KC60YN", "SE068", "SE234", "MTIC1", "MPWC1", "421SE", "SE053", "KSBA"]
STATION_NAMES = {
    "KC60YN": "La Cumbre",
    "SE068": "VOR",
    "SE234": "AntFarm",
    "MTIC1": "Montecito",
    "MPWC1": "SM Pass",
    "421SE": "Upper Parma",
    "SE053": "Romero Cyn",
    "KSBA": "Airport",
}

RASS_BASE = "https://downloads.psl.noaa.gov/psd2/data/realtime/Radar449/WwTemp/sba/"
MADIS_BASE = "https://madis-data.ncep.noaa.gov/madisPublic/cgi-bin/madisXmlPublicDir"

# Repo-relative outputs so GitHub Actions can run this anywhere.
CHART_PATH = Path("sba_wwtemp_chart.svg")
CSV_PATH = Path("madis_recent60_stations.csv")
RASS_TEXT_PATH = Path("sba_latest.01t")

MS_TO_MPH = 2.23694
PST = timezone(timedelta(hours=-8), name="PST")


def fetch_text(url: str, timeout: int = 25) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def latest_rass_file() -> Tuple[str, str, str]:
    root_html = fetch_text(RASS_BASE)
    years = [int(v) for v in re.findall(r'href="(20\d{2})/"', root_html)]
    if not years:
        raise RuntimeError("No RASS year directories found")

    year = max(years)
    year_url = f"{RASS_BASE}{year}/"
    year_html = fetch_text(year_url)
    doys = [int(v) for v in re.findall(r'href="(\d{3})/"', year_html)]
    if not doys:
        raise RuntimeError("No RASS day directories found")

    doy = max(doys)
    day_url = f"{year_url}{doy:03d}/"
    day_html = fetch_text(day_url)
    files = re.findall(r'href="(sba\d{5}\.\d{2}t)"', day_html)
    if not files:
        raise RuntimeError("No RASS files found in latest day")

    return str(year), f"{doy:03d}", sorted(files)[-1]


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


def utc_iso_to_pst_hhmm(iso_time: Optional[str]) -> Optional[str]:
    if not iso_time:
        return None
    fmt = "%Y-%m-%dT%H:%M:%S" if len(iso_time) == 19 else "%Y-%m-%dT%H:%M"
    dt_utc = datetime.strptime(iso_time, fmt).replace(tzinfo=timezone.utc)
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


def draw_svg(
    rass_points: List[Tuple[int, float]],
    stations_recent: List[Dict],
    stations_all: List[Dict],
    title_text: str,
    rass_time_hhmm_pst: str,
) -> str:
    width, height = 1180, 600
    margin_left, margin_right, margin_top, margin_bottom = 90, 420, 50, 80
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    anchor_alt, anchor_temp = rass_points[0]

    def dalr_temp(alt_m: float) -> float:
        return anchor_temp - 9.8 * (alt_m - anchor_alt) / 1000.0

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
    y_cursor = int(math.ceil(y_min / 200.0) * 200)
    while y_cursor <= y_max:
        y_ticks.append(y_cursor)
        y_cursor += 200

    lines = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">' % (width, height, width, height),
        "<style>",
        "  .axis { stroke: #202020; stroke-width: 1; }",
        "  .grid { stroke: #dddddd; stroke-width: 1; }",
        "  .title { font-family: Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 600; fill: #111111; }",
        "  .label { font-family: Helvetica, Arial, sans-serif; font-size: 12px; fill: #222222; }",
        "  .legend-h { font-family: Helvetica, Arial, sans-serif; font-size: 12px; font-weight: 600; fill: #222222; }",
        "  .legend-row { font-family: Helvetica, Arial, sans-serif; font-size: 11px; fill: #333333; }",
        "  .rass { fill: none; stroke: #0077b6; stroke-width: 2; }",
        "  .dalr { fill: none; stroke: #d1495b; stroke-width: 2; stroke-dasharray: 6 4; }",
        "  .rass-point { fill: #0077b6; }",
        "  .station { fill: #f4a261; stroke: #8b4c12; stroke-width: 1; }",
        "  .station-label { font-family: Helvetica, Arial, sans-serif; font-size: 11px; fill: #444444; }",
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
    lines.append('<text class="label" x="%.2f" y="%d" text-anchor="middle">Temperature (C)</text>' % (margin_left + plot_w / 2.0, height - 30))
    lines.append(
        '<text class="label" x="26" y="%.2f" text-anchor="middle" transform="rotate(-90 26 %.2f)">Altitude (m)</text>'
        % (margin_top + plot_h / 2.0, margin_top + plot_h / 2.0)
    )

    lines.append('<path class="rass" d="%s" />' % obs_path)
    lines.append('<path class="dalr" d="%s" />' % dalr_path)

    for alt, temp in rass_points:
        lines.append('<circle class="rass-point" cx="%.2f" cy="%.2f" r="2" />' % (x_to_px(temp), y_to_px(alt)))

    placed_ys: List[float] = []
    for row in stations_recent:
        x_px = x_to_px(row["temp_c"])
        y_px = y_to_px(row["elev_m"])
        lines.append('<rect class="station" x="%.2f" y="%.2f" width="6" height="6" />' % (x_px - 3, y_px - 3))

        label_y = y_px
        for _ in range(30):
            if all(abs(label_y - prev) >= 12 for prev in placed_ys):
                break
            label_y += 12
        label_y = max(margin_top + 10, min(height - margin_bottom - 4, label_y))
        placed_ys.append(label_y)

        label = row["name"]
        est_w = 6 * len(label)
        if x_px + est_w + 8 > width - margin_right:
            lines.append('<text class="station-label" x="%.2f" y="%.2f" text-anchor="end">%s</text>' % (x_px - 6, label_y + 4, label))
        else:
            lines.append('<text class="station-label" x="%.2f" y="%.2f" text-anchor="start">%s</text>' % (x_px + 6, label_y + 4, label))

    legend_x = width - margin_right + 14
    legend_y = margin_top + 10
    lines.append('<line class="rass" x1="%d" y1="%d" x2="%d" y2="%d" />' % (legend_x, legend_y, legend_x + 24, legend_y))
    lines.append('<text class="label" x="%d" y="%d">RASS @ %s</text>' % (legend_x + 30, legend_y + 4, rass_time_hhmm_pst))
    lines.append('<line class="dalr" x1="%d" y1="%d" x2="%d" y2="%d" />' % (legend_x, legend_y + 20, legend_x + 24, legend_y + 20))
    lines.append('<text class="label" x="%d" y="%d">DALR (9.8 C/km)</text>' % (legend_x + 30, legend_y + 24))

    list_y0 = legend_y + 62
    lines.append('<text class="legend-h" x="%d" y="%d">Stations (PST)</text>' % (legend_x, list_y0))

    row_y = list_y0 + 16
    for row in stations_all:
        wind_time = utc_iso_to_pst_hhmm(row.get("wind_ob_time"))
        if wind_time:
            text = "%s @ %s - %s" % (row["name"], wind_time, wind_text_for_row(row))
        else:
            text = "%s @ missing - winds missing" % row["name"]
        lines.append('<text class="legend-row" x="%d" y="%d">%s</text>' % (legend_x, row_y, text))
        row_y += 14

    lines.append("</svg>")
    return "\n".join(lines)


def main() -> None:
    now_utc = datetime.now(timezone.utc)

    year, doy, filename = latest_rass_file()
    rass_url = f"{RASS_BASE}{year}/{doy}/{filename}"
    raw_rass = fetch_text(rass_url)
    RASS_TEXT_PATH.write_text(raw_rass)

    rass_time_utc, rass_points = parse_rass(raw_rass)

    stations: List[Dict] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_station, station_id): station_id for station_id in STATIONS}
        for fut in as_completed(futures):
            stations.append(fut.result())

    order = {station: i for i, station in enumerate(STATIONS)}
    stations.sort(key=lambda r: order[r["id"]])

    for row in stations:
        row["recent"] = False
        row["age_min"] = None
        if row.get("temp_ob_time"):
            dt_utc = datetime.strptime(row["temp_ob_time"], "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
            row["age_min"] = (now_utc - dt_utc).total_seconds() / 60.0
            row["recent"] = row["age_min"] <= 60.0

    stations_recent = [r for r in stations if r.get("recent") and r.get("temp_c") is not None and r.get("elev_m") is not None]

    title = "Estimated Cloud Base @ VOR: missing"
    vor = next((r for r in stations if r["id"] == "SE068"), None)
    vor_wind = "winds missing"
    if vor is not None:
        vor_wind = wind_text_for_row(vor)
    if vor and vor.get("elev_m") is not None and vor.get("temp_c") is not None and vor.get("dew_c") is not None:
        cloud_base_m = vor["elev_m"] + 125.0 * (vor["temp_c"] - vor["dew_c"])
        cloud_base_i = int(round(cloud_base_m))
        time_hhmm = utc_iso_to_pst_hhmm(vor.get("temp_ob_time"))
        if time_hhmm:
            title = f"Estimated Cloud Base @ VOR: {cloud_base_i} m - {vor_wind} ({time_hhmm} PST)"
        else:
            title = f"Estimated Cloud Base @ VOR: {cloud_base_i} m - {vor_wind}"
    else:
        title = f"Estimated Cloud Base @ VOR: missing - {vor_wind}"

    rass_hhmm = utc_iso_to_pst_hhmm(rass_time_utc) or "missing"

    svg = draw_svg(rass_points, stations_recent, stations, title, rass_hhmm)
    CHART_PATH.write_text(svg)

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

    print("rass_file=%s" % filename)
    print("rass_time_utc=%s" % (rass_time_utc or "missing"))
    print("title=%s" % title)
    print("recent_station_count=%d" % len(stations_recent))
    for row in stations:
        print(
            "%s name=%s temp=%s dew=%s wind_time=%s recent=%s"
            % (
                row["id"],
                row["name"],
                "" if row.get("temp_c") is None else "%.2f" % row["temp_c"],
                "" if row.get("dew_c") is None else "%.2f" % row["dew_c"],
                row.get("wind_ob_time") or "missing",
                "TRUE" if row.get("recent") else "FALSE",
            )
        )


if __name__ == "__main__":
    main()
