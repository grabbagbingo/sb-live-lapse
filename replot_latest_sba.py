#!/usr/bin/env python3
import math
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple


STATIONS = ["KC60YN", "SE068", "SE234", "MTIC1", "MPWC1", "421SE", "SE053", "KSBA"]
RASS_BASE = "https://downloads.psl.noaa.gov/psd2/data/realtime/Radar449/WwTemp/sba/"
MADIS_BASE = "https://madis-data.ncep.noaa.gov/madisPublic/cgi-bin/madisXmlPublicDir"
CHART_PATH = Path("/Users/james/vibe/RASStastic/sba_wwtemp_chart.svg")
STATION_CSV_PATH = Path("/Users/james/vibe/RASStastic/madis_latest_stations.csv")
RASS_TXT_PATH = Path("/Users/james/vibe/RASStastic/sba_latest.01t")


def fetch_text(url: str, timeout: int = 25) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def latest_rass_file() -> Tuple[str, str, str]:
    root_html = fetch_text(RASS_BASE)
    years = [int(y) for y in re.findall(r'href="(20\d{2})/"', root_html)]
    if not years:
        raise RuntimeError("No year directories found for RASS")
    year = max(years)

    year_url = f"{RASS_BASE}{year}/"
    year_html = fetch_text(year_url)
    doys = [int(d) for d in re.findall(r'href="(\d{3})/"', year_html)]
    if not doys:
        raise RuntimeError("No day directories found for RASS year")
    doy = max(doys)

    day_url = f"{year_url}{doy:03d}/"
    day_html = fetch_text(day_url)
    files = re.findall(r'href="(sba\d{5}\.\d{2}t)"', day_html)
    if not files:
        raise RuntimeError("No SBA profile files found in latest day directory")
    latest_file = sorted(files)[-1]
    return str(year), f"{doy:03d}", latest_file


def parse_rass_profile(raw_text: str) -> Tuple[Optional[str], List[Tuple[int, float]]]:
    lines = raw_text.splitlines()

    obs_dt = None
    for line in lines[:12]:
        parts = line.split()
        if len(parts) >= 6 and all(p.replace(".", "", 1).isdigit() for p in parts[:6]):
            yy, mm, dd, hh, mi, ss = parts[:6]
            if len(yy) <= 2:
                obs_dt = f"20{yy}-{mm.zfill(2)}-{dd.zfill(2)} {hh.zfill(2)}:{mi.zfill(2)}:{ss.zfill(2)}"
                break

    start_idx = None
    for idx, line in enumerate(lines):
        if line.strip().startswith("HT"):
            start_idx = idx + 1
            break
    if start_idx is None:
        raise RuntimeError("Could not find RASS data table start")

    points = []
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

    interpolated = []
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
        interpolated.append((alt, t))

    return obs_dt, interpolated


def query_madis(params: Dict, timeout_s: int = 22) -> Dict[str, Dict]:
    url = MADIS_BASE + "?" + urllib.parse.urlencode(params)
    raw = fetch_text(url, timeout=timeout_s)
    root = ET.fromstring(raw)
    out: Dict[str, Dict] = {}
    for rec in root.findall("record"):
        if rec.attrib.get("var") != "V-T":
            continue
        station_id = rec.attrib.get("shef_id")
        ob_time = rec.attrib.get("ObTime")
        elev = rec.attrib.get("elev")
        value = rec.attrib.get("data_value")
        if not station_id or not ob_time or not elev or not value:
            continue
        row = {
            "id": station_id,
            "elev_m": float(elev),
            "temp_c": float(value) - 273.15,
            "ob_time": ob_time,
            "provider": rec.attrib.get("provider", ""),
        }
        prev = out.get(station_id)
        if prev is None or row["ob_time"] > prev["ob_time"]:
            out[station_id] = row
    return out


def query_ca_snapshot(time_value: str) -> Dict[str, Dict]:
    params = {
        "time": time_value,
        "minbck": "-59",
        "minfwd": "0",
        "recwin": "3",
        "timefilter": "0",
        "dfltrsel": "2",
        "state": "CA",
        "stasel": "0",
        "pvdrsel": "0",
        "varsel": "2",
        "qctype": "0",
        "qcsel": "1",
        "xml": "1",
        "csvmiss": "0",
    }
    return query_madis(params)


def query_station_snapshot(station_id: str, time_value: str) -> Dict[str, Dict]:
    params = {
        "time": time_value,
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
    return query_madis(params, timeout_s=18)


def fetch_one_station_fallback(station_id: str, base_time: datetime) -> Dict:
    lag_checks = [0, 1, 2, 3, 6, 9, 12, 18, 24, 36, 48, 72, 96, 120, 144, 168]
    last_error = None
    for lag_h in lag_checks:
        nominal = "0" if lag_h == 0 else (base_time - timedelta(hours=lag_h)).strftime("%Y%m%d_%H%M")
        try:
            rows = query_station_snapshot(station_id, nominal)
            row = rows.get(station_id)
            if row:
                return row
        except Exception as exc:
            last_error = str(exc)

    return {
        "id": station_id,
        "elev_m": None,
        "temp_c": None,
        "ob_time": None,
        "provider": f"fetch_error: {last_error}" if last_error else "no_temp",
    }


def fetch_madis_stations() -> List[Dict]:
    base_time = datetime.utcnow().replace(second=0, microsecond=0)
    pending = set(STATIONS)
    results: Dict[str, Dict] = {}

    for lag_h in range(0, 73):
        if not pending:
            break
        nominal = "0" if lag_h == 0 else (base_time - timedelta(hours=lag_h)).strftime("%Y%m%d_%H%M")
        try:
            snapshot = query_ca_snapshot(nominal)
        except Exception:
            continue
        for station_id in list(pending):
            row = snapshot.get(station_id)
            if row:
                results[station_id] = row
                pending.remove(station_id)

    if pending:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(fetch_one_station_fallback, stn, base_time): stn for stn in pending}
            for future in as_completed(futures):
                row = future.result()
                results[row["id"]] = row

    return [results.get(stn, {"id": stn, "elev_m": None, "temp_c": None, "ob_time": None, "provider": "no_temp"}) for stn in STATIONS]


def draw_chart(
    rass_points: List[Tuple[int, float]],
    station_points: List[Dict],
    obs_dt: Optional[str],
    rass_file: str,
) -> str:
    width, height = 940, 560
    margin_left, margin_right, margin_top, margin_bottom = 90, 190, 50, 80
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    anchor_alt, anchor_temp = rass_points[0]

    def dalr_temp(alt_m: float) -> float:
        return anchor_temp - 9.8 * (alt_m - anchor_alt) / 1000.0

    station_valid = [s for s in station_points if s["temp_c"] is not None and s["elev_m"] is not None]

    station_alts = [s["elev_m"] for s in station_valid]
    y_min = min([rass_points[0][0], 0.0] + station_alts) if station_alts else min(rass_points[0][0], 0.0)
    y_max = max([rass_points[-1][0]] + station_alts) if station_alts else rass_points[-1][0]
    y_min = int(math.floor(y_min / 100.0) * 100)
    y_max = int(math.ceil(y_max / 100.0) * 100)
    if y_max == y_min:
        y_max += 100

    x_vals = [t for _, t in rass_points]
    x_vals += [dalr_temp(a) for a, _ in rass_points]
    x_vals += [s["temp_c"] for s in station_valid]
    x_min, x_max = min(x_vals), max(x_vals)
    if x_max - x_min < 1.0:
        x_min -= 0.5
        x_max += 0.5
    else:
        x_min -= 0.5
        x_max += 0.5

    def x_to_svg(temp_c: float) -> float:
        return margin_left + (temp_c - x_min) / (x_max - x_min) * plot_w

    def y_to_svg(alt_m: float) -> float:
        return margin_top + (y_max - alt_m) / (y_max - y_min) * plot_h

    obs_path = " ".join(
        (("M" if i == 0 else "L") + f"{x_to_svg(temp):.2f},{y_to_svg(alt):.2f}")
        for i, (alt, temp) in enumerate(rass_points)
    )
    dalr_path = " ".join(
        (("M" if i == 0 else "L") + f"{x_to_svg(dalr_temp(alt)):.2f},{y_to_svg(alt):.2f}")
        for i, (alt, _) in enumerate(rass_points)
    )

    x_range = x_max - x_min
    if x_range <= 5:
        x_step = 1
    elif x_range <= 10:
        x_step = 2
    else:
        x_step = 5
    x_ticks = []
    x_cursor = math.ceil(x_min / x_step) * x_step
    while x_cursor <= x_max:
        x_ticks.append(x_cursor)
        x_cursor += x_step

    y_step = 200
    y_ticks = []
    y_cursor = int(math.ceil(y_min / y_step) * y_step)
    while y_cursor <= y_max:
        y_ticks.append(y_cursor)
        y_cursor += y_step

    title = "SBA 449 MHz RASS Weber Wuertz Temp"
    if obs_dt:
        title += f" ({obs_dt} UTC)"
    subtitle = f"RASS file: {rass_file} | MADIS: latest available per station"

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "  .axis { stroke: #202020; stroke-width: 1; }",
        "  .grid { stroke: #dddddd; stroke-width: 1; }",
        "  .title { font-family: Helvetica, Arial, sans-serif; font-size: 16px; font-weight: 600; fill: #111111; }",
        "  .subtitle { font-family: Helvetica, Arial, sans-serif; font-size: 12px; fill: #555555; }",
        "  .label { font-family: Helvetica, Arial, sans-serif; font-size: 12px; fill: #222222; }",
        "  .rass { fill: none; stroke: #0077b6; stroke-width: 2; }",
        "  .dalr { fill: none; stroke: #d1495b; stroke-width: 2; stroke-dasharray: 6 4; }",
        "  .rass-point { fill: #0077b6; }",
        "  .station { fill: #f4a261; stroke: #8b4c12; stroke-width: 1; }",
        "  .station-label { font-family: Helvetica, Arial, sans-serif; font-size: 11px; fill: #444444; }",
        "  .missing { font-family: Helvetica, Arial, sans-serif; font-size: 11px; fill: #666666; }",
        "</style>",
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="#ffffff" />',
        f'<text class="title" x="{margin_left}" y="{margin_top - 22}">{title}</text>',
        f'<text class="subtitle" x="{margin_left}" y="{margin_top - 6}">{subtitle}</text>',
    ]

    for y in y_ticks:
        y_px = y_to_svg(y)
        lines.append(f'<line class="grid" x1="{margin_left}" y1="{y_px:.2f}" x2="{width - margin_right}" y2="{y_px:.2f}" />')
        lines.append(f'<text class="label" x="{margin_left - 8}" y="{y_px + 4:.2f}" text-anchor="end">{y}</text>')

    for x in x_ticks:
        x_px = x_to_svg(x)
        lines.append(f'<line class="grid" x1="{x_px:.2f}" y1="{margin_top}" x2="{x_px:.2f}" y2="{height - margin_bottom}" />')
        lines.append(f'<text class="label" x="{x_px:.2f}" y="{height - margin_bottom + 18}" text-anchor="middle">{x:.1f}</text>')

    lines.append(f'<line class="axis" x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{height - margin_bottom}" />')
    lines.append(f'<line class="axis" x1="{margin_left}" y1="{height - margin_bottom}" x2="{width - margin_right}" y2="{height - margin_bottom}" />')
    lines.append(
        f'<text class="label" x="{margin_left + plot_w / 2:.2f}" y="{height - 30}" text-anchor="middle">Temperature (C)</text>'
    )
    lines.append(
        f'<text class="label" x="26" y="{margin_top + plot_h / 2:.2f}" text-anchor="middle" '
        f'transform="rotate(-90 26 {margin_top + plot_h / 2:.2f})">Altitude (m)</text>'
    )

    lines.append(f'<path class="rass" d="{obs_path}" />')
    lines.append(f'<path class="dalr" d="{dalr_path}" />')
    for alt, temp in rass_points:
        lines.append(f'<circle class="rass-point" cx="{x_to_svg(temp):.2f}" cy="{y_to_svg(alt):.2f}" r="2" />')

    used_label_y = []
    for stn in station_valid:
        x_px = x_to_svg(stn["temp_c"])
        y_px = y_to_svg(stn["elev_m"])
        lines.append(f'<rect class="station" x="{x_px - 3:.2f}" y="{y_px - 3:.2f}" width="6" height="6" />')
        label_y = y_px
        for _ in range(30):
            if all(abs(label_y - prev) >= 12 for prev in used_label_y):
                break
            label_y += 12
        label_y = max(margin_top + 10, min(height - margin_bottom - 4, label_y))
        used_label_y.append(label_y)
        text_width = 6 * len(stn["id"])
        if x_px + text_width + 8 > width - margin_right:
            lines.append(
                f'<text class="station-label" x="{x_px - 6:.2f}" y="{label_y + 4:.2f}" text-anchor="end">{stn["id"]}</text>'
            )
        else:
            lines.append(
                f'<text class="station-label" x="{x_px + 6:.2f}" y="{label_y + 4:.2f}" text-anchor="start">{stn["id"]}</text>'
            )

    legend_x = width - margin_right + 10
    legend_y = margin_top + 10
    lines.append(f'<line class="rass" x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 24}" y2="{legend_y}" />')
    lines.append(f'<text class="label" x="{legend_x + 30}" y="{legend_y + 4}">Observed (RASS)</text>')
    lines.append(f'<line class="dalr" x1="{legend_x}" y1="{legend_y + 20}" x2="{legend_x + 24}" y2="{legend_y + 20}" />')
    lines.append(f'<text class="label" x="{legend_x + 30}" y="{legend_y + 24}">DALR (9.8 C/km)</text>')
    lines.append(f'<rect class="station" x="{legend_x + 9}" y="{legend_y + 34}" width="6" height="6" />')
    lines.append(f'<text class="label" x="{legend_x + 30}" y="{legend_y + 40}">MADIS station</text>')

    missing = [s["id"] for s in station_points if s["temp_c"] is None or s["elev_m"] is None]
    if missing:
        lines.append(
            f'<text class="missing" x="{legend_x}" y="{legend_y + 62}">No MADIS temp: {", ".join(missing)}</text>'
        )

    lines.append("</svg>")
    return "\n".join(lines)


def main() -> None:
    year, doy, latest_file = latest_rass_file()
    rass_url = f"{RASS_BASE}{year}/{doy}/{latest_file}"
    raw_rass = fetch_text(rass_url)
    RASS_TXT_PATH.write_text(raw_rass)
    obs_dt, rass_points = parse_rass_profile(raw_rass)

    stations = fetch_madis_stations()
    svg = draw_chart(rass_points, stations, obs_dt, latest_file)
    CHART_PATH.write_text(svg)

    csv_rows = ["station,elev_m,temp_c,ob_time,provider"]
    for row in stations:
        if row["elev_m"] is None:
            csv_rows.append(f'{row["id"]},,,,{row["provider"]}')
        else:
            csv_rows.append(
                f'{row["id"]},{row["elev_m"]:.2f},{row["temp_c"]:.2f},{row["ob_time"]},{row["provider"]}'
            )
    STATION_CSV_PATH.write_text("\n".join(csv_rows) + "\n")

    print(f"rass_file={latest_file}")
    print(f"rass_time={obs_dt}")
    print(f"chart={CHART_PATH}")
    print(f"stations_csv={STATION_CSV_PATH}")
    for row in stations:
        if row["elev_m"] is None:
            print(f'{row["id"]}: missing ({row["provider"]})')
        else:
            print(
                f'{row["id"]}: elev_m={row["elev_m"]:.2f}, temp_c={row["temp_c"]:.2f}, '
                f'ob_time={row["ob_time"]}, provider={row["provider"]}'
            )


if __name__ == "__main__":
    main()
