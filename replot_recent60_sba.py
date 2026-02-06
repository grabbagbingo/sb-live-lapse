#!/usr/bin/env python3
import math
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


STATIONS = ["KC60YN", "SE068", "SE234", "MTIC1", "MPWC1", "421SE", "SE053", "KSBA"]
RASS_BASE = "https://downloads.psl.noaa.gov/psd2/data/realtime/Radar449/WwTemp/sba/"
MADIS_BASE = "https://madis-data.ncep.noaa.gov/madisPublic/cgi-bin/madisXmlPublicDir"
CHART_PATH = Path("/Users/james/vibe/RASStastic/sba_wwtemp_chart.svg")
CSV_PATH = Path("/Users/james/vibe/RASStastic/madis_recent60_stations.csv")
RASS_TEXT_PATH = Path("/Users/james/vibe/RASStastic/sba_latest.01t")


def fetch_text(url: str, timeout: int = 25) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="ignore")


def latest_rass_file() -> Tuple[str, str, str]:
    root = fetch_text(RASS_BASE)
    years = [int(v) for v in re.findall(r'href="(20\d{2})/"', root)]
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
    latest = sorted(files)[-1]
    return str(year), f"{doy:03d}", latest


def parse_rass(raw: str) -> Tuple[Optional[str], List[Tuple[int, float]]]:
    lines = raw.splitlines()

    obs_dt = None
    for line in lines[:12]:
        parts = line.split()
        if len(parts) >= 6 and all(p.replace(".", "", 1).isdigit() for p in parts[:6]):
            yy, mm, dd, hh, mi, ss = parts[:6]
            if len(yy) <= 2:
                obs_dt = "20%s-%s-%s %s:%s:%s" % (
                    yy,
                    mm.zfill(2),
                    dd.zfill(2),
                    hh.zfill(2),
                    mi.zfill(2),
                    ss.zfill(2),
                )
                break

    start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("HT"):
            start_idx = i + 1
            break
    if start_idx is None:
        raise RuntimeError("RASS table header not found")

    pairs: List[Tuple[float, float]] = []
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
        pairs.append((alt_m, temp_c))

    if len(pairs) < 2:
        raise RuntimeError("Not enough valid RASS points")

    pairs.sort(key=lambda x: x[0])
    min_alt = int(math.ceil(pairs[0][0] / 100.0) * 100)
    max_alt = int(math.floor(pairs[-1][0] / 100.0) * 100)
    alts = list(range(min_alt, max_alt + 1, 100))

    out: List[Tuple[int, float]] = []
    j = 0
    for alt in alts:
        while j < len(pairs) - 2 and pairs[j + 1][0] < alt:
            j += 1
        a0, t0 = pairs[j]
        a1, t1 = pairs[j + 1]
        if a1 == a0:
            t = t0
        else:
            t = t0 + (t1 - t0) * (alt - a0) / (a1 - a0)
        out.append((alt, t))
    return obs_dt, out


def fetch_station(station_id: str, now_utc: datetime) -> Dict:
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

    try:
        raw = fetch_text(url, timeout=20)
    except Exception as exc:
        return {
            "id": station_id,
            "elev_m": None,
            "temp_c": None,
            "ob_time": None,
            "age_min": None,
            "provider": "fetch_error: %s" % str(exc),
            "recent": False,
        }

    try:
        root = ET.fromstring(raw)
    except Exception as exc:
        return {
            "id": station_id,
            "elev_m": None,
            "temp_c": None,
            "ob_time": None,
            "age_min": None,
            "provider": "xml_error: %s" % str(exc),
            "recent": False,
        }

    records = []
    for rec in root.findall("record"):
        if rec.attrib.get("var") != "V-T":
            continue
        ob_time = rec.attrib.get("ObTime")
        elev = rec.attrib.get("elev")
        val = rec.attrib.get("data_value")
        provider = rec.attrib.get("provider", "")
        if not ob_time or not elev or not val:
            continue
        records.append((ob_time, float(elev), float(val), provider))

    if not records:
        return {
            "id": station_id,
            "elev_m": None,
            "temp_c": None,
            "ob_time": None,
            "age_min": None,
            "provider": "no_temp",
            "recent": False,
        }

    records.sort(key=lambda r: datetime.strptime(r[0], "%Y-%m-%dT%H:%M"))
    ob_time, elev_m, temp_k, provider = records[-1]
    obs_dt = datetime.strptime(ob_time, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
    age_min = (now_utc - obs_dt).total_seconds() / 60.0
    recent = age_min <= 60.0
    return {
        "id": station_id,
        "elev_m": elev_m,
        "temp_c": temp_k - 273.15,
        "ob_time": ob_time,
        "age_min": age_min,
        "provider": provider,
        "recent": recent,
    }


def draw_svg(
    rass_points: List[Tuple[int, float]],
    stations_recent: List[Dict],
    stations_all: List[Dict],
    rass_time: Optional[str],
    rass_file: str,
) -> str:
    width, height = 940, 560
    margin_left, margin_right, margin_top, margin_bottom = 90, 190, 50, 80
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

    x_vals = [t for _, t in rass_points] + [dalr_temp(a) for a, _ in rass_points]
    x_vals.extend([s["temp_c"] for s in stations_recent if s["temp_c"] is not None])
    x_min = min(x_vals) - 0.5
    x_max = max(x_vals) + 0.5
    if x_max - x_min < 1.0:
        x_min -= 0.5
        x_max += 0.5

    def x_to_px(temp_c: float) -> float:
        return margin_left + (temp_c - x_min) / (x_max - x_min) * plot_w

    def y_to_px(alt_m: float) -> float:
        return margin_top + (y_max - alt_m) / (y_max - y_min) * plot_h

    obs_path = " ".join(
        (("M" if i == 0 else "L") + "%.2f,%.2f" % (x_to_px(t), y_to_px(a)))
        for i, (a, t) in enumerate(rass_points)
    )
    dalr_path = " ".join(
        (("M" if i == 0 else "L") + "%.2f,%.2f" % (x_to_px(dalr_temp(a)), y_to_px(a)))
        for i, (a, _) in enumerate(rass_points)
    )

    x_span = x_max - x_min
    if x_span <= 5:
        x_step = 1
    elif x_span <= 10:
        x_step = 2
    else:
        x_step = 5
    x_ticks = []
    xv = math.ceil(x_min / x_step) * x_step
    while xv <= x_max:
        x_ticks.append(xv)
        xv += x_step

    y_ticks = []
    yv = int(math.ceil(y_min / 200.0) * 200)
    while yv <= y_max:
        y_ticks.append(yv)
        yv += 200

    title = "SBA 449 MHz RASS Weber Wuertz Temp"
    if rass_time:
        title += " (%s UTC)" % rass_time
    subtitle = "RASS file: %s | MADIS stations: latest obs within last 60 min" % rass_file

    lines: List[str] = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">' % (width, height, width, height),
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
        "  .note { font-family: Helvetica, Arial, sans-serif; font-size: 11px; fill: #666666; }",
        "</style>",
        '<rect x="0" y="0" width="%d" height="%d" fill="#ffffff" />' % (width, height),
        '<text class="title" x="%d" y="%d">%s</text>' % (margin_left, margin_top - 22, title),
        '<text class="subtitle" x="%d" y="%d">%s</text>' % (margin_left, margin_top - 6, subtitle),
    ]

    for yt in y_ticks:
        ypx = y_to_px(yt)
        lines.append('<line class="grid" x1="%d" y1="%.2f" x2="%d" y2="%.2f" />' % (margin_left, ypx, width - margin_right, ypx))
        lines.append('<text class="label" x="%d" y="%.2f" text-anchor="end">%d</text>' % (margin_left - 8, ypx + 4, yt))

    for xt in x_ticks:
        xpx = x_to_px(xt)
        lines.append('<line class="grid" x1="%.2f" y1="%d" x2="%.2f" y2="%d" />' % (xpx, margin_top, xpx, height - margin_bottom))
        lines.append('<text class="label" x="%.2f" y="%d" text-anchor="middle">%.1f</text>' % (xpx, height - margin_bottom + 18, xt))

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

    used_ys: List[float] = []
    for row in stations_recent:
        xpx = x_to_px(row["temp_c"])
        ypx = y_to_px(row["elev_m"])
        lines.append('<rect class="station" x="%.2f" y="%.2f" width="6" height="6" />' % (xpx - 3, ypx - 3))
        ly = ypx
        for _ in range(25):
            if all(abs(ly - py) >= 12 for py in used_ys):
                break
            ly += 12
        ly = max(margin_top + 10, min(height - margin_bottom - 4, ly))
        used_ys.append(ly)
        if xpx + 6 * len(row["id"]) + 8 > width - margin_right:
            lines.append('<text class="station-label" x="%.2f" y="%.2f" text-anchor="end">%s</text>' % (xpx - 6, ly + 4, row["id"]))
        else:
            lines.append('<text class="station-label" x="%.2f" y="%.2f" text-anchor="start">%s</text>' % (xpx + 6, ly + 4, row["id"]))

    legend_x = width - margin_right + 10
    legend_y = margin_top + 10
    lines.append('<line class="rass" x1="%d" y1="%d" x2="%d" y2="%d" />' % (legend_x, legend_y, legend_x + 24, legend_y))
    lines.append('<text class="label" x="%d" y="%d">Observed (RASS)</text>' % (legend_x + 30, legend_y + 4))
    lines.append('<line class="dalr" x1="%d" y1="%d" x2="%d" y2="%d" />' % (legend_x, legend_y + 20, legend_x + 24, legend_y + 20))
    lines.append('<text class="label" x="%d" y="%d">DALR (9.8 C/km)</text>' % (legend_x + 30, legend_y + 24))
    lines.append('<rect class="station" x="%d" y="%d" width="6" height="6" />' % (legend_x + 9, legend_y + 34))
    lines.append('<text class="label" x="%d" y="%d">MADIS (<=60 min)</text>' % (legend_x + 30, legend_y + 40))

    excluded = [r["id"] for r in stations_all if not r["recent"]]
    if excluded:
        lines.append('<text class="note" x="%d" y="%d">Excluded: %s</text>' % (legend_x, legend_y + 62, ", ".join(excluded)))

    lines.append("</svg>")
    return "\n".join(lines)


def main() -> None:
    now_utc = datetime.now(timezone.utc)

    year, doy, fname = latest_rass_file()
    rass_url = "%s%s/%s/%s" % (RASS_BASE, year, doy, fname)
    raw = fetch_text(rass_url)
    RASS_TEXT_PATH.write_text(raw)
    rass_time, rass_points = parse_rass(raw)

    station_rows: List[Dict] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fetch_station, stn, now_utc): stn for stn in STATIONS}
        for fut in as_completed(futures):
            station_rows.append(fut.result())
    order = {s: i for i, s in enumerate(STATIONS)}
    station_rows.sort(key=lambda x: order[x["id"]])

    station_recent = [r for r in station_rows if r["recent"]]
    svg = draw_svg(rass_points, station_recent, station_rows, rass_time, fname)
    CHART_PATH.write_text(svg)

    csv_lines = ["station,elev_m,temp_c,ob_time,age_min,provider,recent"]
    for row in station_rows:
        if row["elev_m"] is None:
            csv_lines.append("%s,,,,,%s,%s" % (row["id"], row["provider"], str(row["recent"]).lower()))
        else:
            csv_lines.append(
                "%s,%.2f,%.2f,%s,%.1f,%s,%s"
                % (
                    row["id"],
                    row["elev_m"],
                    row["temp_c"],
                    row["ob_time"],
                    row["age_min"],
                    row["provider"],
                    str(row["recent"]).lower(),
                )
            )
    CSV_PATH.write_text("\n".join(csv_lines) + "\n")

    print("rass_file=%s" % fname)
    print("rass_time=%s" % str(rass_time))
    print("chart=%s" % str(CHART_PATH))
    print("station_csv=%s" % str(CSV_PATH))
    print("stations_recent=%d" % len(station_recent))
    for row in station_rows:
        if row["elev_m"] is None:
            print("%s: missing (%s)" % (row["id"], row["provider"]))
        else:
            print(
                "%s: temp_c=%.2f elev_m=%.2f ob_time=%s age_min=%.1f recent=%s"
                % (
                    row["id"],
                    row["temp_c"],
                    row["elev_m"],
                    row["ob_time"],
                    row["age_min"],
                    str(row["recent"]).lower(),
                )
            )


if __name__ == "__main__":
    main()
