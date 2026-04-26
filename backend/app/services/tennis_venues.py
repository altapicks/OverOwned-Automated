"""
Tennis tournament venue coordinates for weather lookup.

Mapping is keyed by lowercased tournament-name *substring*. Lookup walks the
keys looking for a substring match against the tournament name SGO/our DB
exposes — so "Mutua Madrid Open" and "Madrid Open Masters 1000" both resolve
to the "madrid" entry.

Add new venues here as you encounter them. Weather refresh degrades gracefully
on unknown tournaments — they just don't show a weather widget.

is_indoor=True suppresses the outdoor weather display in favor of an "Indoor"
badge. Some venues have retractable roofs; we treat those as outdoor for now
and let the user infer roof closure from the conditions panel.

Coordinates target the actual stadium/grounds where Centre Court / Stadium
Court sits. AccuWeather will bind to the nearest weather station.

elevation_m is the venue altitude in meters above sea level. AccuWeather's
hourly endpoint doesn't return elevation, so we hard-code it. Used in the
homepage weather tile and (future) for engine adjustments to ace/games
projections — high-altitude venues like Madrid (667m) play significantly
faster, especially relevant for Bogotá (2640m) and Kitzbühel (760m) on tour.

surface is the canonical court surface for the venue: "hard", "clay",
"grass", or "carpet". Used by the homepage "Current Tournament" tile and
for CPI lookups. Some venues alternate (Stuttgart ATP grass / WTA clay) —
they get the most-common-recently choice; override per-tournament if needed.
"""
from typing import Optional


# Substring keys are matched in insertion order; order matters when one key
# is a substring of another (e.g., "indian wells" before "wells"). Don't
# alphabetize blindly.
TENNIS_VENUES = {
    # ─── Grand Slams ─────────────────────────────────────────────────────
    "australian open": {"lat": -37.8217, "lon": 144.9789, "name": "Melbourne Park",                 "tz": "Australia/Melbourne", "is_indoor": False, "elevation_m": 31, "surface": "hard"},
    "roland garros": {"lat":  48.8472, "lon":   2.2497, "name": "Stade Roland-Garros",            "tz": "Europe/Paris",        "is_indoor": False, "elevation_m": 35, "surface": "clay"},
    "french open": {"lat":  48.8472, "lon":   2.2497, "name": "Stade Roland-Garros",            "tz": "Europe/Paris",        "is_indoor": False, "elevation_m": 35, "surface": "clay"},
    "wimbledon": {"lat":  51.4338, "lon":  -0.2141, "name": "All England Club",               "tz": "Europe/London",       "is_indoor": False, "elevation_m": 45, "surface": "grass"},
    "us open": {"lat":  40.7499, "lon": -73.8456, "name": "USTA Billie Jean King NTC",      "tz": "America/New_York",    "is_indoor": False, "elevation_m": 4, "surface": "hard"},

    # ─── ATP Masters 1000 / WTA 1000 ─────────────────────────────────────
    "indian wells": {"lat":  33.7236, "lon": -116.3056, "name": "Indian Wells Tennis Garden",    "tz": "America/Los_Angeles", "is_indoor": False, "elevation_m": 43, "surface": "hard"},
    "miami open": {"lat":  25.9580, "lon":  -80.2389, "name": "Hard Rock Stadium",             "tz": "America/New_York",    "is_indoor": False, "elevation_m": 3, "surface": "hard"},
    "monte carlo": {"lat":  43.7475, "lon":   7.4350, "name": "Monte-Carlo Country Club",       "tz": "Europe/Monaco",       "is_indoor": False, "elevation_m": 51, "surface": "clay"},
    "madrid open": {"lat":  40.4380, "lon":  -3.6794, "name": "Caja Mágica",                    "tz": "Europe/Madrid",       "is_indoor": False, "elevation_m": 667, "surface": "clay"},
    "mutua madrid": {"lat":  40.4380, "lon":  -3.6794, "name": "Caja Mágica",                    "tz": "Europe/Madrid",       "is_indoor": False, "elevation_m": 667, "surface": "clay"},
    "italian open": {"lat":  41.9292, "lon":  12.4561, "name": "Foro Italico",                   "tz": "Europe/Rome",         "is_indoor": False, "elevation_m": 21, "surface": "clay"},
    "rome masters": {"lat":  41.9292, "lon":  12.4561, "name": "Foro Italico",                   "tz": "Europe/Rome",         "is_indoor": False, "elevation_m": 21, "surface": "clay"},
    "internazionali": {"lat":  41.9292, "lon":  12.4561, "name": "Foro Italico",                   "tz": "Europe/Rome",         "is_indoor": False, "elevation_m": 21, "surface": "clay"},
    "canadian open": {"lat":  43.6442, "lon": -79.4096, "name": "Sobeys Stadium / IGA Stadium",   "tz": "America/Toronto",     "is_indoor": False, "elevation_m": 76, "surface": "hard"},  # alternates Toronto / Montreal
    "national bank": {"lat":  43.6442, "lon": -79.4096, "name": "Sobeys Stadium / IGA Stadium",   "tz": "America/Toronto",     "is_indoor": False, "elevation_m": 76, "surface": "hard"},
    "rogers cup": {"lat":  43.6442, "lon": -79.4096, "name": "Sobeys Stadium / IGA Stadium",   "tz": "America/Toronto",     "is_indoor": False, "elevation_m": 76, "surface": "hard"},
    "cincinnati": {"lat":  39.1922, "lon": -84.4158, "name": "Lindner Family Tennis Center",   "tz": "America/New_York",    "is_indoor": False, "elevation_m": 155, "surface": "hard"},
    "western & southern": {"lat":  39.1922, "lon": -84.4158, "name": "Lindner Family Tennis Center", "tz": "America/New_York",    "is_indoor": False, "elevation_m": 155, "surface": "hard"},
    "shanghai masters": {"lat":  31.1462, "lon": 121.6029,  "name": "Qizhong Forest Sports City",    "tz": "Asia/Shanghai",       "is_indoor": False, "elevation_m": 4, "surface": "hard"},
    "rolex shanghai": {"lat":  31.1462, "lon": 121.6029,  "name": "Qizhong Forest Sports City",    "tz": "Asia/Shanghai",       "is_indoor": False, "elevation_m": 4, "surface": "hard"},
    "paris masters": {"lat":  48.8316, "lon":   2.3833, "name": "Accor Arena",                    "tz": "Europe/Paris",        "is_indoor": True, "elevation_m": 35, "surface": "hard"},
    "rolex paris": {"lat":  48.8316, "lon":   2.3833, "name": "Accor Arena",                    "tz": "Europe/Paris",        "is_indoor": True, "elevation_m": 35, "surface": "hard"},
    "doha": {"lat":  25.2641, "lon":  51.4396, "name": "Khalifa Tennis Complex",         "tz": "Asia/Qatar",          "is_indoor": False, "elevation_m": 10, "surface": "hard"},
    "qatar open": {"lat":  25.2641, "lon":  51.4396, "name": "Khalifa Tennis Complex",         "tz": "Asia/Qatar",          "is_indoor": False, "elevation_m": 10, "surface": "hard"},
    "dubai": {"lat":  25.2459, "lon":  55.3367, "name": "Aviation Club Tennis Centre",    "tz": "Asia/Dubai",          "is_indoor": False, "elevation_m": 5, "surface": "hard"},
    "china open": {"lat":  39.9920, "lon": 116.4673, "name": "National Tennis Center, Beijing","tz": "Asia/Shanghai",       "is_indoor": False, "elevation_m": 43, "surface": "hard"},
    "wuhan open": {"lat":  30.6014, "lon": 114.3043, "name": "Optics Valley Int'l Tennis Ctr", "tz": "Asia/Shanghai",       "is_indoor": False, "elevation_m": 37, "surface": "hard"},

    # ─── Year-end / mid-tier (ATP 500, WTA 500) ──────────────────────────
    "atp finals": {"lat":  45.0844, "lon":   7.6764, "name": "Inalpi Arena, Turin",            "tz": "Europe/Rome",         "is_indoor": True, "elevation_m": 239, "surface": "hard"},
    "nitto atp": {"lat":  45.0844, "lon":   7.6764, "name": "Inalpi Arena, Turin",            "tz": "Europe/Rome",         "is_indoor": True, "elevation_m": 239, "surface": "hard"},
    "wta finals": {"lat":  24.7136, "lon":  46.6753, "name": "King Saud University, Riyadh",   "tz": "Asia/Riyadh",         "is_indoor": True, "elevation_m": 612, "surface": "hard"},  # 2024-26
    "next gen": {"lat":  24.4539, "lon":  54.3773, "name": "Etihad Arena, Abu Dhabi",        "tz": "Asia/Dubai",          "is_indoor": True, "elevation_m": 3, "surface": "hard"},
    "rotterdam": {"lat":  51.9244, "lon":   4.4912, "name": "Rotterdam Ahoy",                 "tz": "Europe/Amsterdam",    "is_indoor": True, "elevation_m": -2, "surface": "hard"},
    "rio open": {"lat": -22.9750, "lon": -43.2185, "name": "Jockey Club Brasileiro",         "tz": "America/Sao_Paulo",   "is_indoor": False, "elevation_m": 4, "surface": "clay"},
    "acapulco": {"lat":  16.8531, "lon": -99.8237, "name": "Princess Mundo Imperial",        "tz": "America/Mexico_City", "is_indoor": False, "elevation_m": 3, "surface": "hard"},
    "mexican open": {"lat":  16.8531, "lon": -99.8237, "name": "Princess Mundo Imperial",        "tz": "America/Mexico_City", "is_indoor": False, "elevation_m": 3, "surface": "hard"},
    "barcelona open": {"lat":  41.3954, "lon":   2.1217, "name": "Real Club de Tenis Barcelona",   "tz": "Europe/Madrid",       "is_indoor": False, "elevation_m": 12, "surface": "clay"},
    "godo": {"lat":  41.3954, "lon":   2.1217, "name": "Real Club de Tenis Barcelona",   "tz": "Europe/Madrid",       "is_indoor": False, "elevation_m": 12, "surface": "clay"},
    "queens": {"lat":  51.4877, "lon":  -0.2128, "name": "Queen's Club, London",           "tz": "Europe/London",       "is_indoor": False, "elevation_m": 15, "surface": "grass"},
    "queen's club": {"lat":  51.4877, "lon":  -0.2128, "name": "Queen's Club, London",           "tz": "Europe/London",       "is_indoor": False, "elevation_m": 15, "surface": "grass"},
    "halle": {"lat":  52.0467, "lon":   8.4517, "name": "OWL Arena, Halle",               "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 105, "surface": "grass"},
    "terra wortmann": {"lat":  52.0467, "lon":   8.4517, "name": "OWL Arena, Halle",               "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 105, "surface": "grass"},
    "hamburg": {"lat":  53.5634, "lon":   9.9789, "name": "Rothenbaum Tennis Stadion",      "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 6, "surface": "clay"},
    "washington": {"lat":  38.9069, "lon": -77.0419, "name": "Rock Creek Park Tennis Center",  "tz": "America/New_York",    "is_indoor": False, "elevation_m": 45, "surface": "hard"},
    "citi open": {"lat":  38.9069, "lon": -77.0419, "name": "Rock Creek Park Tennis Center",  "tz": "America/New_York",    "is_indoor": False, "elevation_m": 45, "surface": "hard"},
    "tokyo": {"lat":  35.6645, "lon": 139.7188, "name": "Ariake Coliseum",                "tz": "Asia/Tokyo",          "is_indoor": False, "elevation_m": 3, "surface": "hard"},
    "japan open": {"lat":  35.6645, "lon": 139.7188, "name": "Ariake Coliseum",                "tz": "Asia/Tokyo",          "is_indoor": False, "elevation_m": 3, "surface": "hard"},
    "vienna": {"lat":  48.2233, "lon":  16.3825, "name": "Wiener Stadthalle",              "tz": "Europe/Vienna",       "is_indoor": True, "elevation_m": 171, "surface": "hard"},
    "erste bank": {"lat":  48.2233, "lon":  16.3825, "name": "Wiener Stadthalle",              "tz": "Europe/Vienna",       "is_indoor": True, "elevation_m": 171, "surface": "hard"},
    "basel": {"lat":  47.5418, "lon":   7.5479, "name": "St. Jakobshalle Basel",          "tz": "Europe/Zurich",       "is_indoor": True, "elevation_m": 260, "surface": "hard"},
    "stuttgart": {"lat":  48.7898, "lon":   9.2280, "name": "TC Weissenhof",                  "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 245, "surface": "grass"},  # ATP grass / WTA clay
    "charleston": {"lat":  32.7833, "lon": -79.9319, "name": "Credit One Stadium",             "tz": "America/New_York",    "is_indoor": False, "elevation_m": 3, "surface": "clay"},
    "credit one": {"lat":  32.7833, "lon": -79.9319, "name": "Credit One Stadium",             "tz": "America/New_York",    "is_indoor": False, "elevation_m": 3, "surface": "clay"},

    # ─── ATP 250 / WTA 250 stops (most common) ───────────────────────────
    "adelaide": {"lat": -34.9214, "lon": 138.5973, "name": "Memorial Drive Tennis Centre",   "tz": "Australia/Adelaide",  "is_indoor": False, "elevation_m": 50, "surface": "hard"},
    "auckland": {"lat": -36.8654, "lon": 174.7726, "name": "ASB Tennis Centre",              "tz": "Pacific/Auckland",    "is_indoor": False, "elevation_m": 24, "surface": "hard"},
    "brisbane": {"lat": -27.4747, "lon": 153.0247, "name": "Pat Rafter Arena",               "tz": "Australia/Brisbane",  "is_indoor": False, "elevation_m": 28, "surface": "hard"},
    "hong kong": {"lat":  22.3149, "lon": 114.1923, "name": "Victoria Park Tennis Stadium",   "tz": "Asia/Hong_Kong",      "is_indoor": False, "elevation_m": 10, "surface": "hard"},
    "marseille": {"lat":  43.2706, "lon":   5.3955, "name": "Palais des Sports",              "tz": "Europe/Paris",        "is_indoor": True, "elevation_m": 27, "surface": "hard"},
    "buenos aires": {"lat": -34.5734, "lon": -58.4053, "name": "Buenos Aires Lawn Tennis Club",  "tz": "America/Argentina/Buenos_Aires", "is_indoor": False, "elevation_m": 25, "surface": "clay"},
    "santiago": {"lat": -33.4128, "lon": -70.6065, "name": "Estadio San Carlos de Apoquindo","tz": "America/Santiago",    "is_indoor": False, "elevation_m": 570, "surface": "clay"},
    "delray beach": {"lat":  26.4615, "lon": -80.0728, "name": "Delray Beach Stadium",           "tz": "America/New_York",    "is_indoor": False, "elevation_m": 5, "surface": "hard"},
    "los cabos": {"lat":  22.8909, "lon": -109.9124, "name": "Mexican Tennis Open Cabos",     "tz": "America/Mazatlan",    "is_indoor": False, "elevation_m": 15, "surface": "hard"},
    "newport": {"lat":  41.4862, "lon": -71.3052, "name": "Int'l Tennis Hall of Fame",      "tz": "America/New_York",    "is_indoor": False, "elevation_m": 15, "surface": "grass"},
    "atlanta": {"lat":  33.7912, "lon": -84.4084, "name": "Atlantic Station",               "tz": "America/New_York",    "is_indoor": False, "elevation_m": 308, "surface": "hard"},
    "winston-salem": {"lat":  36.1300, "lon": -80.2737, "name": "Wake Forest Tennis Center",      "tz": "America/New_York",    "is_indoor": False, "elevation_m": 271, "surface": "hard"},
    "estoril": {"lat":  38.7019, "lon":  -9.4128, "name": "Clube de Ténis do Estoril",      "tz": "Europe/Lisbon",       "is_indoor": False, "elevation_m": 50, "surface": "clay"},
    "munich": {"lat":  48.1571, "lon":  11.6181, "name": "MTTC Iphitos",                   "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 520, "surface": "clay"},
    "bmw open": {"lat":  48.1571, "lon":  11.6181, "name": "MTTC Iphitos",                   "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 520, "surface": "clay"},
    "geneva": {"lat":  46.2083, "lon":   6.1416, "name": "Tennis Club de Genève",          "tz": "Europe/Zurich",       "is_indoor": False, "elevation_m": 375, "surface": "clay"},
    "lyon": {"lat":  45.7775, "lon":   4.8408, "name": "Parc de la Tête d'Or",           "tz": "Europe/Paris",        "is_indoor": False, "elevation_m": 170, "surface": "clay"},
    "kitzbuhel": {"lat":  47.4474, "lon":  12.3925, "name": "Kitzbüheler Tennis Club",        "tz": "Europe/Vienna",       "is_indoor": False, "elevation_m": 760, "surface": "clay"},
    "bastad": {"lat":  56.4253, "lon":  12.8511, "name": "Båstad Tennis Stadion",          "tz": "Europe/Stockholm",    "is_indoor": False, "elevation_m": 5, "surface": "clay"},
    "umag": {"lat":  45.4350, "lon":  13.5256, "name": "ATP Stadium Goran Ivaniševic",   "tz": "Europe/Zagreb",       "is_indoor": False, "elevation_m": 5, "surface": "clay"},
    "metz": {"lat":  49.1193, "lon":   6.1757, "name": "Les Arènes de Metz",             "tz": "Europe/Paris",        "is_indoor": True, "elevation_m": 192, "surface": "hard"},
    "antwerp": {"lat":  51.2255, "lon":   4.4218, "name": "Lotto Arena",                    "tz": "Europe/Brussels",     "is_indoor": True, "elevation_m": 8, "surface": "hard"},
    "stockholm": {"lat":  59.3413, "lon":  18.0788, "name": "Royal Tennis Hall",              "tz": "Europe/Stockholm",    "is_indoor": True, "elevation_m": 28, "surface": "hard"},

    # ─── WTA-specific stops ──────────────────────────────────────────────
    "abu dhabi": {"lat":  24.4539, "lon":  54.3773, "name": "Etihad Arena, Abu Dhabi",        "tz": "Asia/Dubai",          "is_indoor": True, "elevation_m": 15, "surface": "hard"},
    "merida": {"lat":  20.9670, "lon": -89.5926, "name": "Yucatan Country Club",           "tz": "America/Merida",      "is_indoor": False, "elevation_m": 10, "surface": "hard"},
    "guadalajara": {"lat":  20.6597, "lon":-103.3496, "name": "Pan American Tennis Center",     "tz": "America/Mexico_City", "is_indoor": False, "elevation_m": 1566, "surface": "hard"},
    "san diego": {"lat":  32.7770, "lon":-117.2073, "name": "Barnes Tennis Center",           "tz": "America/Los_Angeles", "is_indoor": False, "elevation_m": 15, "surface": "hard"},
    "monterrey": {"lat":  25.6516, "lon":-100.2895, "name": "Sierra Madre Tennis Club",       "tz": "America/Monterrey",   "is_indoor": False, "elevation_m": 540, "surface": "hard"},
    "bogota": {"lat":   4.6486, "lon": -74.0660, "name": "Centro de Alto Rendimiento",     "tz": "America/Bogota",      "is_indoor": False, "elevation_m": 2640, "surface": "clay"},
    "cluj": {"lat":  46.7657, "lon":  23.6038, "name": "BT Arena, Cluj",                 "tz": "Europe/Bucharest",    "is_indoor": True, "elevation_m": 340, "surface": "hard"},
    "tallinn": {"lat":  59.4225, "lon":  24.7553, "name": "Tondiraba Ice Hall",             "tz": "Europe/Tallinn",      "is_indoor": True, "elevation_m": 10, "surface": "hard"},
    "transylvania": {"lat":  46.7657, "lon":  23.6038, "name": "BT Arena, Cluj",                 "tz": "Europe/Bucharest",    "is_indoor": True, "elevation_m": 340, "surface": "hard"},
    "linz": {"lat":  48.3069, "lon":  14.2858, "name": "TipsArena Linz",                 "tz": "Europe/Vienna",       "is_indoor": True, "elevation_m": 266, "surface": "hard"},
    "ningbo": {"lat":  29.8683, "lon": 121.5440, "name": "Ningbo Olympic Sports Center",   "tz": "Asia/Shanghai",       "is_indoor": False, "elevation_m": 10, "surface": "hard"},
    "chennai": {"lat":  13.0827, "lon":  80.2707, "name": "SDAT Tennis Stadium",            "tz": "Asia/Kolkata",        "is_indoor": False, "elevation_m": 6, "surface": "hard"},
    "seoul": {"lat":  37.5189, "lon": 127.1219, "name": "Olympic Park Tennis Center",     "tz": "Asia/Seoul",          "is_indoor": False, "elevation_m": 38, "surface": "hard"},
    "luxembourg": {"lat":  49.6116, "lon":   6.1319, "name": "CK Sportcenter Kockelscheuer",   "tz": "Europe/Luxembourg",   "is_indoor": True, "elevation_m": 300, "surface": "hard"},
    "bad homburg": {"lat":  50.2272, "lon":   8.6101, "name": "Tennis-Club Bad Homburg",        "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 190, "surface": "grass"},
    "berlin": {"lat":  52.4661, "lon":  13.2628, "name": "LTTC Rot-Weiß Berlin",           "tz": "Europe/Berlin",       "is_indoor": False, "elevation_m": 50, "surface": "grass"},
    "eastbourne": {"lat":  50.7635, "lon":   0.2876, "name": "Devonshire Park",                "tz": "Europe/London",       "is_indoor": False, "elevation_m": 8, "surface": "grass"},
    "rabat": {"lat":  34.0209, "lon":  -6.8416, "name": "ROCT Tennis Complex",            "tz": "Africa/Casablanca",   "is_indoor": False, "elevation_m": 75, "surface": "clay"},
    "marrakech": {"lat":  31.6295, "lon":  -7.9811, "name": "Royal Tennis Club de Marrakech", "tz": "Africa/Casablanca",   "is_indoor": False, "elevation_m": 467, "surface": "clay"},
}


def lookup_venue(tournament_name: Optional[str]) -> Optional[dict]:
    """Find a venue by tournament name. Substring match, case-insensitive.

    Walks keys in declaration order so more specific entries win when one is
    a substring of another (e.g., "indian wells" before "wells"). Returns None
    if no key matches — caller should treat as "weather unavailable" and skip.
    """
    if not tournament_name or not isinstance(tournament_name, str):
        return None
    name_lower = tournament_name.lower().strip()
    if not name_lower:
        return None
    for key, venue in TENNIS_VENUES.items():
        if key in name_lower:
            return {**venue, "_matched_key": key}
    return None
