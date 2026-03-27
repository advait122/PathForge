import json
from pathlib import Path
from threading import Lock
from urllib.request import Request, urlopen


_LOCATION_SOURCE_URL = (
    "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/"
    "master/json/countries+states+cities.json"
)
_CACHE_PATH = Path(__file__).resolve().parents[3] / ".cache" / "locations_world.json"
_DOWNLOAD_TIMEOUT_SECONDS = 35

_LOAD_LOCK = Lock()
_CATALOG: dict | None = None


def _normalize(value: str) -> str:
    return str(value or "").strip().lower()


def _safe_name(raw: object) -> str:
    return str(raw or "").strip()


def _filter_values(values: list[str], query: str, limit: int) -> list[str]:
    max_items = max(1, min(int(limit), 10000))
    normalized_query = _normalize(query)
    if not normalized_query:
        return values[:max_items]

    starts_with: list[str] = []
    contains: list[str] = []
    for item in values:
        normalized_item = _normalize(item)
        if normalized_item.startswith(normalized_query):
            starts_with.append(item)
        elif normalized_query in normalized_item:
            contains.append(item)

    return (starts_with + contains)[:max_items]


def _download_catalog_payload() -> list[dict]:
    request = Request(_LOCATION_SOURCE_URL, headers={"User-Agent": "CodeMap/1.0"})
    with urlopen(request, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as response:
        raw_bytes = response.read()
    decoded = raw_bytes.decode("utf-8")
    loaded = json.loads(decoded)
    if not isinstance(loaded, list):
        raise ValueError("Unexpected locations dataset format.")
    return loaded


def _load_catalog_payload() -> list[dict]:
    if _CACHE_PATH.exists():
        try:
            with _CACHE_PATH.open("r", encoding="utf-8") as cache_file:
                cached = json.load(cache_file)
            if isinstance(cached, list) and cached:
                return cached
        except Exception:
            pass

    downloaded = _download_catalog_payload()
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _CACHE_PATH.open("w", encoding="utf-8") as cache_file:
            json.dump(downloaded, cache_file, ensure_ascii=False)
    except Exception:
        # Cache write failure should not block runtime behavior.
        pass
    return downloaded


def _build_catalog(source_rows: list[dict]) -> dict:
    countries: list[str] = []
    country_lookup: dict[str, str] = {}
    states_by_country: dict[str, list[str]] = {}
    state_lookup_by_country: dict[str, dict[str, str]] = {}
    cities_by_country_state: dict[str, dict[str, list[str]]] = {}

    for country_row in source_rows:
        if not isinstance(country_row, dict):
            continue
        country_name = _safe_name(country_row.get("name"))
        if not country_name:
            continue

        country_key = _normalize(country_name)
        if country_key in country_lookup:
            continue

        country_lookup[country_key] = country_name
        countries.append(country_name)

        states_raw = country_row.get("states") or []
        if not isinstance(states_raw, list):
            states_raw = []

        states_for_country: list[str] = []
        state_lookup: dict[str, str] = {}
        city_map: dict[str, list[str]] = {}

        for state_row in states_raw:
            if not isinstance(state_row, dict):
                continue
            state_name = _safe_name(state_row.get("name"))
            if not state_name:
                continue

            state_key = _normalize(state_name)
            if state_key in state_lookup:
                continue

            state_lookup[state_key] = state_name
            states_for_country.append(state_name)

            cities_raw = state_row.get("cities") or []
            if not isinstance(cities_raw, list):
                cities_raw = []

            city_names: list[str] = []
            city_seen: set[str] = set()
            for city_row in cities_raw:
                if isinstance(city_row, dict):
                    city_name = _safe_name(city_row.get("name"))
                else:
                    city_name = _safe_name(city_row)
                if not city_name:
                    continue
                city_key = _normalize(city_name)
                if city_key in city_seen:
                    continue
                city_seen.add(city_key)
                city_names.append(city_name)

            city_names.sort(key=lambda value: value.casefold())
            city_map[state_name] = city_names

        states_for_country.sort(key=lambda value: value.casefold())
        states_by_country[country_name] = states_for_country
        state_lookup_by_country[country_name] = state_lookup
        cities_by_country_state[country_name] = city_map

    countries.sort(key=lambda value: value.casefold())

    return {
        "countries": countries,
        "country_lookup": country_lookup,
        "states_by_country": states_by_country,
        "state_lookup_by_country": state_lookup_by_country,
        "cities_by_country_state": cities_by_country_state,
    }


def _load_catalog() -> dict:
    global _CATALOG
    if _CATALOG is not None:
        return _CATALOG

    with _LOAD_LOCK:
        if _CATALOG is not None:
            return _CATALOG

        try:
            source_rows = _load_catalog_payload()
            _CATALOG = _build_catalog(source_rows)
        except Exception:
            _CATALOG = {
                "countries": [],
                "country_lookup": {},
                "states_by_country": {},
                "state_lookup_by_country": {},
                "cities_by_country_state": {},
            }
        return _CATALOG


def _resolve_country(catalog: dict, country_value: str) -> str:
    normalized_country = _normalize(country_value)
    if not normalized_country:
        return ""

    country_lookup: dict[str, str] = catalog["country_lookup"]
    exact = country_lookup.get(normalized_country)
    if exact:
        return exact

    matching = [
        country_name
        for country_name in catalog["countries"]
        if _normalize(country_name).startswith(normalized_country)
    ]
    if len(matching) == 1:
        return matching[0]
    return ""


def _resolve_state(catalog: dict, country_name: str, state_value: str) -> str:
    normalized_state = _normalize(state_value)
    if not country_name or not normalized_state:
        return ""

    state_lookup = catalog["state_lookup_by_country"].get(country_name, {})
    exact = state_lookup.get(normalized_state)
    if exact:
        return exact

    states = catalog["states_by_country"].get(country_name, [])
    matching = [
        state_name
        for state_name in states
        if _normalize(state_name).startswith(normalized_state)
    ]
    if len(matching) == 1:
        return matching[0]
    return ""


def search_countries(*, q: str = "", limit: int = 500) -> list[str]:
    catalog = _load_catalog()
    return _filter_values(catalog["countries"], q, limit)


def search_states(*, country: str, q: str = "", limit: int = 500) -> list[str]:
    catalog = _load_catalog()
    resolved_country = _resolve_country(catalog, country)
    if not resolved_country:
        return []
    states = catalog["states_by_country"].get(resolved_country, [])
    return _filter_values(states, q, limit)


def search_cities(*, country: str, state: str, q: str = "", limit: int = 500) -> list[str]:
    catalog = _load_catalog()
    resolved_country = _resolve_country(catalog, country)
    if not resolved_country:
        return []

    resolved_state = _resolve_state(catalog, resolved_country, state)
    if not resolved_state:
        return []

    cities = catalog["cities_by_country_state"].get(resolved_country, {}).get(resolved_state, [])
    return _filter_values(cities, q, limit)
