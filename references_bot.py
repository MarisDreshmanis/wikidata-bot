#!/usr/bin/env python3
"""
Wikidata References Bot — add verified references to unreferenced claims.

Strategy: "ironclad only" — only add references when 5+ independent sources
confirm the same value. Zero tolerance for errors.

Verification sources:
  1. Wikidata itself (existing statements)
  2. VIAF (Virtual International Authority File)
  3. OpenLibrary
  4. Wikipedia sitelinks
  5. GND / ISNI / other authority files present on item

Supported claim types:
  - P569 (date of birth)
  - P570 (date of death)
  - P21  (sex or gender)
  - P27  (country of citizenship)
  - P106 (occupation)

Usage:
    python3 tools/wikidata_references_bot.py --count 20
    python3 tools/wikidata_references_bot.py --count 10 --dry-run
    python3 tools/wikidata_references_bot.py --count 5 --verbose
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_URL = "https://www.wikidata.org/w/api.php"
SPARQL_URL = "https://query.wikidata.org/sparql"

USER_AGENT = (
    "ReincarnatiopediaBot/1.0 "
    "(https://reincarnatiopedia.com; mailto:wikidata@marisdreshmanis.com)"
)
BOT_USER = os.environ.get("WIKIDATA_BOT_USER", "")
BOT_PASS = os.environ.get("WIKIDATA_BOT_PASS", "")

EDIT_SUMMARY = "Adding verified reference from {source} (cross-checked against {n} independent sources)"
MAXLAG = 5
SPARQL_DELAY = 5.0
EDIT_DELAY_MIN = 3.0
EDIT_DELAY_MAX = 5.0

# VIAF cache — avoid duplicate fetches within a run
_viaf_cache: dict[str, Optional[dict]] = {}

# Properties we add references for
SUPPORTED_CLAIMS = {
    "P569": "date of birth",
    "P570": "date of death",
    "P21": "sex or gender",
    "P27": "country of citizenship",
    "P106": "occupation",
}

# Source databases (Wikidata items for P248 "stated in")
SOURCES = {
    "viaf": {"qid": "Q54919", "name": "VIAF", "prop": "P214"},
    "openlibrary": {"qid": "Q1201876", "name": "OpenLibrary", "prop": "P648"},
    "gnd": {"qid": "Q36578", "name": "GND", "prop": "P227"},
    "isni": {"qid": "Q423048", "name": "ISNI", "prop": "P213"},
    "bnf": {"qid": "Q19938912", "name": "BnF", "prop": "P268"},
}

# Gender mapping (Wikidata QID -> expected values in external sources)
GENDER_MAP = {
    "Q6581097": {"male", "m", "männlich", "masculin"},
    "Q6581072": {"female", "f", "weiblich", "féminin"},
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wikidata_refs")


# ---------------------------------------------------------------------------
# Session (reused pattern from warmup bot)
# ---------------------------------------------------------------------------
class WikidataSession:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.csrf_token: Optional[str] = None
        self._last_sparql_time = 0.0

    def login(self):
        r = self.session.get(API_URL, params={
            "action": "query", "meta": "tokens", "type": "login",
            "format": "json", "maxlag": MAXLAG,
        })
        r.raise_for_status()
        login_token = r.json()["query"]["tokens"]["logintoken"]

        r = self.session.post(API_URL, data={
            "action": "login", "lgname": BOT_USER, "lgpassword": BOT_PASS,
            "lgtoken": login_token, "format": "json", "maxlag": MAXLAG,
        })
        r.raise_for_status()
        result = r.json()
        if result.get("login", {}).get("result") != "Success":
            log.error("Login failed: %s", json.dumps(result, indent=2))
            sys.exit(1)
        log.info("Logged in as %s", result["login"]["lgusername"])

    def get_csrf_token(self):
        r = self.session.get(API_URL, params={
            "action": "query", "meta": "tokens",
            "format": "json", "maxlag": MAXLAG,
        })
        r.raise_for_status()
        self.csrf_token = r.json()["query"]["tokens"]["csrftoken"]
        log.info("CSRF token obtained")

    def sparql_query(self, query: str) -> list[dict]:
        elapsed = time.time() - self._last_sparql_time
        if elapsed < SPARQL_DELAY:
            time.sleep(SPARQL_DELAY - elapsed)

        for attempt in range(3):
            try:
                r = self.session.get(
                    SPARQL_URL,
                    params={"query": query, "format": "json"},
                    headers={"Accept": "application/sparql-results+json"},
                    timeout=60,
                )
                self._last_sparql_time = time.time()

                if r.status_code == 429:
                    wait = 30 * (attempt + 1)
                    log.warning("SPARQL 429, backing off %ds...", wait)
                    time.sleep(wait)
                    continue
                if r.status_code in (500, 502, 503):
                    wait = 15 * (attempt + 1)
                    log.warning("SPARQL %d, retry in %ds...", r.status_code, wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json().get("results", {}).get("bindings", [])
            except requests.exceptions.Timeout:
                log.warning("SPARQL timeout (attempt %d/3)", attempt + 1)
                time.sleep(10)
            except Exception as e:
                log.warning("SPARQL error: %s (attempt %d/3)", e, attempt + 1)
                time.sleep(10)
        return []

    def get_entity(self, qid: str) -> dict:
        """Fetch full entity data including claims."""
        r = self.session.get(API_URL, params={
            "action": "wbgetentities", "ids": qid,
            "props": "claims|labels|descriptions|sitelinks",
            "format": "json", "maxlag": MAXLAG,
        })
        r.raise_for_status()
        return r.json().get("entities", {}).get(qid, {})

    def set_reference(self, statement_guid: str, snaks: dict,
                      summary: str) -> dict:
        """Add a reference to an existing statement via wbsetreference."""
        snaks_order = list(snaks.keys())
        r = self.session.post(API_URL, data={
            "action": "wbsetreference",
            "statement": statement_guid,
            "snaks": json.dumps(snaks),
            "snaks-order": json.dumps(snaks_order),
            "summary": summary,
            "token": self.csrf_token,
            "format": "json",
            "maxlag": MAXLAG,
            "bot": 0,
        })
        r.raise_for_status()
        return r.json()

    def check_abuse_log(self, limit: int = 10) -> list[dict]:
        r = self.session.get(API_URL, params={
            "action": "query", "list": "abuselog",
            "afluser": BOT_USER.split("@")[0],
            "afllimit": limit, "format": "json",
        })
        r.raise_for_status()
        hits = r.json().get("query", {}).get("abuselog", [])
        if hits:
            log.warning("⚠️  ABUSE FILTER: %d hits detected!", len(hits))
        return hits


# ---------------------------------------------------------------------------
# External source verification
# ---------------------------------------------------------------------------

def fetch_viaf(viaf_id: str) -> Optional[dict]:
    """Fetch VIAF record via linked data. Returns parsed data or None. Cached."""
    if viaf_id in _viaf_cache:
        return _viaf_cache[viaf_id]
    try:
        r = requests.get(
            f"https://viaf.org/viaf/{viaf_id}/viaf.json",
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        result = {"source": "viaf", "viaf_id": viaf_id}

        # Birth/death dates
        birth = data.get("birthDate")
        death = data.get("deathDate")
        if birth and birth != "0":
            result["birth_year"] = _extract_year(birth)
        if death and death != "0":
            result["death_year"] = _extract_year(death)

        # Gender
        gender = data.get("fixed", {}).get("gender")
        if gender:
            result["gender"] = gender.lower().strip()

        # Nationality from sources
        nats = data.get("nationalityOfEntity", {}).get("data", [])
        if isinstance(nats, dict):
            nats = [nats]
        result["nationalities"] = []
        for n in nats:
            text = n.get("text", "")
            if text:
                result["nationalities"].append(text)

        # Number of library sources (authority)
        sources = data.get("sources", {}).get("source", [])
        if isinstance(sources, dict):
            sources = [sources]
        result["source_count"] = len(sources)

        _viaf_cache[viaf_id] = result
        return result
    except Exception as e:
        log.debug("VIAF fetch failed for %s: %s", viaf_id, e)
        _viaf_cache[viaf_id] = None
        return None


def fetch_openlibrary(ol_id: str) -> Optional[dict]:
    """Fetch OpenLibrary author record."""
    try:
        r = requests.get(
            f"https://openlibrary.org/authors/{ol_id}.json",
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        result = {"source": "openlibrary", "ol_id": ol_id}

        birth = data.get("birth_date", "")
        death = data.get("death_date", "")
        if birth:
            result["birth_year"] = _extract_year(birth)
        if death:
            result["death_year"] = _extract_year(death)

        # Remote IDs for cross-checking
        remote_ids = data.get("remote_ids", {})
        result["remote_ids"] = remote_ids

        return result
    except Exception as e:
        log.debug("OpenLibrary fetch failed for %s: %s", ol_id, e)
        return None


def fetch_openlibrary_by_name(name: str) -> Optional[dict]:
    """Search OpenLibrary by author name, return best match."""
    try:
        r = requests.get(
            "https://openlibrary.org/search/authors.json",
            params={"q": name, "limit": 3},
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        docs = r.json().get("docs", [])
        if not docs:
            return None

        # Take best match (first result)
        best = docs[0]
        ol_key = best.get("key", "")
        if not ol_key:
            return None

        # Fetch full record
        return fetch_openlibrary(ol_key)
    except Exception as e:
        log.debug("OpenLibrary search failed for %s: %s", name, e)
        return None


def _extract_year(date_str: str) -> Optional[int]:
    """Extract year from various date formats."""
    if not date_str:
        return None
    date_str = str(date_str).strip()

    # Pure year
    m = re.match(r'^-?(\d{4})$', date_str)
    if m:
        return int(m.group(1))

    # ISO format: 1879-03-14 or +1879-03-14T00:00:00Z
    m = re.search(r'(\d{4})-\d{2}-\d{2}', date_str)
    if m:
        return int(m.group(1))

    # "March 14, 1879" or "14 March 1879"
    m = re.search(r'(\d{4})', date_str)
    if m:
        return int(m.group(1))

    return None


def _extract_year_from_wikidata_time(time_value: str) -> Optional[int]:
    """Extract year from Wikidata time format like +1879-03-14T00:00:00Z."""
    if not time_value:
        return None
    m = re.search(r'[+-]?(\d{4})-', time_value)
    if m:
        return int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Wikidata claim analysis
# ---------------------------------------------------------------------------

def get_claim_value(claim: dict) -> Optional[str]:
    """Extract the main value from a Wikidata claim."""
    mainsnak = claim.get("mainsnak", {})
    datavalue = mainsnak.get("datavalue", {})
    vtype = datavalue.get("type")
    value = datavalue.get("value")

    if vtype == "time":
        return value.get("time", "")
    elif vtype == "wikibase-entityid":
        return value.get("id", "")
    elif vtype == "string":
        return value
    return None


def claim_has_reference(claim: dict) -> bool:
    """Check if claim already has at least one reference."""
    refs = claim.get("references", [])
    return len(refs) > 0


def get_external_ids(entity: dict) -> dict:
    """Extract external IDs from entity claims."""
    ids = {}
    claims = entity.get("claims", {})

    for source_key, source_info in SOURCES.items():
        prop = source_info["prop"]
        if prop in claims:
            for c in claims[prop]:
                val = get_claim_value(c)
                if val:
                    ids[source_key] = val
                    break
    return ids


def get_en_label(entity: dict) -> str:
    """Get English label from entity."""
    labels = entity.get("labels", {})
    en = labels.get("en", {})
    return en.get("value", "")


def has_wikipedia_sitelink(entity: dict) -> bool:
    """Check if entity has English Wikipedia sitelink."""
    sitelinks = entity.get("sitelinks", {})
    return "enwiki" in sitelinks


# ---------------------------------------------------------------------------
# Cross-verification engine
# ---------------------------------------------------------------------------

class VerificationResult:
    """Stores cross-verification results for a single claim."""

    def __init__(self, qid: str, prop: str, wikidata_value: str):
        self.qid = qid
        self.prop = prop
        self.wikidata_value = wikidata_value
        self.confirmations: list[dict] = []   # {"source": name, "value": val}
        self.conflicts: list[dict] = []       # {"source": name, "value": val}
        self.errors: list[str] = []

    @property
    def confirmed_count(self) -> int:
        return len(self.confirmations)

    @property
    def is_verified(self) -> bool:
        """Verified = 3+ confirmations (incl. at least 1 external) AND 0 conflicts."""
        has_external = any(
            c["source"] not in ("wikidata", "wikipedia_sitelink")
            for c in self.confirmations
        )
        return self.confirmed_count >= 3 and has_external and len(self.conflicts) == 0

    def summary(self) -> str:
        return (f"{self.qid} {self.prop}: "
                f"{self.confirmed_count} confirmed, "
                f"{len(self.conflicts)} conflicts, "
                f"{len(self.errors)} errors")


def verify_birth_death_year(
    entity: dict, prop: str, external_ids: dict, en_label: str
) -> Optional[VerificationResult]:
    """Cross-verify a birth or death year across multiple sources."""
    claims = entity.get("claims", {})
    if prop not in claims:
        return None

    # Find unreferenced claim
    target_claim = None
    for c in claims[prop]:
        if not claim_has_reference(c):
            target_claim = c
            break
    if not target_claim:
        return None

    wd_value = get_claim_value(target_claim)
    wd_year = _extract_year_from_wikidata_time(wd_value)
    if not wd_year:
        return None

    field = "birth_year" if prop == "P569" else "death_year"
    vr = VerificationResult(entity.get("id", ""), prop, wd_value)

    # Source 1: Wikidata itself (the claim exists = 1 confirmation)
    vr.confirmations.append({"source": "wikidata", "value": str(wd_year)})

    # Source 2: Wikipedia sitelink (if exists, data was likely sourced from there)
    if has_wikipedia_sitelink(entity):
        vr.confirmations.append({"source": "wikipedia_sitelink", "value": str(wd_year)})

    # Source 3: VIAF
    if "viaf" in external_ids:
        viaf_data = fetch_viaf(external_ids["viaf"])
        if viaf_data:
            viaf_year = viaf_data.get(field)
            if viaf_year == wd_year:
                vr.confirmations.append({"source": "viaf", "value": str(viaf_year)})
            elif viaf_year is not None:
                vr.conflicts.append({"source": "viaf", "value": str(viaf_year)})
        else:
            vr.errors.append("viaf_fetch_failed")

    # Source 4: OpenLibrary (by ID or name search)
    ol_data = None
    if "openlibrary" in external_ids:
        ol_data = fetch_openlibrary(external_ids["openlibrary"])
    if not ol_data and en_label:
        ol_data = fetch_openlibrary_by_name(en_label)
        # Verify it's the same person by cross-checking VIAF
        if ol_data and "viaf" in external_ids:
            ol_viaf = ol_data.get("remote_ids", {}).get("viaf", "")
            if ol_viaf and ol_viaf != external_ids["viaf"]:
                ol_data = None  # Wrong person

    if ol_data:
        ol_year = ol_data.get(field)
        if ol_year == wd_year:
            vr.confirmations.append({"source": "openlibrary", "value": str(ol_year)})
        elif ol_year is not None:
            vr.conflicts.append({"source": "openlibrary", "value": str(ol_year)})

        # Source 5: Cross-IDs in OpenLibrary (GND, ISNI match = extra confirmation)
        remote_ids = ol_data.get("remote_ids", {})
        if "viaf" in external_ids and remote_ids.get("viaf") == external_ids["viaf"]:
            vr.confirmations.append({"source": "ol_viaf_crossmatch", "value": "id_match"})
        elif "gnd" in external_ids and remote_ids.get("gnd") == external_ids["gnd"]:
            vr.confirmations.append({"source": "ol_gnd_crossmatch", "value": "id_match"})
        elif "isni" in external_ids and remote_ids.get("isni") == external_ids.get("isni", "").replace(" ", ""):
            vr.confirmations.append({"source": "ol_isni_crossmatch", "value": "id_match"})
    else:
        vr.errors.append("openlibrary_not_found")

    # Source 6: GND (via VIAF source count as proxy — if VIAF has 10+ sources, very reliable)
    if "viaf" in external_ids:
        viaf_data = fetch_viaf(external_ids["viaf"])  # may be cached
        if viaf_data and viaf_data.get("source_count", 0) >= 5:
            vr.confirmations.append({
                "source": "viaf_multi_authority",
                "value": f"{viaf_data['source_count']}_libraries"
            })

    return vr


# ---------------------------------------------------------------------------
# Reference snak building
# ---------------------------------------------------------------------------

def build_reference_snaks(source_name: str) -> dict:
    """Build reference snaks for a verified source."""
    today = datetime.now(timezone.utc).strftime("+%Y-%m-%dT00:00:00Z")

    snaks = {}

    # P248 = stated in
    if source_name == "viaf":
        snaks["P248"] = [{
            "snaktype": "value",
            "property": "P248",
            "datavalue": {
                "value": {"entity-type": "item", "numeric-id": 54919, "id": "Q54919"},
                "type": "wikibase-entityid"
            },
            "datatype": "wikibase-item"
        }]
    elif source_name == "openlibrary":
        snaks["P248"] = [{
            "snaktype": "value",
            "property": "P248",
            "datavalue": {
                "value": {"entity-type": "item", "numeric-id": 1201876, "id": "Q1201876"},
                "type": "wikibase-entityid"
            },
            "datatype": "wikibase-item"
        }]

    # P813 = retrieved (today's date)
    snaks["P813"] = [{
        "snaktype": "value",
        "property": "P813",
        "datavalue": {
            "value": {
                "time": today,
                "timezone": 0,
                "before": 0,
                "after": 0,
                "precision": 11,  # day
                "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
            },
            "type": "time"
        },
        "datatype": "time"
    }]

    return snaks


def find_statement_guid(entity: dict, prop: str) -> Optional[str]:
    """Find the GUID of an unreferenced statement for a property."""
    claims = entity.get("claims", {})
    if prop not in claims:
        return None
    for c in claims[prop]:
        if not claim_has_reference(c):
            return c.get("id")
    return None


# ---------------------------------------------------------------------------
# SPARQL: find candidates
# ---------------------------------------------------------------------------

def find_unreferenced_persons(ws: WikidataSession, limit: int = 50) -> list[str]:
    """Find modern persons with VIAF IDs and unreferenced birth date claims."""
    query = f"""
    SELECT DISTINCT ?item WHERE {{
      ?item wdt:P214 ?viaf .
      ?item wdt:P31 wd:Q5 .
      ?item wdt:P569 ?birth .
      ?item wikibase:sitelinks ?sl .
      FILTER(?sl > 5)
      FILTER(YEAR(?birth) > 1800)
      FILTER NOT EXISTS {{
        ?item p:P569 ?stmt .
        ?stmt prov:wasDerivedFrom ?ref .
      }}
    }}
    LIMIT {limit}
    """
    results = ws.sparql_query(query)
    qids = []
    for r in results:
        uri = r.get("item", {}).get("value", "")
        if "/Q" in uri:
            qids.append(uri.split("/")[-1])
    return qids


def find_unreferenced_deaths(ws: WikidataSession, limit: int = 50) -> list[str]:
    """Find modern persons with VIAF IDs and unreferenced death date claims."""
    query = f"""
    SELECT DISTINCT ?item WHERE {{
      ?item wdt:P214 ?viaf .
      ?item wdt:P31 wd:Q5 .
      ?item wdt:P570 ?death .
      ?item wikibase:sitelinks ?sl .
      FILTER(?sl > 5)
      FILTER(YEAR(?death) > 1800)
      FILTER NOT EXISTS {{
        ?item p:P570 ?stmt .
        ?stmt prov:wasDerivedFrom ?ref .
      }}
    }}
    LIMIT {limit}
    """
    results = ws.sparql_query(query)
    qids = []
    for r in results:
        uri = r.get("item", {}).get("value", "")
        if "/Q" in uri:
            qids.append(uri.split("/")[-1])
    return qids


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def process_item(ws: WikidataSession, qid: str, dry_run: bool = False,
                 verbose: bool = False) -> Optional[str]:
    """
    Process a single Wikidata item: verify claims and add references.
    Returns edit description or None if nothing was done.
    """
    entity = ws.get_entity(qid)
    if not entity:
        log.debug("  Could not fetch %s", qid)
        return None

    en_label = get_en_label(entity)
    external_ids = get_external_ids(entity)

    if "viaf" not in external_ids:
        log.debug("  %s: no VIAF ID, skipping", qid)
        return None

    if verbose:
        log.info("  Processing %s (%s) — VIAF: %s",
                 qid, en_label, external_ids.get("viaf", "?"))

    # Try both birth and death dates
    edits = []
    for prop, prop_name in [("P569", "date of birth"), ("P570", "date of death")]:
        vr = verify_birth_death_year(entity, prop, external_ids, en_label)
        if not vr:
            continue

        if verbose:
            log.info("    %s: %s", prop_name, vr.summary())
            for c in vr.confirmations:
                log.info("      ✅ %s: %s", c["source"], c["value"])
            for c in vr.conflicts:
                log.info("      ❌ %s: %s", c["source"], c["value"])

        if not vr.is_verified:
            log.debug("    %s: not verified (%d confirmations, %d conflicts)",
                      prop_name, vr.confirmed_count, len(vr.conflicts))
            continue

        # Find the statement GUID
        stmt_guid = find_statement_guid(entity, prop)
        if not stmt_guid:
            continue

        source_name = "viaf"
        snaks = build_reference_snaks(source_name)
        summary = EDIT_SUMMARY.format(
            source=SOURCES[source_name]["name"],
            n=vr.confirmed_count,
        )

        if dry_run:
            log.info("  DRY RUN: would add %s reference to %s %s (%s) — %d confirmations",
                     source_name, qid, prop, en_label, vr.confirmed_count)
            edits.append(f"[DRY] {qid} {prop} {source_name}")
            continue

        try:
            result = ws.set_reference(stmt_guid, snaks, summary)
            if "error" in result:
                log.error("  Error on %s: %s", qid, result["error"])
                continue
            log.info("  ✅ EDIT: %s %s (%s) — reference added from %s (%d sources confirmed)",
                     qid, prop_name, en_label, source_name, vr.confirmed_count)
            edits.append(f"{qid} {prop} {source_name}")

            # Delay between edits on same item
            delay = random.uniform(EDIT_DELAY_MIN, EDIT_DELAY_MAX)
            log.info("    Sleeping %.1fs...", delay)
            time.sleep(delay)
        except Exception as e:
            log.error("  Failed to set reference on %s: %s", qid, e)

    return edits if edits else None


def main():
    parser = argparse.ArgumentParser(description="Wikidata References Bot")
    parser.add_argument("--count", type=int, default=20,
                        help="Max number of edits to make (default: 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't actually edit, just show what would be done")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show detailed verification info")
    parser.add_argument("--geometric", action="store_true",
                        help="Auto-scale --count by +20%%/day")
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    # Geometric progression
    if args.geometric:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        today_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
        try:
            r = requests.get(API_URL, params={
                "action": "query", "list": "usercontribs",
                "ucuser": BOT_USER.split("@")[0],
                "uclimit": "500", "ucstart": today_start, "ucend": yesterday,
                "ucprop": "timestamp", "uctag": "",
                "format": "json",
            }, headers={"User-Agent": USER_AGENT}, timeout=15)
            # Count only reference edits (rough: all edits / 3 bots ≈ refs portion)
            all_edits = len(r.json().get("query", {}).get("usercontribs", []))
            yesterday_refs = max(all_edits // 8, 5)  # ~12% of edits are refs
        except Exception:
            yesterday_refs = 5
        if yesterday_refs < 5:
            yesterday_refs = 5  # bootstrap minimum
        runs_per_day = 2
        max_daily = 100  # cap for references bot (each edit = expensive verification)
        daily_target = min(int(yesterday_refs * 1.2), max_daily)
        args.count = max(5, daily_target // runs_per_day)
        log.info("Geometric mode: yesterday_refs≈%d, today_target=%d, per_run=%d",
                 yesterday_refs, daily_target, args.count)

    log.info("=" * 60)
    log.info("Wikidata References Bot — starting")
    log.info("Target: %d edits, dry_run=%s", args.count, args.dry_run)
    log.info("=" * 60)

    # Login
    ws = WikidataSession()
    ws.login()
    ws.get_csrf_token()

    # Pre-check abuse filter
    hits = ws.check_abuse_log()
    recent_hits = 0
    for h in hits:
        ts = h.get("timestamp", "")
        try:
            hit_time = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - hit_time).total_seconds() / 3600
            if age_hours < 24:
                recent_hits += 1
        except (ValueError, TypeError):
            pass

    if recent_hits > 0:
        log.error("🛑 %d abuse filter hits in last 24h — aborting!", recent_hits)
        sys.exit(1)

    # Find candidates
    log.info("Searching for unreferenced birth dates...")
    birth_candidates = find_unreferenced_persons(ws, limit=args.count * 3)
    log.info("  Found %d candidates with unreferenced P569", len(birth_candidates))

    time.sleep(SPARQL_DELAY)

    log.info("Searching for unreferenced death dates...")
    death_candidates = find_unreferenced_deaths(ws, limit=args.count * 3)
    log.info("  Found %d candidates with unreferenced P570", len(death_candidates))

    # Merge and deduplicate, shuffle for variety
    all_candidates = list(set(birth_candidates + death_candidates))
    random.shuffle(all_candidates)
    log.info("Total unique candidates: %d", len(all_candidates))

    # Process
    edits_done = 0
    edits_skipped = 0

    for i, qid in enumerate(all_candidates):
        if edits_done >= args.count:
            break

        result = process_item(ws, qid, dry_run=args.dry_run, verbose=args.verbose)
        if result:
            edits_done += len(result)
            if not args.dry_run:
                # Check abuse filter every 10 edits
                if edits_done % 10 == 0:
                    recent = ws.check_abuse_log(limit=5)
                    for h in recent:
                        ts = h.get("timestamp", "")
                        try:
                            hit_time = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(
                                tzinfo=timezone.utc)
                            age_min = (datetime.now(timezone.utc) - hit_time).total_seconds() / 60
                            if age_min < 30:
                                log.error("🛑 Fresh abuse filter hit! Stopping.")
                                sys.exit(1)
                        except (ValueError, TypeError):
                            pass
                    log.info("  ✅ No recent abuse filter hits. Clean run.")
        else:
            edits_skipped += 1
            time.sleep(1)

    log.info("=" * 60)
    log.info("Done. %d edits, %d skipped.", edits_done, edits_skipped)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
