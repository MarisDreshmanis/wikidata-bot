#!/usr/bin/env python3
"""
Wikidata Latvian Bot — add missing Latvian (lv) labels and descriptions.
Designed to run in PAWS (JupyterLab on Wikimedia servers).

A Latvian citizen contributing Latvian translations to Wikidata.

Usage in PAWS notebook:
    %run wikidata_latvian_bot.py --count 20 --dry-run
    %run wikidata_latvian_bot.py --count 20

Usage on server:
    python3 tools/wikidata_latvian_bot.py --count 20 --lang lv --dry-run
"""

import argparse
import json
import logging
import os
import random
import sys
import time
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
EDIT_SUMMARY = "Adding missing Latvian labels and descriptions"
MAXLAG = 5
SPARQL_DELAY = 5.0

# ---------------------------------------------------------------------------
# Latvian translations — hand-curated, high quality
# ---------------------------------------------------------------------------

# Labels: EN -> LV (exact item names)
LATVIAN_LABELS = {
    # AI & ML
    "artificial intelligence": "mākslīgais intelekts",
    "machine learning": "mašīnmācīšanās",
    "deep learning": "dziļā mācīšanās",
    "neural network": "neironu tīkls",
    "natural language processing": "dabiskās valodas apstrāde",
    "computer vision": "datorredze",
    "reinforcement learning": "pastiprinātā mācīšanās",
    "supervised learning": "mācīšanās ar uzraudzību",
    "unsupervised learning": "mācīšanās bez uzraudzības",
    "speech recognition": "runas atpazīšana",
    "image recognition": "attēlu atpazīšana",
    "data mining": "datu ieguve",
    "big data": "lielie dati",
    "robotics": "robotika",
    "expert system": "ekspertu sistēma",
    "genetic algorithm": "ģenētiskais algoritms",
    "decision tree": "lēmumu koks",
    "random forest": "nejaušais mežs",
    "support vector machine": "atbalsta vektoru mašīna",
    "gradient descent": "gradienta nolaišanās",
    "convolutional neural network": "konvolūcijas neironu tīkls",
    "recurrent neural network": "rekurentais neironu tīkls",
    "Bayesian network": "Bejesa tīkls",
    "knowledge representation": "zināšanu reprezentācija",
    "sentiment analysis": "sentimenta analīze",
    "chatbot": "tērzēšanas robots",
    "perceptron": "perceptrons",
    "Turing test": "Tjūringa tests",
    # CS fundamentals
    "algorithm": "algoritms",
    "programming language": "programmēšanas valoda",
    "software": "programmatūra",
    "database": "datubāze",
    "operating system": "operētājsistēma",
    "compiler": "kompilators",
    "computer program": "datorprogramma",
    "computer science": "datorzinātne",
    "information technology": "informācijas tehnoloģija",
    "cryptography": "kriptogrāfija",
    "cybersecurity": "kiberdrošība",
    "data structure": "datu struktūra",
    "computer network": "datortīkls",
    "cloud computing": "mākoņdatošana",
    "Internet of things": "lietu internets",
    "blockchain": "blokķēde",
    "virtual reality": "virtuālā realitāte",
    "augmented reality": "papildinātā realitāte",
    # Science
    "mathematics": "matemātika",
    "physics": "fizika",
    "chemistry": "ķīmija",
    "biology": "bioloģija",
    "statistics": "statistika",
    "probability": "varbūtība",
    "linear algebra": "lineārā algebra",
    "calculus": "matemātiskā analīze",
    "graph theory": "grafu teorija",
    "optimization": "optimizācija",
    "simulation": "simulācija",
    "scientific journal": "zinātniskais žurnāls",
    "academic journal": "akadēmiskais žurnāls",
    "peer review": "recenzēšana",
    "open access": "brīvpiekļuve",
    "university": "universitāte",
    "professor": "profesors",
    "researcher": "pētnieks",
    "scientist": "zinātnieks",
    "laboratory": "laboratorija",
}

# Descriptions: EN pattern -> LV description
LATVIAN_DESCRIPTIONS = {
    # Software types
    "machine learning framework": "mašīnmācīšanās ietvars",
    "deep learning framework": "dziļās mācīšanās ietvars",
    "deep learning library": "dziļās mācīšanās bibliotēka",
    "machine learning library": "mašīnmācīšanās bibliotēka",
    "programming language": "programmēšanas valoda",
    "open-source software": "atvērtā koda programmatūra",
    "free and open-source software": "brīvā un atvērtā koda programmatūra",
    "Python library": "Python bibliotēka",
    "software library": "programmatūras bibliotēka",
    "software framework": "programmatūras ietvars",
    "web framework": "tīmekļa ietvars",
    "web browser": "tīmekļa pārlūkprogramma",
    "search engine": "meklētājprogramma",
    "text editor": "teksta redaktors",
    "video game": "videospēle",
    "mobile app": "mobilā lietotne",
    "operating system": "operētājsistēma",
    # Science types
    "scientific journal": "zinātniskais žurnāls",
    "academic journal": "akadēmiskais žurnāls",
    "peer-reviewed journal": "recenzēts zinātniskais žurnāls",
    "scholarly article": "zinātnisks raksts",
    "scientific article": "zinātnisks raksts",
    "research institute": "pētniecības institūts",
    "branch of mathematics": "matemātikas nozare",
    "branch of physics": "fizikas nozare",
    "branch of computer science": "datorzinātnes nozare",
    "area of computer science": "datorzinātnes joma",
    "field of study": "pētījumu joma",
    "academic discipline": "akadēmiskā disciplīna",
    "scientific conference": "zinātniskā konference",
    "academic conference": "akadēmiskā konference",
    # Algorithm types
    "sorting algorithm": "šķirošanas algoritms",
    "search algorithm": "meklēšanas algoritms",
    "optimization algorithm": "optimizācijas algoritms",
    "machine learning algorithm": "mašīnmācīšanās algoritms",
    "classification algorithm": "klasifikācijas algoritms",
    "clustering algorithm": "klasterizācijas algoritms",
    "graph algorithm": "grafu algoritms",
    "encryption algorithm": "šifrēšanas algoritms",
    # Organization types
    "technology company": "tehnoloģiju uzņēmums",
    "software company": "programmatūras uzņēmums",
    "multinational corporation": "starptautiska korporācija",
    "non-profit organization": "bezpeļņas organizācija",
    "research university": "pētniecības universitāte",
    "public university": "valsts universitāte",
    "private university": "privātā universitāte",
    # Person types
    "computer scientist": "datorzinātnieks",
    "mathematician": "matemātiķis",
    "physicist": "fiziķis",
    "chemist": "ķīmiķis",
    "biologist": "biologs",
    "software engineer": "programmatūras inženieris",
    "electrical engineer": "elektroinženieris",
    # Data types
    "file format": "datņu formāts",
    "data format": "datu formāts",
    "communication protocol": "sakaru protokols",
    "network protocol": "tīkla protokols",
    "markup language": "iezīmēšanas valoda",
    "query language": "vaicājumu valoda",
    # Expanded patterns for SPARQL results
    "expert system": "ekspertu sistēma",
    "statistical software": "statistikas programmatūra",
    "software tool": "programmatūras rīks",
    "computer-assisted": "datorizēts",
    "decision support system": "lēmumu atbalsta sistēma",
    "diagnostic software": "diagnostikas programmatūra",
    "instant messaging service": "tūlītējās ziņapmaiņas pakalpojums",
    "knowledge base": "zināšanu bāze",
    "spell checker": "pareizrakstības pārbaudītājs",
    "web service": "tīmekļa pakalpojums",
    "artificial neural network": "mākslīgais neironu tīkls",
    "predictive analytics": "prognozējošā analītika",
    "natural language processing tool": "dabiskās valodas apstrādes rīks",
    "computer algebra system": "datoralgebras sistēma",
    "image processing": "attēlu apstrāde",
    "data visualization": "datu vizualizācija",
    "version control system": "versiju kontroles sistēma",
    "content management system": "satura pārvaldības sistēma",
    "machine translation": "mašīntulkošana",
    "speech synthesis": "runas sintēze",
    "autonomous vehicle": "autonomais transportlīdzeklis",
    "open-source project": "atvērtā koda projekts",
    "API": "lietojumprogrammu saskarne",
    "data science": "datu zinātne",
    "information retrieval": "informācijas izguve",
    "recommender system": "ieteikumu sistēma",
    "pattern recognition": "attēlu atpazīšana",
    "medical expert system": "medicīniskā ekspertu sistēma",
    "legal expert system": "juridiskā ekspertu sistēma",
    "combinatorial algorithm": "kombinatorisks algoritms",
    "heuristic algorithm": "heiristiskais algoritms",
    "subfield of artificial intelligence": "mākslīgā intelekta apakšnozare",
}

# Generic descriptions to skip (too vague for Wikidata)
GENERIC_SKIP = {
    "software", "algorithm", "tool", "library", "framework",
    "model", "method", "technique", "system", "application",
    "website", "service", "platform", "product", "project",
    "concept", "term", "type", "class", "category",
}

log = logging.getLogger("wikidata-lv")


# ---------------------------------------------------------------------------
# Wikidata session
# ---------------------------------------------------------------------------
class WikidataSession:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.csrf_token = None

    def login(self):
        r = self.session.get(API_URL, params={
            "action": "query", "meta": "tokens",
            "type": "login", "format": "json"})
        r.raise_for_status()
        login_token = r.json()["query"]["tokens"]["logintoken"]

        r = self.session.post(API_URL, data={
            "action": "login", "lgname": BOT_USER,
            "lgpassword": BOT_PASS, "lgtoken": login_token,
            "format": "json"})
        r.raise_for_status()
        result = r.json().get("login", {}).get("result")
        if result != "Success":
            raise RuntimeError(f"Login failed: {result}")
        log.info("Logged in as %s", BOT_USER)

        r = self.session.get(API_URL, params={
            "action": "query", "meta": "tokens", "format": "json"})
        r.raise_for_status()
        self.csrf_token = r.json()["query"]["tokens"]["csrftoken"]

    def set_label(self, qid: str, lang: str, value: str) -> dict:
        r = self.session.post(API_URL, data={
            "action": "wbsetlabel", "id": qid, "language": lang,
            "value": value, "token": self.csrf_token,
            "summary": EDIT_SUMMARY, "bot": "0",
            "maxlag": MAXLAG, "format": "json"})
        r.raise_for_status()
        return r.json()

    def set_description(self, qid: str, lang: str, value: str) -> dict:
        r = self.session.post(API_URL, data={
            "action": "wbsetdescription", "id": qid, "language": lang,
            "value": value, "token": self.csrf_token,
            "summary": EDIT_SUMMARY, "bot": "0",
            "maxlag": MAXLAG, "format": "json"})
        r.raise_for_status()
        return r.json()

    def get_entities(self, qids: list[str]) -> dict:
        r = self.session.get(API_URL, params={
            "action": "wbgetentities", "ids": "|".join(qids),
            "props": "labels|descriptions", "languages": "en|lv",
            "format": "json"})
        r.raise_for_status()
        return r.json().get("entities", {})


# ---------------------------------------------------------------------------
# SPARQL: find items missing Latvian labels/descriptions
# ---------------------------------------------------------------------------
SPARQL_ROOTS = [
    "Q11660",   # artificial intelligence
    "Q2539",    # machine learning
    "Q7397",    # software
    "Q9143",    # programming language
    "Q170730",  # algorithm
    "Q41298",   # scientific journal
    "Q3918",    # university
    "Q11862829", # academic discipline
    "Q1668024", # conference
]


def sparql_find_missing_lv(limit: int = 200) -> list[dict]:
    """Find items that have EN label/description but no LV label or description.
    Tries all SPARQL roots until enough results found."""
    all_results = []
    roots = SPARQL_ROOTS.copy()
    random.shuffle(roots)
    for root in roots:
        if len(all_results) >= limit:
            break
        query = f"""
    SELECT ?item ?itemLabel ?itemDescription WHERE {{
      ?item wdt:P31/wdt:P279* wd:{root}.
      ?item rdfs:label ?itemLabel. FILTER(LANG(?itemLabel) = "en")
      OPTIONAL {{ ?item schema:description ?itemDescription. FILTER(LANG(?itemDescription) = "en") }}
      FILTER NOT EXISTS {{ ?item rdfs:label ?lvLabel. FILTER(LANG(?lvLabel) = "lv") }}
    }}
    LIMIT {limit}
    """
        time.sleep(SPARQL_DELAY)
        try:
            r = requests.get(SPARQL_URL, params={"query": query, "format": "json"},
                             headers={"User-Agent": USER_AGENT}, timeout=30)
            r.raise_for_status()
            for b in r.json().get("results", {}).get("bindings", []):
                qid = b["item"]["value"].split("/")[-1]
                en_label = b.get("itemLabel", {}).get("value", "")
                en_desc = b.get("itemDescription", {}).get("value", "")
                all_results.append({"qid": qid, "en_label": en_label, "en_desc": en_desc})
        except Exception as e:
            log.warning("SPARQL failed for %s: %s", root, e)
    return all_results


def sparql_find_missing_lv_descriptions(limit: int = 200) -> list[dict]:
    """Find items that have LV label but no LV description."""
    query = f"""
    SELECT ?item ?itemLabel ?enDesc WHERE {{
      ?item rdfs:label ?itemLabel. FILTER(LANG(?itemLabel) = "lv")
      ?item schema:description ?enDesc. FILTER(LANG(?enDesc) = "en")
      FILTER NOT EXISTS {{ ?item schema:description ?lvDesc. FILTER(LANG(?lvDesc) = "lv") }}
      ?item wdt:P31/wdt:P279* wd:Q11660.
    }}
    LIMIT {limit}
    """
    time.sleep(SPARQL_DELAY)
    r = requests.get(SPARQL_URL, params={"query": query, "format": "json"},
                     headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    results = []
    for b in r.json().get("results", {}).get("bindings", []):
        qid = b["item"]["value"].split("/")[-1]
        en_desc = b.get("enDesc", {}).get("value", "")
        results.append({"qid": qid, "en_desc": en_desc})
    return results


# ---------------------------------------------------------------------------
# Translation logic (rule-based only, no LLM needed)
# ---------------------------------------------------------------------------
def translate_label(en_label: str) -> Optional[str]:
    """Translate EN label to LV using curated dictionary."""
    key = en_label.lower().strip()
    return LATVIAN_LABELS.get(key)


def _load_lv_dictionary() -> dict:
    """Load EN->LV dictionary scraped from Wikidata itself."""
    import os
    dict_path = os.path.join(os.path.dirname(__file__), "wikidata_lv_dictionary.json")
    if os.path.exists(dict_path):
        with open(dict_path) as f:
            return json.load(f)
    return {}


_LV_DICT_CACHE = None


def _get_lv_dict() -> dict:
    global _LV_DICT_CACHE
    if _LV_DICT_CACHE is None:
        _LV_DICT_CACHE = _load_lv_dictionary()
        # Merge hardcoded descriptions (higher priority)
        _LV_DICT_CACHE.update({k.lower(): v for k, v in LATVIAN_DESCRIPTIONS.items()})
    return _LV_DICT_CACHE


def translate_description(en_desc: str) -> Optional[str]:
    """Translate EN description to LV using Wikidata-sourced dictionary + curated patterns."""
    if not en_desc:
        return None
    desc_lower = en_desc.lower().strip().rstrip(".")

    # Skip generic single-word descriptions
    if desc_lower in GENERIC_SKIP:
        return None
    if " " not in desc_lower:
        return None

    lv_dict = _get_lv_dict()

    # Exact match
    if desc_lower in lv_dict:
        return lv_dict[desc_lower]

    # Flexible match: find longest matching pattern
    best_match = ""
    best_translation = None
    for pattern, translation in lv_dict.items():
        if pattern in desc_lower and len(pattern) > len(best_match):
            best_match = pattern
            best_translation = translation

    if best_translation and len(best_match) > 5:
        return best_translation

    # NO LLM for Latvian — grammar too complex, risk of errors
    # Only use dictionary (2781 verified pairs from Wikidata itself)
    return None


def _validate_with_deepseek(lv_desc: str, en_desc: str) -> Optional[str]:
    """Ask DeepSeek to verify and fix the translation. Second pass QA."""
    import urllib.request, json as _json
    prompt = (
        f"Check this Latvian Wikidata description for errors.\n"
        f"Fix any issues: wrong capitalization of proper nouns, grammar, spelling.\n"
        f"Proper nouns (names, places) MUST be Capitalized in Latvian.\n"
        f"First letter of description must be lowercase (Wikidata rule).\n"
        f"Output ONLY the corrected Latvian text, nothing else.\n\n"
        f"English original: {en_desc}\n"
        f"Latvian to check: {lv_desc}\n"
        f"Corrected Latvian:"
    )
    try:
        payload = _json.dumps({
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1, "max_tokens": 80,
        }).encode()
        req = urllib.request.Request(
            "https://api.deepseek.com/chat/completions",
            data=payload, headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {os.environ.get('DEEPSEEK_API_KEY', '')}",
            })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read())
        text = data["choices"][0]["message"]["content"].strip()
        text = text.strip('"\'').strip().rstrip(".")
        if text and len(text) >= 3 and len(text) <= 200:
            return text
    except Exception:
        pass
    return lv_desc  # Return original if validation fails


def _fix_proper_noun_capitalization(lv_desc: str, en_desc: str) -> str:
    """Fix proper noun capitalization in LV by comparing with EN original.
    If a word is capitalized in EN (not first word), its LV equivalent should be too.
    Also: Wikidata descriptions start lowercase, but proper nouns stay uppercase."""
    en_words = en_desc.split()
    lv_words = lv_desc.split()

    # Collect capitalized words from EN (skip first word, skip common words)
    skip = {"a", "an", "the", "of", "in", "for", "and", "or", "by", "from", "to", "with", "on", "at"}
    en_caps = set()
    for i, w in enumerate(en_words):
        clean = w.strip("(),.:;!?\"'")
        if i > 0 and clean and clean[0].isupper() and clean.lower() not in skip:
            en_caps.add(clean.lower())

    # Fix LV words that correspond to EN capitalized words
    fixed = []
    for i, w in enumerate(lv_words):
        clean = w.strip("(),.:;!?\"'")
        # Check if this LV word looks like a transliteration of an EN capitalized word
        should_cap = False
        for en_cap in en_caps:
            # Simple heuristic: first 3+ chars match
            if len(clean) >= 3 and len(en_cap) >= 3:
                if clean[:3].lower() == en_cap[:3].lower():
                    should_cap = True
                    break
            # Or the word is clearly a name (same in EN and LV)
            if clean.lower() == en_cap:
                should_cap = True
                break

        if should_cap and clean and clean[0].islower() and i > 0:
            # Capitalize but keep the rest
            w = w[0].upper() + w[1:]
        fixed.append(w)

    return " ".join(fixed)


def _deepseek_translate(en_desc: str) -> Optional[str]:
    """Use DeepSeek API to translate description to Latvian."""
    import urllib.request, json as _json
    prompt = (
        f"Translate this Wikidata item description to Latvian.\n"
        f"Rules:\n"
        f"- Short (max 80 chars), no period at end\n"
        f"- First letter lowercase (Wikidata convention)\n"
        f"- BUT proper nouns (names, places, organizations) MUST be Capitalized\n"
        f"- Example: 'artwork by Stephen Blumrich' → 'mākslas darbs no Stīvena Blumriha'\n"
        f"- Output ONLY the Latvian translation, nothing else\n\n"
        f"English: {en_desc}\nLatvian:"
    )
    try:
        payload = _json.dumps({
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.2, "max_tokens": 80,
        }).encode()
        req = urllib.request.Request(
            "https://api.deepseek.com/chat/completions",
            data=payload, headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {os.environ.get('DEEPSEEK_API_KEY', '')}",
            })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = _json.loads(resp.read())
        text = data["choices"][0]["message"]["content"].strip()
        text = text.strip('"\'').strip().rstrip(".")
        if "<think>" in text:
            text = text.split("</think>")[-1].strip()
        if not text or len(text) < 3 or len(text) > 200:
            return None
        if text.lower().startswith(("translation", "here", "latvian")):
            return None
        return text
    except Exception:
        return None


def _ollama_translate(en_desc: str) -> Optional[str]:
    """Use local Ollama deepseek-r1:14b to translate description to Latvian."""
    prompt = (
        f"Translate this Wikidata item description to Latvian.\n"
        f"Rules:\n"
        f"- Short (max 80 chars), no period at end\n"
        f"- First letter lowercase (Wikidata convention)\n"
        f"- BUT proper nouns (names, places, organizations) MUST be Capitalized\n"
        f"- Example: 'artwork by Stephen Blumrich' → 'mākslas darbs no Stīvena Blumriha'\n"
        f"- Output ONLY the Latvian translation, nothing else\n\n"
        f"English: {en_desc}\nLatvian:"
    )
    try:
        r = requests.post("http://localhost:11434/api/generate",
            json={"model": "deepseek-r1:14b", "prompt": prompt,
                  "stream": False, "options": {"temperature": 0.1, "num_predict": 80}},
            timeout=30)
        r.raise_for_status()
        text = r.json().get("response", "").strip()
        if "<think>" in text:
            text = text.split("</think>")[-1].strip()
        text = text.strip('"\'').strip().rstrip(".")
        if not text or len(text) < 3 or len(text) > 200:
            return None
        if text.lower().startswith("translation") or text.lower().startswith("here"):
            return None
        return text
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def collect_candidates(count: int) -> list[tuple]:
    """Collect (qid, action, lang, value) tuples.
    Deduplication: max 2 identical descriptions per run to avoid mass-cloning."""
    candidates = []
    desc_counts: dict[str, int] = {}
    MAX_SAME_DESC = 2

    # Phase 1: items missing LV labels
    log.info("Searching for items missing Latvian labels...")
    items = sparql_find_missing_lv(limit=count * 5)
    log.info("Found %d items without LV labels", len(items))

    for item in items:
        if len(candidates) >= count:
            break
        # Labels: only translate if we have exact match (common terms)
        lv_label = translate_label(item["en_label"])
        if lv_label:
            candidates.append((item["qid"], "label", "lv", lv_label))

        # Descriptions: translate EN description pattern to LV
        lv_desc = translate_description(item["en_desc"])
        if lv_desc and len(candidates) < count:
            if desc_counts.get(lv_desc, 0) >= MAX_SAME_DESC:
                log.info("  Skipping %s — description '%s' already used %d times",
                         item["qid"], lv_desc[:40], MAX_SAME_DESC)
                continue
            desc_counts[lv_desc] = desc_counts.get(lv_desc, 0) + 1
            candidates.append((item["qid"], "description", "lv", lv_desc))
        elif not lv_label and lv_desc:
            if desc_counts.get(lv_desc, 0) >= MAX_SAME_DESC:
                continue
            desc_counts[lv_desc] = desc_counts.get(lv_desc, 0) + 1
            candidates.append((item["qid"], "description", "lv", lv_desc))

    # Phase 2: items with LV label but missing LV description
    if len(candidates) < count:
        log.info("Searching for items missing Latvian descriptions...")
        items2 = sparql_find_missing_lv_descriptions(limit=count * 5)
        log.info("Found %d items with LV label but no LV description", len(items2))

        for item in items2:
            if len(candidates) >= count:
                break
            lv_desc = translate_description(item["en_desc"])
            if lv_desc:
                if desc_counts.get(lv_desc, 0) >= MAX_SAME_DESC:
                    log.info("  Skipping %s — description '%s' already used %d times",
                             item["qid"], lv_desc[:40], MAX_SAME_DESC)
                    continue
                desc_counts[lv_desc] = desc_counts.get(lv_desc, 0) + 1
                candidates.append((item["qid"], "description", "lv", lv_desc))

    if desc_counts:
        dupes = {k: v for k, v in desc_counts.items() if v > 1}
        if dupes:
            log.info("Description diversity: %d unique, %d capped at %d",
                     len(desc_counts), len(dupes), MAX_SAME_DESC)

    return candidates


def main():
    parser = argparse.ArgumentParser(description="Wikidata Latvian Bot")
    parser.add_argument("--count", type=int, default=20,
                        help="Number of edits to make (default: 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't actually edit, just show what would be done")
    parser.add_argument("--geometric", action="store_true",
                        help="Auto-scale count +10%%/day based on yesterday's edits")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")

    # Geometric progression: yesterday's LV edits × 1.1, divided by 3 runs/day
    if args.geometric:
        from datetime import datetime, timedelta, timezone
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        today_start = datetime.now(timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
        try:
            r = requests.get(API_URL, params={
                "action": "query", "list": "usercontribs",
                "ucuser": BOT_USER.split("@")[0],
                "uclimit": "500", "ucstart": today_start, "ucend": yesterday,
                "ucprop": "timestamp", "format": "json",
            }, headers={"User-Agent": USER_AGENT}, timeout=15)
            yesterday_edits = len(r.json().get("query", {}).get("usercontribs", []))
        except Exception:
            yesterday_edits = 0
        if yesterday_edits < 10:
            yesterday_edits = 20  # bootstrap minimum
        runs_per_day = 3
        max_daily = 100  # conservative cap for LV bot
        daily_target = min(int(yesterday_edits * 1.1), max_daily)
        args.count = max(5, daily_target // runs_per_day)
        log.info("Geometric mode: yesterday=%d, today_target=%d, per_run=%d",
                 yesterday_edits, daily_target, args.count)

    candidates = collect_candidates(args.count)
    log.info("Collected %d candidates", len(candidates))

    if not candidates:
        log.info("No candidates found. Dictionary may need expansion.")
        return

    if args.dry_run:
        for qid, action, lang, value in candidates:
            log.info("[DRY RUN] %s %s [%s] = %s", qid, action, lang, value)
        log.info("Dry run complete. %d edits would be made.", len(candidates))
        return

    ws = WikidataSession()
    ws.login()

    edits_done = 0
    for qid, action, lang, value in candidates:
        try:
            if action == "label":
                result = ws.set_label(qid, lang, value)
            else:
                result = ws.set_description(qid, lang, value)

            if "error" in result:
                err = result["error"]
                if err.get("code") == "maxlag":
                    log.warning("Maxlag hit, waiting 10s...")
                    time.sleep(10)
                    continue
                log.warning("Error on %s: %s", qid, err.get("info", err))
                continue

            edits_done += 1
            log.info("[%d/%d] %s %s [%s] = %s",
                     edits_done, args.count, qid, action, lang, value)
            time.sleep(random.uniform(4, 8))

        except Exception as e:
            log.error("Failed %s: %s", qid, e)
            time.sleep(5)

    log.info("Done. %d edits completed.", edits_done)


if __name__ == "__main__":
    main()
