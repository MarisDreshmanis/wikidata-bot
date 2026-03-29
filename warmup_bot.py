#!/usr/bin/env python3
"""
Wikidata Warmup — add missing descriptions to science/tech Wikidata items.
Goal: reach 500 edits on a new account.

Usage:
    python3 tools/wikidata_warmup.py --count 50 --lang ru
    python3 tools/wikidata_warmup.py --count 50 --lang de,es,fr,pt
    python3 tools/wikidata_warmup.py --count 50 --lang uk,pl --dry-run
    python3 tools/wikidata_warmup.py --count 50 --lang ja,ko --use-llm --dry-run
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
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "deepseek-r1:14b"

USER_AGENT = (
    "ReincarnatiopediaBot/1.0 "
    "(https://reincarnatiopedia.com; mailto:wikidata@marisdreshmanis.com)"
)
BOT_USER = os.environ.get("WIKIDATA_BOT_USER", "")
BOT_PASS = os.environ.get("WIKIDATA_BOT_PASS", "")
EDIT_SUMMARY = "Adding missing language descriptions for science and technology items"
MAXLAG = 5

# Min delay between SPARQL queries (Wikidata rate limit)
SPARQL_DELAY = 5.0

# AI-related root items to use as SPARQL seeds
AI_TOPICS = [
    "Q11660",   # artificial intelligence
    "Q2539",    # machine learning
    "Q175263",  # deep learning
    "Q7397",    # software
    "Q80006",   # natural language processing
    "Q192776",  # neural network
    "Q207858",  # decision tree
    "Q221349",  # genetic algorithm
    "Q235557",  # random forest
    "Q245005",  # reinforcement learning
    "Q497956",  # computer vision
    "Q728059",  # data mining
    "Q840829",  # Bayesian network
    "Q913764",  # sentiment analysis
    "Q1142960", # support vector machine
    "Q1426559", # recurrent neural network
    "Q2048316", # Markov decision process
    "Q2374463", # knowledge representation
    "Q3400548", # convolutional neural network
    "Q5135881", # computational linguistics
    "Q5155786", # computer-assisted translation
    "Q5276855", # dimensionality reduction
    "Q5443616", # feature extraction
    "Q7889012", # unsupervised learning
    "Q11019",   # robotics
    "Q68",      # computer
    "Q9143",    # programming language
    "Q5300",    # image recognition
    "Q188444",  # big data
    "Q131476",  # expert system
    "Q332154",  # speech recognition
    "Q846104",  # gradient descent
    "Q6397614", # k-nearest neighbors
    "Q917253",  # k-means clustering
    "Q44418",   # Turing test
    "Q204570",  # perceptron
]

# Root categories for SPARQL queries (broader set)
SPARQL_ROOTS = [
    ("Q11660", "artificial intelligence"),
    ("Q2539", "machine learning"),
    ("Q175263", "deep learning"),
    ("Q80006", "natural language processing"),
    ("Q7397", "software"),
    ("Q170730", "algorithm"),
    ("Q41298", "scientific journal"),
    ("Q192776", "neural network"),
    ("Q3918", "university"),
    ("Q1668024", "conference"),
    ("Q21198342", "scholarly article"),   # huge category — many missing descs
    ("Q7889012", "unsupervised learning"),
    ("Q497956", "computer vision"),
    ("Q11019", "robotics"),
    ("Q68", "computer"),
]

# All supported languages for rule-based translation
RULE_BASED_LANGS = {"ru", "de", "es", "fr", "pt"}

# Extended target languages
EXTENDED_LANGS = [
    "ja", "zh", "ko", "ar", "hi", "tr", "uk", "pl", "cs", "sv",
    "nl", "fi", "da", "no", "el", "he", "th", "vi", "id", "ms",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("wikidata_warmup")


# ---------------------------------------------------------------------------
# Session helper
# ---------------------------------------------------------------------------
class WikidataSession:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.csrf_token: Optional[str] = None
        self._last_sparql_time = 0.0

    # ---- auth ----
    def login(self):
        # Step 1: get login token
        r = self.session.get(API_URL, params={
            "action": "query",
            "meta": "tokens",
            "type": "login",
            "format": "json",
            "maxlag": MAXLAG,
        })
        r.raise_for_status()
        login_token = r.json()["query"]["tokens"]["logintoken"]

        # Step 2: login
        r = self.session.post(API_URL, data={
            "action": "login",
            "lgname": BOT_USER,
            "lgpassword": BOT_PASS,
            "lgtoken": login_token,
            "format": "json",
            "maxlag": MAXLAG,
        })
        r.raise_for_status()
        result = r.json()
        if result.get("login", {}).get("result") != "Success":
            log.error("Login failed: %s", json.dumps(result, indent=2))
            sys.exit(1)
        log.info("Logged in as %s", result["login"]["lgusername"])

    def get_csrf_token(self):
        r = self.session.get(API_URL, params={
            "action": "query",
            "meta": "tokens",
            "format": "json",
            "maxlag": MAXLAG,
        })
        r.raise_for_status()
        self.csrf_token = r.json()["query"]["tokens"]["csrftoken"]
        log.info("CSRF token obtained")

    # ---- SPARQL ----
    def sparql_query(self, query: str) -> list[dict]:
        """Run a SPARQL query against Wikidata Query Service with rate limiting."""
        # Enforce rate limit
        elapsed = time.time() - self._last_sparql_time
        if elapsed < SPARQL_DELAY:
            wait = SPARQL_DELAY - elapsed
            log.info("  SPARQL rate limit: waiting %.1fs...", wait)
            time.sleep(wait)

        try:
            r = self.session.get(
                SPARQL_URL,
                params={"query": query, "format": "json"},
                headers={"Accept": "application/sparql-results+json"},
                timeout=60,
            )
            self._last_sparql_time = time.time()

            if r.status_code == 429:
                log.warning("SPARQL 429 Too Many Requests, backing off 30s...")
                time.sleep(30)
                return []
            if r.status_code == 500:
                log.warning("SPARQL 500 (query too heavy), skipping")
                return []
            r.raise_for_status()
            return r.json().get("results", {}).get("bindings", [])
        except requests.exceptions.Timeout:
            log.warning("SPARQL query timed out")
            return []
        except Exception as e:
            log.warning("SPARQL query failed: %s", e)
            return []

    # ---- read helpers ----
    def get_entities(self, qids: list[str], props="descriptions|labels"):
        """Fetch entity data for a batch of QIDs (max 50)."""
        r = self.session.get(API_URL, params={
            "action": "wbgetentities",
            "ids": "|".join(qids),
            "props": props,
            "format": "json",
            "maxlag": MAXLAG,
        })
        r.raise_for_status()
        return r.json().get("entities", {})

    def search_items(self, query: str, limit=50) -> list[str]:
        """Search Wikidata items by text query, return QIDs."""
        r = self.session.get(API_URL, params={
            "action": "wbsearchentities",
            "search": query,
            "language": "en",
            "type": "item",
            "limit": limit,
            "format": "json",
            "maxlag": MAXLAG,
        })
        r.raise_for_status()
        return [item["id"] for item in r.json().get("search", [])]

    def get_backlinks(self, qid: str, limit=100) -> list[str]:
        """Get items that link to the given QID (e.g. instance-of or subclass-of)."""
        r = self.session.get(API_URL, params={
            "action": "query",
            "list": "backlinks",
            "bltitle": qid,
            "blnamespace": 0,
            "bllimit": limit,
            "format": "json",
            "maxlag": MAXLAG,
        })
        r.raise_for_status()
        results = r.json().get("query", {}).get("backlinks", [])
        qids = []
        for item in results:
            title = item.get("title", "")
            if title.startswith("Q"):
                qids.append(title)
        return qids

    # ---- write ----
    def set_description(self, qid: str, lang: str, description: str) -> dict:
        """Set a description on a Wikidata item."""
        r = self.session.post(API_URL, data={
            "action": "wbsetdescription",
            "id": qid,
            "language": lang,
            "value": description,
            "summary": EDIT_SUMMARY,
            "token": self.csrf_token,
            "format": "json",
            "maxlag": MAXLAG,
            "bot": 0,  # not a bot flag — regular edit
        })
        r.raise_for_status()
        return r.json()

    def check_abuse_log(self, limit: int = 10) -> list[dict]:
        """Check recent abuse filter hits for our user. Returns list of hits."""
        r = self.session.get(API_URL, params={
            "action": "query",
            "list": "abuselog",
            "afluser": BOT_USER.split("@")[0],  # "Maris Dreshmanis"
            "afllimit": limit,
            "format": "json",
        })
        r.raise_for_status()
        data = r.json()
        hits = data.get("query", {}).get("abuselog", [])
        if hits:
            log.warning("⚠️  ABUSE FILTER: %d recent hits detected!", len(hits))
            for h in hits[:3]:
                log.warning("  Filter #%s on %s: %s",
                            h.get("filter_id", "?"),
                            h.get("title", "?"),
                            h.get("filter", "?"))
        return hits


# ---------------------------------------------------------------------------
# SPARQL candidate search
# ---------------------------------------------------------------------------
def build_sparql_query(root_qid: str, target_lang: str, limit: int = 200) -> str:
    """
    Build a SPARQL query that finds items that are instances/subclasses of
    root_qid, have an English description, but LACK a description in target_lang.
    """
    return f"""
SELECT ?item ?itemLabel ?itemDescription WHERE {{
  ?item wdt:P31/wdt:P279* wd:{root_qid}.
  ?item rdfs:label ?itemLabel. FILTER(LANG(?itemLabel) = "en")
  ?item schema:description ?itemDescription. FILTER(LANG(?itemDescription) = "en")
  FILTER NOT EXISTS {{ ?item schema:description ?desc. FILTER(LANG(?desc) = "{target_lang}") }}
}} LIMIT {limit}
"""


def sparql_find_candidates(ws: WikidataSession, lang: str, needed: int) -> list[tuple[str, str, str]]:
    """
    Use SPARQL to find items missing descriptions in `lang`.
    Returns list of (qid, en_label, en_desc) tuples.
    """
    results = []
    seen = set()

    for root_qid, root_name in SPARQL_ROOTS:
        if len(results) >= needed:
            break

        log.info("  SPARQL: items under %s (%s) missing [%s]...", root_qid, root_name, lang)
        query = build_sparql_query(root_qid, lang, limit=min(200, needed - len(results) + 50))
        bindings = ws.sparql_query(query)

        for b in bindings:
            if len(results) >= needed:
                break
            item_uri = b.get("item", {}).get("value", "")
            qid = item_uri.split("/")[-1] if "/" in item_uri else ""
            if not qid.startswith("Q") or qid in seen:
                continue
            seen.add(qid)

            en_label = b.get("itemLabel", {}).get("value", "")
            en_desc = b.get("itemDescription", {}).get("value", "")
            if en_label and en_desc and len(en_desc) >= 5:
                results.append((qid, en_label, en_desc))

        if bindings:
            log.info("    Found %d items from %s", len(bindings), root_name)

    return results


# ---------------------------------------------------------------------------
# LLM translation (Ollama)
# ---------------------------------------------------------------------------
# Language names for LLM prompts
LANG_NAMES = {
    "ru": "Russian", "de": "German", "es": "Spanish", "fr": "French",
    "pt": "Portuguese", "ja": "Japanese", "zh": "Chinese (Simplified)",
    "ko": "Korean", "ar": "Arabic", "hi": "Hindi", "tr": "Turkish",
    "uk": "Ukrainian", "pl": "Polish", "cs": "Czech", "sv": "Swedish",
    "nl": "Dutch", "fi": "Finnish", "da": "Danish", "no": "Norwegian",
    "el": "Greek", "he": "Hebrew", "th": "Thai", "vi": "Vietnamese",
    "id": "Indonesian", "ms": "Malay",
}


def llm_translate(en_desc: str, target_lang: str,
                   en_label: str = "", item_type: str = "") -> Optional[str]:
    """
    Generate a unique, localized Wikidata description in the target language.
    NOT a translation — writes from scratch in the target language,
    using the item's context (label, type, EN description) as input.
    Tries Ollama first (local), then Gemini API (free), then rule-based fallback.
    """
    lang_name = LANG_NAMES.get(target_lang, target_lang)

    context = f"Item label: {en_label}\n" if en_label else ""
    context += f"Item type: {item_type}\n" if item_type else ""
    context += f"English description: {en_desc}"

    prompt = (
        f"Write a SHORT Wikidata description in {lang_name} for this item.\n"
        f"Rules:\n"
        f"- Write natively in {lang_name}, do NOT translate from English\n"
        f"- Maximum 80 characters\n"
        f"- No period at the end\n"
        f"- Must be specific to THIS item, not generic\n"
        f"- Start with lowercase (unless language requires uppercase)\n"
        f"- Output ONLY the description, nothing else\n\n"
        f"{context}\n\n"
        f"{lang_name} description:"
    )

    # Priority 1: DeepSeek API (fast, reliable, ~2s per request)
    result = _deepseek_generate_description(prompt)
    if result:
        return result

    # Priority 2: Ollama local (slow on CPU, ~60-120s, skip if DeepSeek works)
    try:
        r = requests.post(
            OLLAMA_URL.replace("/api/generate", "/api/chat"),
            json={
                "model": OLLAMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 250},
            },
            timeout=120,
        )
        r.raise_for_status()
        response = r.json().get("message", {}).get("content", "").strip()

        if "<think>" in response:
            parts = response.split("</think>")
            response = parts[-1].strip() if len(parts) > 1 else response
        response = response.strip('"\'').strip()

        if response and 2 <= len(response) <= 250:
            if not response.lower().startswith(("translation:", "here")):
                return response
    except Exception:
        pass

    # Priority 3: Gemini API (often 429, last resort)
    return _gemini_generate_description(prompt, target_lang)


import json as _jj
from pathlib import Path as _PP
GEMINI_KEYS = os.environ.get("GEMINI_API_KEY", "").split(",") if os.environ.get("GEMINI_API_KEY") else []
_gemini_key_idx = 0


def _gemini_generate_description(prompt: str, target_lang: str) -> Optional[str]:
    """Use Gemini API (free) to generate a localized Wikidata description.
    Rotates through all keys on failure before giving up."""
    global _gemini_key_idx
    import urllib.request, json as _json

    for attempt in range(len(GEMINI_KEYS)):
        key = GEMINI_KEYS[(_gemini_key_idx + attempt) % len(GEMINI_KEYS)]

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent?key={key}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 100},
        }

        try:
            req = urllib.request.Request(
                url, data=_json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = _json.loads(resp.read())
            text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            text = text.strip('"\'').strip()
            # Remove thinking blocks if any
            if "<think>" in text:
                text = text.split("</think>")[-1].strip()
            if not text or len(text) < 2 or len(text) > 250:
                _gemini_key_idx = (_gemini_key_idx + attempt + 1) % len(GEMINI_KEYS)
                return None
            if text.lower().startswith("description:") or text.lower().startswith("here"):
                text = text.split(":", 1)[-1].strip() if ":" in text else None
            _gemini_key_idx = (_gemini_key_idx + attempt + 1) % len(GEMINI_KEYS)
            return text
        except Exception as e:
            if attempt == len(GEMINI_KEYS) - 1:
                log.warning("Gemini API failed on all %d keys: %s", len(GEMINI_KEYS), e)
            continue

    _gemini_key_idx = (_gemini_key_idx + len(GEMINI_KEYS)) % len(GEMINI_KEYS)
    return None


DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"


def _deepseek_generate_description(prompt: str) -> Optional[str]:
    """Use DeepSeek API as fallback when Gemini is rate-limited."""
    import urllib.request, json as _json

    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 100,
    }

    try:
        req = urllib.request.Request(
            DEEPSEEK_API_URL,
            data=_json.dumps(payload).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            })
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = _json.loads(resp.read())
        text = data["choices"][0]["message"]["content"].strip()
        text = text.strip('"\'').strip()
        if "<think>" in text:
            text = text.split("</think>")[-1].strip()
        if not text or len(text) < 2 or len(text) > 250:
            return None
        if text.lower().startswith("description:") or text.lower().startswith("here"):
            text = text.split(":", 1)[-1].strip() if ":" in text else None
        return text
    except Exception as e:
        log.warning("DeepSeek API failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Description generation — rule-based (flexible matching)
# ---------------------------------------------------------------------------

# Shared pattern dictionary used by all rule-based translators.
# Keys are English patterns; if the description CONTAINS the pattern, it matches.
COMMON_PATTERNS = {
    "scientific article": True,
    "scholarly article": True,
    "academic journal": True,
    "peer-reviewed journal": True,
    "academic conference": True,
    "research institute": True,
    "research laboratory": True,
    "computer science concept": True,
    "mathematical concept": True,
    "statistical method": True,
    "statistical model": True,
    "probability distribution": True,
    "optimization algorithm": True,
    "data structure": True,
    "software library": True,
    "open-source software": True,
    "python library": True,
    "java library": True,
    "loss function": True,
    "activation function": True,
    "neural network architecture": True,
    "machine learning model": True,
    "machine learning algorithm": True,
    "machine learning method": True,
    "machine learning technique": True,
    "deep learning model": True,
    "deep learning algorithm": True,
    "deep learning architecture": True,
    "classification algorithm": True,
    "clustering algorithm": True,
    "regression algorithm": True,
    "scientific journal": True,
    "computer program": True,
    "web application": True,
    "mobile application": True,
    "operating system": True,
    "database": True,
    "search engine": True,
    "file format": True,
    "sorting algorithm": True,
    "graph algorithm": True,
    "encryption algorithm": True,
    "communication protocol": True,
    "software tool": True,
    "software framework": True,
    "programming language": True,
    "algorithm": True,
    "neural network": True,
    "machine learning": True,
    "deep learning": True,
    "artificial intelligence": True,
    "natural language processing": True,
    "computer vision": True,
    "data mining": True,
    "reinforcement learning": True,
    "supervised learning": True,
    "unsupervised learning": True,
    "speech recognition": True,
    "image recognition": True,
    "robotics": True,
    "software": True,
    "dataset": True,
    "compiler": True,
    "interpreter": True,
    "debugger": True,
    "language model": True,
    "numerical method": True,
}


def _find_best_pattern(en_desc_lower: str, translation_dict: dict) -> Optional[str]:
    """
    Matching strategy for quality descriptions:
    1. EXACT match (en_desc == pattern): translate directly
    2. NEAR-EXACT (en_desc starts with pattern, rest is short): translate
    3. COMPOSITE (en_desc has prefix like "type of X"): handled by template logic
    4. Otherwise: return None (let LLM handle or skip)

    This prevents generic translations like "artificial intelligence" for items
    whose EN description contains more specific context.
    """
    stripped = en_desc_lower.strip()
    sorted_patterns = sorted(translation_dict.keys(), key=len, reverse=True)

    for pattern in sorted_patterns:
        # Exact match — best case
        if stripped == pattern:
            return translation_dict[pattern]
        # Near-exact: description IS the pattern with minor extras (year, parenthetical)
        # e.g. "machine learning (computer science)" or "algorithm (2019)"
        if stripped.startswith(pattern):
            rest = stripped[len(pattern):].strip()
            # Only allow short suffixes like "(2019)" or " system"
            if len(rest) < 20 and (rest.startswith("(") or rest.startswith(",")):
                return translation_dict[pattern]

    # No good match — don't force a generic translation
    return None


def _strip_article(d: str) -> str:
    """Strip leading English articles for matching."""
    for prefix in ("a ", "an ", "the "):
        if d.startswith(prefix):
            return d[len(prefix):]
    return d


# ---------------------------------------------------------------------------
# Quality gate — prevents abuse filter triggers
# ---------------------------------------------------------------------------
# Single-word or very short generic terms that Wikidata filter #64 catches
GENERIC_DESCRIPTIONS = {
    "software", "algorithm", "algorithme", "algoritmo", "algorithmus",
    "logiciel", "programme", "programa", "программа", "алгоритм",
    "application", "aplicación", "aplicação", "приложение", "anwendung",
    "tool", "outil", "herramienta", "ferramenta", "инструмент", "werkzeug",
    "library", "bibliothèque", "biblioteca", "библиотека", "bibliothek",
    "framework", "système", "sistema", "система",
    "model", "modèle", "modelo", "модель", "modell",
    "method", "méthode", "método", "метод", "methode",
    "technique", "técnica", "техника", "technik",
    "journal", "revista", "журнал", "zeitschrift",
    "conference", "conférence", "conferencia", "conferência", "конференция", "konferenz",
    "database", "base de données", "base de datos", "banco de dados", "база данных", "datenbank",
    "website", "site web", "sitio web", "сайт", "webseite",
    "service", "servicio", "serviço", "сервис", "dienst",
    "platform", "plateforme", "plataforma", "платформа", "plattform",
    "machine learning", "maschinelles lernen", "aprendizaje automático",
    "apprentissage automatique", "aprendizado de máquina", "машинное обучение",
    "artificial intelligence", "intelligence artificielle",
    "inteligencia artificial", "inteligência artificial",
    "künstliche intelligenz", "искусственный интеллект",
    "deep learning", "apprentissage profond", "aprendizaje profundo",
    "aprendizado profundo", "глубокое обучение",
    "neural network", "réseau de neurones", "red neuronal",
    "rede neural", "нейронная сеть", "neuronales netz",
    "chatbot", "bot",
}

MIN_DESCRIPTION_LENGTH = 15  # Wikidata filter #64 catches short generic descriptions


def _sanitize_description(desc: str) -> Optional[str]:
    """Clean up a description: strip quotes, periods, whitespace."""
    if not desc:
        return None
    desc = desc.strip().strip('"\'').strip()
    # Remove trailing period (filter #72)
    desc = desc.rstrip(".")
    # Remove leading/trailing whitespace again
    desc = desc.strip()
    if not desc:
        return None
    return desc


def _passes_quality_gate(desc: str) -> bool:
    """Check if description is good enough for Wikidata (avoids filter #64)."""
    if not desc:
        return False
    # Too short = generic
    if len(desc) < MIN_DESCRIPTION_LENGTH:
        return False
    # Exact match against known generic terms
    if desc.lower().strip() in GENERIC_DESCRIPTIONS:
        return False
    # Single word = almost certainly generic
    if " " not in desc.strip():
        return False
    return True


def _validate_german_capitalization(desc: str) -> Optional[str]:
    """Fix German capitalization: nouns must be capitalized.
    Wikidata descriptions start lowercase, but German nouns are always capitalized."""
    # Common German nouns that MUST be capitalized
    german_nouns = {
        "intelligenz", "software", "algorithmus", "system", "programm",
        "sprache", "netzwerk", "methode", "modell", "technik", "werkzeug",
        "bibliothek", "rahmenwerk", "anwendung", "datenbank", "protokoll",
        "institut", "universität", "konferenz", "zeitschrift", "artikel",
        "analyse", "erkennung", "verarbeitung", "optimierung", "struktur",
        "funktion", "theorie", "steuerung", "robotik", "lernen", "vision",
        "sicherheit", "dienst", "plattform", "heuristik", "ableitung",
        "teilgebiet", "unternehmens", "expertensystem", "figur", "film",
        "assistent", "regelung", "klassifizierung", "lösung", "betrieb",
        "rechtschreibprüfung", "nachahmung", "strategie", "quelle",
        "entscheidungshilfe", "busbetrieb", "robotaxi",
    }
    words = desc.split()
    fixed = []
    for i, word in enumerate(words):
        w_lower = word.lower().rstrip(".,;:!?")
        if i == 0:
            # First word: lowercase per Wikidata convention
            fixed.append(word)
        elif w_lower in german_nouns or any(w_lower.startswith(n) for n in german_nouns):
            # Capitalize German noun
            fixed.append(word[0].upper() + word[1:] if word[0].islower() else word)
        else:
            fixed.append(word)
    return " ".join(fixed)


def generate_description(en_label: str, en_desc: str, lang: str,
                         use_llm: bool = False,
                         item_type: str = "") -> Optional[str]:
    """
    Generate a unique, localized description in the target language.

    Strategy (Wikipedia-compliant):
    1. Rule-based patterns: curated dictionaries of exact matches (no AI/LLM)
    2. LLM fallback: only when --use-llm explicitly passed (not used in production)
    3. Skip if can't produce quality description

    Quality gate: reject generic descriptions that match common patterns
    (e.g. just "inteligência artificial" without context).
    """
    if not en_desc:
        return None

    en_desc_lower = en_desc.lower().strip()

    # Skip if the English description is too short or generic
    if len(en_desc_lower) < 5:
        return None
    # Skip if EN source is itself generic (single word or known generic term)
    # These produce bad translations that trigger Wikidata abuse filter #64
    if " " not in en_desc_lower or en_desc_lower in GENERIC_DESCRIPTIONS:
        return None

    # STRATEGY 1: Rule-based patterns (primary — no LLM, Wikipedia-compliant)
    result = None
    if lang == "ru":
        result = _translate_to_ru(en_label, en_desc)
    elif lang == "de":
        result = _translate_to_de(en_label, en_desc)
    elif lang == "es":
        result = _translate_to_es(en_label, en_desc)
    elif lang == "fr":
        result = _translate_to_fr(en_label, en_desc)
    elif lang == "pt":
        result = _translate_to_pt(en_label, en_desc)

    if result:
        result = _sanitize_description(result)
    if result and _passes_quality_gate(result):
        return result

    # STRATEGY 2: LLM-generated (only when --use-llm explicitly passed)
    if use_llm:
        llm_result = llm_translate(
            en_desc, lang, en_label=en_label, item_type=item_type)
        if llm_result:
            llm_result = _sanitize_description(llm_result)
        if llm_result and _passes_quality_gate(llm_result):
            if lang == "de":
                llm_result = _validate_german_capitalization(llm_result)
            return llm_result

    return None


def _translate_to_ru(en_label: str, en_desc: str) -> Optional[str]:
    """Rule-based translation of common AI/CS descriptions to Russian."""
    d = en_desc.lower().strip().rstrip(".")
    d_clean = _strip_article(d)

    # Direct mapping of very common description patterns
    mapping = {
        "artificial intelligence": "искусственный интеллект",
        "machine learning": "машинное обучение",
        "deep learning": "глубокое обучение",
        "natural language processing": "обработка естественного языка",
        "computer vision": "компьютерное зрение",
        "neural network": "нейронная сеть",
        "data mining": "интеллектуальный анализ данных",
        "reinforcement learning": "обучение с подкреплением",
        "supervised learning": "обучение с учителем",
        "unsupervised learning": "обучение без учителя",
        "speech recognition": "распознавание речи",
        "image recognition": "распознавание изображений",
        "robotics": "робототехника",
        "big data": "большие данные",
        "programming language": "язык программирования",
        "software": "программное обеспечение",
        "algorithm": "алгоритм",
    }

    # Pattern-based translations
    templates_ru = {
        "branch of": "раздел",
        "subfield of": "подраздел",
        "field of": "область",
        "area of": "область",
        "type of": "тип",
        "class of": "класс",
        "family of": "семейство",
        "method of": "метод",
        "technique in": "техника в области",
        "approach to": "подход к",
        "application of": "применение",
        "form of": "форма",
        "kind of": "вид",
        "subset of": "подмножество",
        "used in": "используется в",
        "used for": "используется для",
        "part of": "часть",
    }

    # Try pattern: "<prefix> ... <known_term> ..."
    for prefix_en, prefix_ru in templates_ru.items():
        if d.startswith(prefix_en) or d_clean.startswith(prefix_en):
            rest = d[d.index(prefix_en) + len(prefix_en):].strip()
            for term_en, term_ru in mapping.items():
                if term_en in rest:
                    result = f"{prefix_ru} {term_ru}"
                    if len(result) < 120:
                        return result

    simple_ru = {
        "scientific article": "научная статья",
        "scholarly article": "научная статья",
        "academic journal": "научный журнал",
        "peer-reviewed journal": "рецензируемый журнал",
        "academic conference": "научная конференция",
        "research institute": "исследовательский институт",
        "research laboratory": "исследовательская лаборатория",
        "computer science concept": "понятие информатики",
        "mathematical concept": "математическое понятие",
        "statistical method": "статистический метод",
        "statistical model": "статистическая модель",
        "probability distribution": "распределение вероятности",
        "optimization algorithm": "алгоритм оптимизации",
        "data structure": "структура данных",
        "software library": "программная библиотека",
        "open-source software": "открытое программное обеспечение",
        "python library": "библиотека Python",
        "java library": "библиотека Java",
        "loss function": "функция потерь",
        "activation function": "функция активации",
        "neural network architecture": "архитектура нейронной сети",
        "machine learning model": "модель машинного обучения",
        "machine learning algorithm": "алгоритм машинного обучения",
        "machine learning method": "метод машинного обучения",
        "machine learning technique": "метод машинного обучения",
        "deep learning model": "модель глубокого обучения",
        "deep learning algorithm": "алгоритм глубокого обучения",
        "deep learning architecture": "архитектура глубокого обучения",
        "classification algorithm": "алгоритм классификации",
        "clustering algorithm": "алгоритм кластеризации",
        "regression algorithm": "алгоритм регрессии",
        "dimensionality reduction technique": "метод снижения размерности",
        "feature selection method": "метод отбора признаков",
        "ensemble learning method": "метод ансамблевого обучения",
        "regularization technique": "метод регуляризации",
        "hyperparameter": "гиперпараметр",
        "benchmark dataset": "эталонный набор данных",
        "dataset": "набор данных",
        "evaluation metric": "метрика оценки",
        "scientific journal": "научный журнал",
        "computer program": "компьютерная программа",
        "web application": "веб-приложение",
        "mobile application": "мобильное приложение",
        "operating system": "операционная система",
        "database": "база данных",
        "search engine": "поисковая система",
        "file format": "формат файла",
        "communication protocol": "протокол связи",
        "encryption algorithm": "алгоритм шифрования",
        "sorting algorithm": "алгоритм сортировки",
        "graph algorithm": "алгоритм на графах",
        "numerical method": "численный метод",
        "linear algebra": "линейная алгебра",
        "calculus": "математический анализ",
        "topology": "топология",
        "information theory": "теория информации",
        "game theory": "теория игр",
        "control theory": "теория управления",
        "signal processing": "обработка сигналов",
        "image processing": "обработка изображений",
        "data visualization": "визуализация данных",
        "text mining": "анализ текстов",
        "sentiment analysis": "анализ тональности",
        "object detection": "обнаружение объектов",
        "autonomous robot": "автономный робот",
        "software tool": "программный инструмент",
        "software framework": "программный фреймворк",
        "programming framework": "фреймворк программирования",
        "recommender system": "рекомендательная система",
        "time series analysis": "анализ временных рядов",
        "anomaly detection": "обнаружение аномалий",
        "named entity recognition": "распознавание именованных сущностей",
        "machine translation": "машинный перевод",
        "text classification": "классификация текстов",
        "image segmentation": "сегментация изображений",
        "facial recognition": "распознавание лиц",
        "optical character recognition": "оптическое распознавание символов",
        "data warehouse": "хранилище данных",
        "version control": "система контроля версий",
        "compiler": "компилятор",
        "interpreter": "интерпретатор",
        "debugger": "отладчик",
        "integrated development environment": "интегрированная среда разработки",
        "code editor": "редактор кода",
        "package manager": "менеджер пакетов",
        "differential equation": "дифференциальное уравнение",
        "stochastic process": "случайный процесс",
        "Markov chain": "цепь Маркова",
        "Fourier transform": "преобразование Фурье",
        "convex optimization": "выпуклая оптимизация",
        "graph theory": "теория графов",
        "number theory": "теория чисел",
        "numerical analysis": "численный анализ",
        "matrix decomposition": "матричное разложение",
        "path planning algorithm": "алгоритм планирования пути",
        "swarm robotics": "роевая робототехника",
        "humanoid robot": "человекоподобный робот",
        "industrial robot": "промышленный робот",
        "question answering": "ответы на вопросы",
        "text summarization": "автоматическое реферирование",
        "language model": "языковая модель",
        "collaborative filtering": "коллаборативная фильтрация",
        "business intelligence": "бизнес-аналитика",
    }

    # Flexible matching: find the best (longest) pattern contained in description
    result = _find_best_pattern(d_clean, simple_ru)
    if result:
        return result
    result = _find_best_pattern(d, simple_ru)
    if result:
        return result

    # Pattern: "X algorithm" / "X method" / "X model" with known terms
    type_words = {
        "algorithm": "алгоритм",
        "method": "метод",
        "model": "модель",
        "technique": "метод",
        "framework": "фреймворк",
        "library": "библиотека",
        "tool": "инструмент",
        "concept in": "понятие в области",
        "metric": "метрика",
        "measure": "мера",
        "process": "процесс",
        "system": "система",
        "classifier": "классификатор",
        "function": "функция",
        "theorem": "теорема",
        "network": "сеть",
        "architecture": "архитектура",
        "programming language": "язык программирования",
        "software": "программное обеспечение",
    }

    for tw_en, tw_ru in type_words.items():
        if tw_en in d:
            for term_en, term_ru in mapping.items():
                if term_en in d and tw_en != term_en:
                    return f"{tw_ru} {_genitive(term_ru)}"

    return None


def _translate_to_de(en_label: str, en_desc: str) -> Optional[str]:
    """Rule-based translation of common AI/CS descriptions to German."""
    d = en_desc.lower().strip().rstrip(".")
    d_clean = _strip_article(d)

    simple_de = {
        "artificial intelligence": "künstliche Intelligenz",
        "machine learning": "maschinelles Lernen",
        "deep learning": "tiefes Lernen",
        "natural language processing": "Verarbeitung natürlicher Sprache",
        "computer vision": "maschinelles Sehen",
        "neural network": "neuronales Netz",
        "data mining": "Data-Mining",
        "reinforcement learning": "bestärkendes Lernen",
        "supervised learning": "überwachtes Lernen",
        "unsupervised learning": "unüberwachtes Lernen",
        "speech recognition": "Spracherkennung",
        "image recognition": "Bilderkennung",
        "robotics": "Robotik",
        "big data": "Big Data",
        "programming language": "Programmiersprache",
        "software": "Software",
        "algorithm": "Algorithmus",
        "scientific article": "wissenschaftlicher Artikel",
        "scholarly article": "wissenschaftlicher Artikel",
        "academic journal": "wissenschaftliche Zeitschrift",
        "peer-reviewed journal": "begutachtete Zeitschrift",
        "academic conference": "wissenschaftliche Konferenz",
        "research institute": "Forschungsinstitut",
        "research laboratory": "Forschungslabor",
        "computer science concept": "Konzept der Informatik",
        "mathematical concept": "mathematisches Konzept",
        "statistical method": "statistische Methode",
        "statistical model": "statistisches Modell",
        "probability distribution": "Wahrscheinlichkeitsverteilung",
        "optimization algorithm": "Optimierungsalgorithmus",
        "data structure": "Datenstruktur",
        "software library": "Softwarebibliothek",
        "open-source software": "Open-Source-Software",
        "python library": "Python-Bibliothek",
        "java library": "Java-Bibliothek",
        "loss function": "Verlustfunktion",
        "activation function": "Aktivierungsfunktion",
        "neural network architecture": "Architektur neuronaler Netze",
        "machine learning model": "Modell des maschinellen Lernens",
        "machine learning algorithm": "Algorithmus des maschinellen Lernens",
        "machine learning method": "Methode des maschinellen Lernens",
        "machine learning technique": "Technik des maschinellen Lernens",
        "deep learning model": "Deep-Learning-Modell",
        "deep learning algorithm": "Deep-Learning-Algorithmus",
        "deep learning architecture": "Deep-Learning-Architektur",
        "classification algorithm": "Klassifikationsalgorithmus",
        "clustering algorithm": "Clusteringalgorithmus",
        "regression algorithm": "Regressionsalgorithmus",
        "dimensionality reduction technique": "Verfahren zur Dimensionsreduktion",
        "feature selection method": "Methode zur Merkmalsauswahl",
        "ensemble learning method": "Ensemble-Lernmethode",
        "regularization technique": "Regularisierungsverfahren",
        "hyperparameter": "Hyperparameter",
        "benchmark dataset": "Benchmark-Datensatz",
        "dataset": "Datensatz",
        "evaluation metric": "Bewertungsmetrik",
        "scientific journal": "wissenschaftliche Zeitschrift",
        "computer program": "Computerprogramm",
        "web application": "Webanwendung",
        "mobile application": "mobile Anwendung",
        "operating system": "Betriebssystem",
        "database": "Datenbank",
        "search engine": "Suchmaschine",
        "file format": "Dateiformat",
        "communication protocol": "Kommunikationsprotokoll",
        "encryption algorithm": "Verschlüsselungsalgorithmus",
        "sorting algorithm": "Sortieralgorithmus",
        "graph algorithm": "Graphenalgorithmus",
        "numerical method": "numerisches Verfahren",
        "linear algebra": "lineare Algebra",
        "calculus": "Infinitesimalrechnung",
        "topology": "Topologie",
        "information theory": "Informationstheorie",
        "game theory": "Spieltheorie",
        "control theory": "Regelungstheorie",
        "signal processing": "Signalverarbeitung",
        "image processing": "Bildverarbeitung",
        "data visualization": "Datenvisualisierung",
        "text mining": "Text-Mining",
        "sentiment analysis": "Sentimentanalyse",
        "object detection": "Objekterkennung",
        "autonomous robot": "autonomer Roboter",
        "software tool": "Softwarewerkzeug",
        "software framework": "Software-Framework",
        "programming framework": "Programmier-Framework",
        "recommender system": "Empfehlungssystem",
        "time series analysis": "Zeitreihenanalyse",
        "anomaly detection": "Anomalieerkennung",
        "named entity recognition": "Eigennamenerkennung",
        "machine translation": "maschinelle Übersetzung",
        "text classification": "Textklassifikation",
        "image segmentation": "Bildsegmentierung",
        "facial recognition": "Gesichtserkennung",
        "optical character recognition": "optische Zeichenerkennung",
        "data warehouse": "Data-Warehouse",
        "version control": "Versionsverwaltung",
        "compiler": "Compiler",
        "interpreter": "Interpreter",
        "debugger": "Debugger",
        "integrated development environment": "integrierte Entwicklungsumgebung",
        "code editor": "Code-Editor",
        "package manager": "Paketverwaltung",
        "differential equation": "Differentialgleichung",
        "stochastic process": "stochastischer Prozess",
        "Markov chain": "Markow-Kette",
        "Fourier transform": "Fourier-Transformation",
        "convex optimization": "konvexe Optimierung",
        "graph theory": "Graphentheorie",
        "number theory": "Zahlentheorie",
        "numerical analysis": "numerische Analysis",
        "matrix decomposition": "Matrixzerlegung",
        "path planning algorithm": "Pfadplanungsalgorithmus",
        "swarm robotics": "Schwarmrobotik",
        "humanoid robot": "humanoider Roboter",
        "industrial robot": "Industrieroboter",
        "question answering": "Frage-Antwort-System",
        "text summarization": "automatische Textzusammenfassung",
        "language model": "Sprachmodell",
        "collaborative filtering": "kollaboratives Filtern",
        "business intelligence": "Business Intelligence",
    }

    # Flexible matching
    result = _find_best_pattern(d_clean, simple_de)
    if result:
        return result
    result = _find_best_pattern(d, simple_de)
    if result:
        return result

    # Pattern-based
    templates_de = {
        "branch of": "Teilgebiet von",
        "subfield of": "Teilgebiet von",
        "field of": "Bereich von",
        "area of": "Bereich von",
        "type of": "Art von",
        "class of": "Klasse von",
        "family of": "Familie von",
        "method of": "Methode von",
        "technique in": "Technik in",
        "application of": "Anwendung von",
        "form of": "Form von",
        "subset of": "Teilmenge von",
        "used in": "verwendet in",
        "used for": "verwendet für",
        "part of": "Teil von",
    }

    mapping_de = {
        "artificial intelligence": "künstlicher Intelligenz",
        "machine learning": "maschinellem Lernen",
        "deep learning": "tiefem Lernen",
        "computer science": "Informatik",
        "natural language processing": "Verarbeitung natürlicher Sprache",
        "computer vision": "maschinellem Sehen",
        "data mining": "Data-Mining",
        "statistics": "Statistik",
        "mathematics": "Mathematik",
    }

    for prefix_en, prefix_de in templates_de.items():
        if d.startswith(prefix_en) or d_clean.startswith(prefix_en):
            src = d if d.startswith(prefix_en) else d_clean
            rest = src[src.index(prefix_en) + len(prefix_en):].strip()
            for term_en, term_de in mapping_de.items():
                if term_en in rest:
                    return f"{prefix_de} {term_de}"

    return None


def _translate_to_es(en_label: str, en_desc: str) -> Optional[str]:
    """Rule-based translation of common science/tech descriptions to Spanish."""
    d = en_desc.lower().strip().rstrip(".")
    d_clean = _strip_article(d)

    simple_es = {
        "artificial intelligence": "inteligencia artificial",
        "machine learning": "aprendizaje automático",
        "deep learning": "aprendizaje profundo",
        "natural language processing": "procesamiento del lenguaje natural",
        "computer vision": "visión por computador",
        "neural network": "red neuronal",
        "data mining": "minería de datos",
        "reinforcement learning": "aprendizaje por refuerzo",
        "supervised learning": "aprendizaje supervisado",
        "unsupervised learning": "aprendizaje no supervisado",
        "speech recognition": "reconocimiento de voz",
        "image recognition": "reconocimiento de imágenes",
        "robotics": "robótica",
        "big data": "macrodatos",
        "programming language": "lenguaje de programación",
        "software": "software",
        "algorithm": "algoritmo",
        "scientific article": "artículo científico",
        "scholarly article": "artículo científico",
        "academic journal": "revista científica",
        "peer-reviewed journal": "revista revisada por pares",
        "academic conference": "conferencia científica",
        "research institute": "instituto de investigación",
        "research laboratory": "laboratorio de investigación",
        "computer science concept": "concepto de informática",
        "mathematical concept": "concepto matemático",
        "statistical method": "método estadístico",
        "statistical model": "modelo estadístico",
        "probability distribution": "distribución de probabilidad",
        "optimization algorithm": "algoritmo de optimización",
        "data structure": "estructura de datos",
        "software library": "biblioteca de software",
        "open-source software": "software de código abierto",
        "python library": "biblioteca de Python",
        "java library": "biblioteca de Java",
        "loss function": "función de pérdida",
        "activation function": "función de activación",
        "neural network architecture": "arquitectura de red neuronal",
        "machine learning model": "modelo de aprendizaje automático",
        "machine learning algorithm": "algoritmo de aprendizaje automático",
        "machine learning method": "método de aprendizaje automático",
        "machine learning technique": "técnica de aprendizaje automático",
        "deep learning model": "modelo de aprendizaje profundo",
        "deep learning algorithm": "algoritmo de aprendizaje profundo",
        "deep learning architecture": "arquitectura de aprendizaje profundo",
        "classification algorithm": "algoritmo de clasificación",
        "clustering algorithm": "algoritmo de agrupamiento",
        "regression algorithm": "algoritmo de regresión",
        "dimensionality reduction technique": "técnica de reducción de dimensionalidad",
        "feature selection method": "método de selección de características",
        "ensemble learning method": "método de aprendizaje conjunto",
        "regularization technique": "técnica de regularización",
        "hyperparameter": "hiperparámetro",
        "benchmark dataset": "conjunto de datos de referencia",
        "dataset": "conjunto de datos",
        "evaluation metric": "métrica de evaluación",
        "scientific journal": "revista científica",
        "computer program": "programa informático",
        "web application": "aplicación web",
        "mobile application": "aplicación móvil",
        "operating system": "sistema operativo",
        "database": "base de datos",
        "search engine": "motor de búsqueda",
        "file format": "formato de archivo",
        "communication protocol": "protocolo de comunicación",
        "encryption algorithm": "algoritmo de cifrado",
        "sorting algorithm": "algoritmo de ordenamiento",
        "graph algorithm": "algoritmo de grafos",
        "numerical method": "método numérico",
        "linear algebra": "álgebra lineal",
        "calculus": "cálculo",
        "topology": "topología",
        "information theory": "teoría de la información",
        "game theory": "teoría de juegos",
        "control theory": "teoría de control",
        "signal processing": "procesamiento de señales",
        "image processing": "procesamiento de imágenes",
        "data visualization": "visualización de datos",
        "text mining": "minería de textos",
        "sentiment analysis": "análisis de sentimientos",
        "object detection": "detección de objetos",
        "autonomous robot": "robot autónomo",
        "software tool": "herramienta de software",
        "software framework": "marco de software",
        "programming framework": "marco de programación",
        "recommender system": "sistema de recomendación",
        "time series analysis": "análisis de series temporales",
        "anomaly detection": "detección de anomalías",
        "named entity recognition": "reconocimiento de entidades nombradas",
        "machine translation": "traducción automática",
        "text classification": "clasificación de textos",
        "image segmentation": "segmentación de imágenes",
        "facial recognition": "reconocimiento facial",
        "optical character recognition": "reconocimiento óptico de caracteres",
        "data warehouse": "almacén de datos",
        "version control": "control de versiones",
        "compiler": "compilador",
        "interpreter": "intérprete",
        "debugger": "depurador",
        "integrated development environment": "entorno de desarrollo integrado",
        "code editor": "editor de código",
        "package manager": "gestor de paquetes",
        "language model": "modelo de lenguaje",
        "collaborative filtering": "filtrado colaborativo",
        "business intelligence": "inteligencia empresarial",
    }

    # Flexible matching
    result = _find_best_pattern(d_clean, simple_es)
    if result:
        return result
    result = _find_best_pattern(d, simple_es)
    if result:
        return result

    templates_es = {
        "branch of": "rama de",
        "subfield of": "subcampo de",
        "field of": "campo de",
        "area of": "área de",
        "type of": "tipo de",
        "class of": "clase de",
        "family of": "familia de",
        "method of": "método de",
        "technique in": "técnica en",
        "application of": "aplicación de",
        "form of": "forma de",
        "subset of": "subconjunto de",
        "used in": "utilizado en",
        "used for": "utilizado para",
        "part of": "parte de",
    }

    mapping_es = {
        "artificial intelligence": "inteligencia artificial",
        "machine learning": "aprendizaje automático",
        "deep learning": "aprendizaje profundo",
        "computer science": "informática",
        "natural language processing": "procesamiento del lenguaje natural",
        "computer vision": "visión por computador",
        "data mining": "minería de datos",
        "statistics": "estadística",
        "mathematics": "matemáticas",
    }

    for prefix_en, prefix_es in templates_es.items():
        if d.startswith(prefix_en) or d_clean.startswith(prefix_en):
            src = d if d.startswith(prefix_en) else d_clean
            rest = src[src.index(prefix_en) + len(prefix_en):].strip()
            for term_en, term_es in mapping_es.items():
                if term_en in rest:
                    return f"{prefix_es} {term_es}"

    return None


def _translate_to_fr(en_label: str, en_desc: str) -> Optional[str]:
    """Rule-based translation of common science/tech descriptions to French."""
    d = en_desc.lower().strip().rstrip(".")
    d_clean = _strip_article(d)

    simple_fr = {
        "artificial intelligence": "intelligence artificielle",
        "machine learning": "apprentissage automatique",
        "deep learning": "apprentissage profond",
        "natural language processing": "traitement automatique du langage naturel",
        "computer vision": "vision par ordinateur",
        "neural network": "réseau de neurones",
        "data mining": "exploration de données",
        "reinforcement learning": "apprentissage par renforcement",
        "supervised learning": "apprentissage supervisé",
        "unsupervised learning": "apprentissage non supervisé",
        "speech recognition": "reconnaissance vocale",
        "image recognition": "reconnaissance d'images",
        "robotics": "robotique",
        "big data": "mégadonnées",
        "programming language": "langage de programmation",
        "software": "logiciel",
        "algorithm": "algorithme",
        "scientific article": "article scientifique",
        "scholarly article": "article scientifique",
        "academic journal": "revue scientifique",
        "peer-reviewed journal": "revue à comité de lecture",
        "academic conference": "conférence scientifique",
        "research institute": "institut de recherche",
        "research laboratory": "laboratoire de recherche",
        "computer science concept": "concept d'informatique",
        "mathematical concept": "concept mathématique",
        "statistical method": "méthode statistique",
        "statistical model": "modèle statistique",
        "probability distribution": "loi de probabilité",
        "optimization algorithm": "algorithme d'optimisation",
        "data structure": "structure de données",
        "software library": "bibliothèque logicielle",
        "open-source software": "logiciel libre",
        "python library": "bibliothèque Python",
        "java library": "bibliothèque Java",
        "loss function": "fonction de perte",
        "activation function": "fonction d'activation",
        "neural network architecture": "architecture de réseau neuronal",
        "machine learning model": "modèle d'apprentissage automatique",
        "machine learning algorithm": "algorithme d'apprentissage automatique",
        "machine learning method": "méthode d'apprentissage automatique",
        "machine learning technique": "technique d'apprentissage automatique",
        "deep learning model": "modèle d'apprentissage profond",
        "deep learning algorithm": "algorithme d'apprentissage profond",
        "deep learning architecture": "architecture d'apprentissage profond",
        "classification algorithm": "algorithme de classification",
        "clustering algorithm": "algorithme de partitionnement",
        "regression algorithm": "algorithme de régression",
        "dimensionality reduction technique": "technique de réduction de dimensionnalité",
        "feature selection method": "méthode de sélection de caractéristiques",
        "ensemble learning method": "méthode d'apprentissage ensembliste",
        "regularization technique": "technique de régularisation",
        "hyperparameter": "hyperparamètre",
        "benchmark dataset": "jeu de données de référence",
        "dataset": "jeu de données",
        "evaluation metric": "métrique d'évaluation",
        "scientific journal": "revue scientifique",
        "computer program": "programme informatique",
        "web application": "application web",
        "mobile application": "application mobile",
        "operating system": "système d'exploitation",
        "database": "base de données",
        "search engine": "moteur de recherche",
        "file format": "format de fichier",
        "communication protocol": "protocole de communication",
        "encryption algorithm": "algorithme de chiffrement",
        "sorting algorithm": "algorithme de tri",
        "graph algorithm": "algorithme de graphes",
        "numerical method": "méthode numérique",
        "linear algebra": "algèbre linéaire",
        "calculus": "calcul infinitésimal",
        "topology": "topologie",
        "information theory": "théorie de l'information",
        "game theory": "théorie des jeux",
        "control theory": "théorie du contrôle",
        "signal processing": "traitement du signal",
        "image processing": "traitement d'images",
        "data visualization": "visualisation de données",
        "text mining": "fouille de textes",
        "sentiment analysis": "analyse de sentiments",
        "object detection": "détection d'objets",
        "autonomous robot": "robot autonome",
        "software tool": "outil logiciel",
        "software framework": "cadriciel",
        "programming framework": "cadriciel de programmation",
        "recommender system": "système de recommandation",
        "time series analysis": "analyse de séries temporelles",
        "anomaly detection": "détection d'anomalies",
        "named entity recognition": "reconnaissance d'entités nommées",
        "machine translation": "traduction automatique",
        "text classification": "classification de textes",
        "image segmentation": "segmentation d'images",
        "facial recognition": "reconnaissance faciale",
        "optical character recognition": "reconnaissance optique de caractères",
        "data warehouse": "entrepôt de données",
        "version control": "gestion de versions",
        "compiler": "compilateur",
        "interpreter": "interpréteur",
        "debugger": "débogueur",
        "integrated development environment": "environnement de développement intégré",
        "code editor": "éditeur de code",
        "package manager": "gestionnaire de paquets",
        "language model": "modèle de langage",
        "collaborative filtering": "filtrage collaboratif",
        "business intelligence": "informatique décisionnelle",
    }

    # Flexible matching
    result = _find_best_pattern(d_clean, simple_fr)
    if result:
        return result
    result = _find_best_pattern(d, simple_fr)
    if result:
        return result

    templates_fr = {
        "branch of": "branche de",
        "subfield of": "sous-domaine de",
        "field of": "domaine de",
        "area of": "domaine de",
        "type of": "type de",
        "class of": "classe de",
        "family of": "famille de",
        "method of": "méthode de",
        "technique in": "technique en",
        "application of": "application de",
        "form of": "forme de",
        "subset of": "sous-ensemble de",
        "used in": "utilisé en",
        "used for": "utilisé pour",
        "part of": "partie de",
    }

    mapping_fr = {
        "artificial intelligence": "l'intelligence artificielle",
        "machine learning": "l'apprentissage automatique",
        "deep learning": "l'apprentissage profond",
        "computer science": "l'informatique",
        "natural language processing": "traitement automatique du langage naturel",
        "computer vision": "la vision par ordinateur",
        "data mining": "l'exploration de données",
        "statistics": "la statistique",
        "mathematics": "les mathématiques",
    }

    for prefix_en, prefix_fr in templates_fr.items():
        if d.startswith(prefix_en) or d_clean.startswith(prefix_en):
            src = d if d.startswith(prefix_en) else d_clean
            rest = src[src.index(prefix_en) + len(prefix_en):].strip()
            for term_en, term_fr in mapping_fr.items():
                if term_en in rest:
                    return f"{prefix_fr} {term_fr}"

    return None


def _translate_to_pt(en_label: str, en_desc: str) -> Optional[str]:
    """Rule-based translation of common science/tech descriptions to Portuguese."""
    d = en_desc.lower().strip().rstrip(".")
    d_clean = _strip_article(d)

    simple_pt = {
        "artificial intelligence": "inteligência artificial",
        "machine learning": "aprendizado de máquina",
        "deep learning": "aprendizado profundo",
        "natural language processing": "processamento de linguagem natural",
        "computer vision": "visão computacional",
        "neural network": "rede neural",
        "data mining": "mineração de dados",
        "reinforcement learning": "aprendizado por reforço",
        "supervised learning": "aprendizado supervisionado",
        "unsupervised learning": "aprendizado não supervisionado",
        "speech recognition": "reconhecimento de fala",
        "image recognition": "reconhecimento de imagens",
        "robotics": "robótica",
        "big data": "megadados",
        "programming language": "linguagem de programação",
        "software": "software",
        "algorithm": "algoritmo",
        "scientific article": "artigo científico",
        "scholarly article": "artigo científico",
        "academic journal": "revista científica",
        "peer-reviewed journal": "revista com revisão por pares",
        "academic conference": "conferência científica",
        "research institute": "instituto de pesquisa",
        "research laboratory": "laboratório de pesquisa",
        "computer science concept": "conceito de ciência da computação",
        "mathematical concept": "conceito matemático",
        "statistical method": "método estatístico",
        "statistical model": "modelo estatístico",
        "probability distribution": "distribuição de probabilidade",
        "optimization algorithm": "algoritmo de otimização",
        "data structure": "estrutura de dados",
        "software library": "biblioteca de software",
        "open-source software": "software de código aberto",
        "python library": "biblioteca Python",
        "java library": "biblioteca Java",
        "loss function": "função de perda",
        "activation function": "função de ativação",
        "neural network architecture": "arquitetura de rede neural",
        "machine learning model": "modelo de aprendizado de máquina",
        "machine learning algorithm": "algoritmo de aprendizado de máquina",
        "machine learning method": "método de aprendizado de máquina",
        "machine learning technique": "técnica de aprendizado de máquina",
        "deep learning model": "modelo de aprendizado profundo",
        "deep learning algorithm": "algoritmo de aprendizado profundo",
        "deep learning architecture": "arquitetura de aprendizado profundo",
        "classification algorithm": "algoritmo de classificação",
        "clustering algorithm": "algoritmo de agrupamento",
        "regression algorithm": "algoritmo de regressão",
        "dimensionality reduction technique": "técnica de redução de dimensionalidade",
        "feature selection method": "método de seleção de características",
        "ensemble learning method": "método de aprendizado em conjunto",
        "regularization technique": "técnica de regularização",
        "hyperparameter": "hiperparâmetro",
        "benchmark dataset": "conjunto de dados de referência",
        "dataset": "conjunto de dados",
        "evaluation metric": "métrica de avaliação",
        "scientific journal": "revista científica",
        "computer program": "programa de computador",
        "web application": "aplicação web",
        "mobile application": "aplicação móvel",
        "operating system": "sistema operacional",
        "database": "banco de dados",
        "search engine": "motor de busca",
        "file format": "formato de arquivo",
        "communication protocol": "protocolo de comunicação",
        "encryption algorithm": "algoritmo de criptografia",
        "sorting algorithm": "algoritmo de ordenação",
        "graph algorithm": "algoritmo de grafos",
        "numerical method": "método numérico",
        "linear algebra": "álgebra linear",
        "calculus": "cálculo",
        "topology": "topologia",
        "information theory": "teoria da informação",
        "game theory": "teoria dos jogos",
        "control theory": "teoria de controle",
        "signal processing": "processamento de sinais",
        "image processing": "processamento de imagens",
        "data visualization": "visualização de dados",
        "text mining": "mineração de textos",
        "sentiment analysis": "análise de sentimentos",
        "object detection": "detecção de objetos",
        "autonomous robot": "robô autônomo",
        "software tool": "ferramenta de software",
        "software framework": "framework de software",
        "programming framework": "framework de programação",
        "recommender system": "sistema de recomendação",
        "time series analysis": "análise de séries temporais",
        "anomaly detection": "detecção de anomalias",
        "named entity recognition": "reconhecimento de entidades nomeadas",
        "machine translation": "tradução automática",
        "text classification": "classificação de textos",
        "image segmentation": "segmentação de imagens",
        "facial recognition": "reconhecimento facial",
        "optical character recognition": "reconhecimento óptico de caracteres",
        "data warehouse": "armazém de dados",
        "version control": "controle de versão",
        "compiler": "compilador",
        "interpreter": "interpretador",
        "debugger": "depurador",
        "integrated development environment": "ambiente de desenvolvimento integrado",
        "code editor": "editor de código",
        "package manager": "gerenciador de pacotes",
        "language model": "modelo de linguagem",
        "collaborative filtering": "filtragem colaborativa",
        "business intelligence": "inteligência empresarial",
    }

    # Flexible matching
    result = _find_best_pattern(d_clean, simple_pt)
    if result:
        return result
    result = _find_best_pattern(d, simple_pt)
    if result:
        return result

    templates_pt = {
        "branch of": "ramo de",
        "subfield of": "subcampo de",
        "field of": "campo de",
        "area of": "área de",
        "type of": "tipo de",
        "class of": "classe de",
        "family of": "família de",
        "method of": "método de",
        "technique in": "técnica em",
        "application of": "aplicação de",
        "form of": "forma de",
        "subset of": "subconjunto de",
        "used in": "utilizado em",
        "used for": "utilizado para",
        "part of": "parte de",
    }

    mapping_pt = {
        "artificial intelligence": "inteligência artificial",
        "machine learning": "aprendizado de máquina",
        "deep learning": "aprendizado profundo",
        "computer science": "ciência da computação",
        "natural language processing": "processamento de linguagem natural",
        "computer vision": "visão computacional",
        "data mining": "mineração de dados",
        "statistics": "estatística",
        "mathematics": "matemática",
    }

    for prefix_en, prefix_pt in templates_pt.items():
        if d.startswith(prefix_en) or d_clean.startswith(prefix_en):
            src = d if d.startswith(prefix_en) else d_clean
            rest = src[src.index(prefix_en) + len(prefix_en):].strip()
            for term_en, term_pt in mapping_pt.items():
                if term_en in rest:
                    return f"{prefix_pt} {term_pt}"

    return None


def _genitive(ru_term: str) -> str:
    """Very rough genitive approximation for common AI terms in Russian."""
    genitives = {
        "искусственный интеллект": "искусственного интеллекта",
        "машинное обучение": "машинного обучения",
        "глубокое обучение": "глубокого обучения",
        "обработка естественного языка": "обработки естественного языка",
        "компьютерное зрение": "компьютерного зрения",
        "нейронная сеть": "нейронных сетей",
        "обучение с подкреплением": "обучения с подкреплением",
        "обучение с учителем": "обучения с учителем",
        "обучение без учителя": "обучения без учителя",
        "распознавание речи": "распознавания речи",
        "распознавание изображений": "распознавания изображений",
        "робототехника": "робототехники",
        "большие данные": "больших данных",
        "программное обеспечение": "программного обеспечения",
        "интеллектуальный анализ данных": "интеллектуального анализа данных",
    }
    return genitives.get(ru_term, ru_term)


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------
def collect_candidates(ws: WikidataSession, langs: list[str], needed: int,
                       use_llm: bool = False) -> list[tuple[str, str, str]]:
    """
    Collect (qid, lang, proposed_description) tuples for items missing descriptions.
    Returns up to `needed` candidates.

    Strategy:
    1. Try SPARQL first (fast, targeted — finds items WITHOUT descriptions)
    2. Fall back to search+backlinks if SPARQL fails (429/timeout)
    3. Use flexible matching on results
    """
    candidates = []
    seen = set()  # (qid, lang) pairs

    # -----------------------------------------------------------
    # Phase 1: SPARQL-based search (preferred)
    # -----------------------------------------------------------
    log.info("Phase 1: SPARQL-based candidate search...")
    sparql_success = False

    for lang in langs:
        if len(candidates) >= needed:
            break

        log.info("Searching SPARQL for items missing [%s] descriptions...", lang)
        sparql_items = sparql_find_candidates(ws, lang, needed=needed - len(candidates))

        if sparql_items:
            sparql_success = True

        for qid, en_label, en_desc in sparql_items:
            if len(candidates) >= needed:
                break
            if (qid, lang) in seen:
                continue

            proposed = generate_description(en_label, en_desc, lang, use_llm=use_llm)
            if proposed:
                seen.add((qid, lang))
                candidates.append((qid, lang, proposed))
                log.info(
                    "  Candidate: %s [%s] -- %s (en: %s)",
                    qid, lang, proposed, en_desc,
                )

    if len(candidates) >= needed:
        return candidates[:needed]

    # -----------------------------------------------------------
    # Phase 2: Fallback — search + backlinks (if SPARQL insufficient)
    # -----------------------------------------------------------
    if not sparql_success or len(candidates) < needed:
        log.info("Phase 2: Fallback search+backlinks approach...")
        all_qids = list(AI_TOPICS)

        search_terms = [
            "machine learning", "neural network", "deep learning",
            "natural language processing", "computer vision",
            "artificial intelligence algorithm", "classification algorithm",
            "clustering algorithm", "reinforcement learning",
            "transformer model", "scientific journal", "academic journal",
            "python library", "open source software", "programming framework",
            "algorithm", "optimization method", "statistical model",
            "data visualization", "recommender system", "anomaly detection",
            "text mining", "sentiment analysis", "machine translation",
            "image segmentation", "object detection", "language model",
        ]

        log.info("Searching for candidate items via API...")
        for term in search_terms:
            if len(all_qids) > needed * 10:
                break
            try:
                results = ws.search_items(term, limit=20)
                all_qids.extend(results)
                time.sleep(0.5)
            except Exception as e:
                log.warning("Search failed for '%s': %s", term, e)

        # Backlink sources
        BACKLINK_SOURCES = AI_TOPICS[:5] + [
            "Q41298", "Q3918", "Q170730", "Q14116",
        ]
        for root_qid in BACKLINK_SOURCES:
            if len(all_qids) > needed * 10:
                break
            try:
                backlinks = ws.get_backlinks(root_qid, limit=50)
                all_qids.extend(backlinks)
                time.sleep(0.5)
            except Exception as e:
                log.warning("Backlinks failed for %s: %s", root_qid, e)

        # Deduplicate
        unique_qids = []
        seen_qids = set()
        for qid in all_qids:
            if qid not in seen_qids:
                seen_qids.add(qid)
                unique_qids.append(qid)

        log.info("Collected %d unique QIDs to check", len(unique_qids))

        # Check in batches of 50
        for i in range(0, len(unique_qids), 50):
            if len(candidates) >= needed:
                break
            batch = unique_qids[i:i + 50]
            try:
                entities = ws.get_entities(batch, props="descriptions|labels")
            except Exception as e:
                log.warning("Failed to fetch batch: %s", e)
                continue

            for qid, entity in entities.items():
                if len(candidates) >= needed:
                    break
                if "missing" in entity:
                    continue

                labels = entity.get("labels", {})
                descriptions = entity.get("descriptions", {})

                en_label = labels.get("en", {}).get("value", "")
                en_desc = descriptions.get("en", {}).get("value", "")

                if not en_label or not en_desc:
                    continue

                for lang in langs:
                    if len(candidates) >= needed:
                        break
                    if lang in descriptions:
                        continue  # already has description in this language
                    if (qid, lang) in seen:
                        continue

                    proposed = generate_description(en_label, en_desc, lang, use_llm=use_llm)
                    if proposed:
                        seen.add((qid, lang))
                        candidates.append((qid, lang, proposed))
                        log.info(
                            "  Candidate: %s [%s] -- %s (en: %s)",
                            qid, lang, proposed, en_desc,
                        )

            time.sleep(0.5)

    return candidates[:needed]


def _check_reverts(ws) -> int:
    """Check if any of our recent edits were reverted by other users."""
    try:
        r = ws.session.get(API_URL, params={
            "action": "query", "list": "usercontribs",
            "ucuser": BOT_USER.split("@")[0],
            "uclimit": "50", "ucprop": "title|timestamp",
            "format": "json",
        })
        our_edits = r.json().get("query", {}).get("usercontribs", [])
        our_qids = set(c["title"] for c in our_edits[:30])

        # Check if any of those items were edited by someone else AFTER us
        revert_count = 0
        for qid in list(our_qids)[:10]:
            r2 = ws.session.get(API_URL, params={
                "action": "query", "titles": qid,
                "prop": "revisions", "rvprop": "user",
                "rvlimit": "3", "format": "json",
            })
            pages = r2.json().get("query", {}).get("pages", {})
            for pid, page in pages.items():
                revs = page.get("revisions", [])
                if len(revs) >= 2:
                    last_user = revs[0].get("user", "")
                    prev_user = revs[1].get("user", "")
                    if prev_user == BOT_USER.split("@")[0] and last_user != prev_user:
                        revert_count += 1
                        log.warning("  Revert detected: %s edited by %s after us", qid, last_user)
            import time
            time.sleep(0.5)
        return revert_count
    except Exception:
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Add missing descriptions to AI-related Wikidata items"
    )
    parser.add_argument(
        "--count", type=int, default=50,
        help="Number of edits to make (default: 50)"
    )
    parser.add_argument(
        "--lang", type=str, default="ru",
        help="Comma-separated language codes, e.g. ru,de,uk,pl (default: ru)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't actually edit, just show what would be done"
    )
    parser.add_argument(
        "--use-llm", action="store_true",
        help="Use local Ollama LLM for translation (covers all languages)"
    )
    parser.add_argument(
        "--geometric", action="store_true",
        help="Auto-scale --count by +20%%/day from geometric start date"
    )
    args = parser.parse_args()

    # Geometric progression: yesterday's edits × 1.2, divided by 4 runs/day
    if args.geometric:
        from datetime import datetime, timedelta
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        today_start = datetime.utcnow().strftime("%Y-%m-%dT00:00:00Z")
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
            yesterday_edits = 50  # bootstrap minimum
        runs_per_day = 6
        max_daily = 400  # cap: no bot flag yet, stay under radar
        daily_target = min(int(yesterday_edits * 1.2), max_daily)
        args.count = max(10, daily_target // runs_per_day)
        log.info("Geometric mode: yesterday=%d, today_target=%d, per_run=%d",
                 yesterday_edits, daily_target, args.count)

    langs = [l.strip() for l in args.lang.split(",")]
    log.info("Target languages: %s, edits: %d, use-llm: %s", langs, args.count, args.use_llm)

    ws = WikidataSession()

    if not args.dry_run:
        ws.login()
        ws.get_csrf_token()

    candidates = collect_candidates(ws, langs, args.count, use_llm=args.use_llm)

    if not candidates:
        log.warning("No candidates found. Try different languages or --use-llm flag.")
        return

    log.info("Found %d candidates, starting edits...", len(candidates))

    # Re-acquire CSRF token after SPARQL queries (different domain may invalidate cookies)
    if not args.dry_run:
        log.info("Re-acquiring CSRF token before edits...")
        ws.get_csrf_token()

    edits_done = 0

    for qid, lang, description in candidates:
        if args.dry_run:
            log.info("[DRY RUN] Would set %s [%s] = %s", qid, lang, description)
            edits_done += 1
            continue

        try:
            result = ws.set_description(qid, lang, description)

            if "error" in result:
                error = result["error"]
                code = error.get("code", "")
                info = error.get("info", "")
                if code == "maxlag":
                    log.warning("Maxlag hit, waiting 10s...")
                    time.sleep(10)
                    # Retry once
                    result = ws.set_description(qid, lang, description)
                    if "error" in result:
                        log.error("Retry failed for %s: %s", qid, result["error"])
                        continue
                elif code in ("modification-failed", "failed-save") or "already has" in info.lower():
                    # Log full error for debugging
                    messages = error.get("messages", [])
                    detail = ""
                    for msg in messages:
                        if isinstance(msg, dict):
                            detail += msg.get("name", "") + ": " + str(msg.get("parameters", {}).get("1", "")) + " "
                    log.info("Skipping %s [%s] -- %s %s", qid, lang, code, detail.strip())
                    continue
                elif code == "badtoken":
                    log.warning("Bad CSRF token, re-acquiring...")
                    ws.get_csrf_token()
                    result = ws.set_description(qid, lang, description)
                    if "error" not in result:
                        edits_done += 1
                        log.info("Retry OK: %s [%s] = %s", qid, lang, description)
                    continue
                else:
                    log.error("Error on %s [%s]: %s -- %s", qid, lang, code, info)
                    continue

            edits_done += 1
            log.info(
                "EDIT %d/%d: %s [%s] = \"%s\"",
                edits_done, args.count, qid, lang, description,
            )

        except requests.exceptions.RequestException as e:
            log.error("Request failed for %s: %s", qid, e)
            continue

        # Check for reverts on our recent edits (safety)
        if edits_done > 0 and edits_done % 5 == 0:
            reverts = _check_reverts(ws)
            if reverts >= 2:
                log.error("SAFETY STOP: %d reverts detected. Stopping to prevent damage.", reverts)
                log.error("  Review: https://www.wikidata.org/wiki/Special:Contributions/Maris_Dreshmanis")
                break

        # Anti-spam delay: 3-5 seconds
        delay = random.uniform(3, 5)
        log.info("  Sleeping %.1fs...", delay)
        time.sleep(delay)

        # Every 10 edits: check abuse log for new hits
        if edits_done > 0 and edits_done % 10 == 0 and not args.dry_run:
            hits = ws.check_abuse_log(limit=5)
            # Check if any hit is recent (within last 10 minutes)
            recent_hits = []
            for h in hits:
                ts = h.get("timestamp", "")
                if ts:
                    from datetime import datetime
                    try:
                        hit_time = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
                        age_min = (datetime.utcnow() - hit_time).total_seconds() / 60
                        if age_min < 10:
                            recent_hits.append(h)
                    except ValueError:
                        pass
            if recent_hits:
                log.error("🛑 EMERGENCY STOP: %d NEW abuse filter hits in last 10 min!", len(recent_hits))
                log.error("  Stopping to prevent account damage.")
                log.error("  Review: https://www.wikidata.org/wiki/Special:AbuseLog?wpSearchUser=Maris+Dreshmanis")
                break

    log.info("Done. %d edits completed.", edits_done)

    # Post-run: final abuse log check
    if not args.dry_run and edits_done > 0:
        log.info("Final abuse filter check...")
        hits = ws.check_abuse_log(limit=5)
        recent_hits = []
        for h in hits:
            ts = h.get("timestamp", "")
            if ts:
                from datetime import datetime
                try:
                    hit_time = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
                    age_min = (datetime.utcnow() - hit_time).total_seconds() / 60
                    if age_min < 30:
                        recent_hits.append(h)
                except ValueError:
                    pass
        if recent_hits:
            log.warning("⚠️  %d abuse filter hits in last 30 min! Review at:", len(recent_hits))
            log.warning("  https://www.wikidata.org/wiki/Special:AbuseLog?wpSearchUser=Maris+Dreshmanis")
        else:
            log.info("✅ No recent abuse filter hits. Clean run.")


if __name__ == "__main__":
    main()
