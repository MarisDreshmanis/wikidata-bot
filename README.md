# Wikidata Bot

Semi-automated tools for improving multilingual coverage on [Wikidata](https://www.wikidata.org/).

**Operator:** [Maris Dreshmanis](https://www.wikidata.org/wiki/User:Maris_Dreshmanis)

## What it does

### Task 1: Multilingual descriptions
Adds missing descriptions in Russian, Spanish, French, and Portuguese for science, technology, and academic items using **deterministic rule-based pattern matching** from existing English descriptions. No AI/LLM is used.

### Task 2: Latvian labels and descriptions
Adds Latvian (lv) labels and descriptions using a **curated dictionary** of 2,800+ verified translation pairs extracted from Wikidata's own existing translations. No machine translation.

### Task 3: Verified references
Adds references (P248 "stated in") to unreferenced biographical claims (birth/death dates) after **cross-verification against 3+ independent authority databases** (VIAF, OpenLibrary). A reference is added only when all sources agree with zero conflicts.

## Technical details

- **Language:** Python 3
- **API:** MediaWiki Action API directly (no pywikibot)
- **Rate limiting:** maxlag=5, 3-5s random delay between edits
- **Safety:** abuse filter monitoring, automatic stop on reverts
- **AI/LLM:** None in production. All outputs are deterministic.

## Setup

```bash
# Clone
git clone https://github.com/MarisDreshmanis/wikidata-bot.git
cd wikidata-bot

# Configure credentials
cp .env.example .env
# Edit .env with your bot username and password

# Run (dry-run first!)
python3 warmup_bot.py --count 10 --lang ru,es --dry-run
python3 references_bot.py --count 5 --dry-run
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `WIKIDATA_BOT_USER` | Bot username (e.g., `YourName@BotPassword`) |
| `WIKIDATA_BOT_PASS` | Bot password from Special:BotPasswords |

## License

MIT
