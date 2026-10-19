# data-processor
# Event Pipeline

Asynchronous pipeline for processing events from Telegram with enrichment from external APIs and notifications.

## Features

- Event ingestion from Telegram (Telethon)
- Regex-based parsing
- Enrichment from external API (aiohttp)
- Rule engine (threshold rules)
- Notifications to Telegram
- SQLite storage
- Excel export
- HTTP health-check
- Docker + docker-compose
- pytest + CI

## Tech Stack

- Python 3.11+
- Telethon, aiohttp
- SQLite, pandas
- asyncio
- pytest, Docker

## Quick Start

```bash
git clone https://github.com/yourname/event-pipeline.git
cd event-pipeline
cp .env.example .env
pip install -r requirements.txt
python -m src.app --dry-run
