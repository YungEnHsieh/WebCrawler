# WebCrawler

Distributed crawler pipeline backed by PostgreSQL, Scrapy, and filesystem IPC.

## Prerequisites

- Docker
- Docker Compose
- GNU Make
- Python 3

## Quick Start

Clone the repo, then initialize the database and seed URLs:

```bash
make bootstrap
```

This will:

1. Start PostgreSQL
2. Create the DB schema
3. Seed URLs from [`seeds/urls.txt`](/Users/enoch/Desktop/WebCrawler/seeds/urls.txt)
4. Start the full compose stack

To start the stack again later without re-seeding:

```bash
make up
```

## Run Multiple Instances On One Machine

This project can coexist with another instance on the same host, but each stack needs its own:

- Compose project name
- PostgreSQL host port
- Runtime data directories

Copy the example env file and adjust the values before starting a second stack:

```bash
cp .env.example .env
```

Example `.env` for a second instance:

```dotenv
COMPOSE_PROJECT_NAME=webcrawler_b
POSTGRES_PORT=55432
POSTGRES_HOST_DIR=./runtime/webcrawler_b/postgres
IPC_HOST_DIR=./runtime/webcrawler_b/ipc
```

Docker Compose will read `.env` automatically, and `make` will use the same values for commands like `make bootstrap`, `make up`, and `make summarize-crawl`.

## Common Commands

Start only PostgreSQL:

```bash
make up-postgres
```

Switch crawler to fixed QPS mode:

```bash
make crawler-fixed
```

Switch crawler to AutoThrottle mode:

```bash
make crawler-autothrottle
```

Summarize the last hour of crawl results:

```bash
make summarize-crawl
```

Generate an offline HTML report with crawl volume and domain QPS charts:

```bash
make plot-crawl-metrics
```

Preview which `ingestor_*` buckets are safe to delete after both ingestor and extractor have processed them:

```bash
make cleanup-ingestor-buckets
```

Actually delete those processed `ingestor_*` buckets:

```bash
make cleanup-ingestor-buckets-apply
```

To chart specific domains instead of the default top domains, run the script directly:

```bash
python3 scripts/plot_crawl_metrics.py \
  --root data/ipc/crawl_result \
  --minutes 60 \
  --bucket-seconds 60 \
  --domain wikipedia.org \
  --domain reddit.com \
  --output tmp/crawl_metrics.html
```

## Repo Layout

- [`seeds/urls.txt`](/Users/enoch/Desktop/WebCrawler/seeds/urls.txt): tracked seed URLs
- [`data/ipc`](/Users/enoch/Desktop/WebCrawler/data/ipc): runtime crawl queues, results, stats
- [`data/postgres`](/Users/enoch/Desktop/WebCrawler/data/postgres): runtime PostgreSQL data directory

`data/ipc` and `data/postgres` are runtime-only and are ignored by git.

If you override `POSTGRES_HOST_DIR` or `IPC_HOST_DIR`, runtime data will go to those paths instead.
