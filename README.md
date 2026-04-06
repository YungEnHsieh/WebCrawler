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

## Repo Layout

- [`seeds/urls.txt`](/Users/enoch/Desktop/WebCrawler/seeds/urls.txt): tracked seed URLs
- [`data/ipc`](/Users/enoch/Desktop/WebCrawler/data/ipc): runtime crawl queues, results, stats
- [`data/postgres`](/Users/enoch/Desktop/WebCrawler/data/postgres): runtime PostgreSQL data directory

`data/ipc` and `data/postgres` are runtime-only and are ignored by git.
