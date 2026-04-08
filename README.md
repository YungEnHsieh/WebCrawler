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

`make bootstrap` supports the same variables as [`scripts/bootstrap.sh`](/Users/enoch/Desktop/WebCrawler/scripts/bootstrap.sh):

- `DSN`: PostgreSQL DSN used by `init_db.py`, `seed_urls.py`, and `repair_crawl_flags.py`. Default: `postgresql+psycopg2://crawler:crawler@postgres:5432/crawlerdb`
- `URL_FILE`: Seed file path as seen inside the container. Default: `/app/seeds/urls.txt`
- `NUM_SHARDS`: Total shard count used for schema init and seed routing. Default: `256`
- `SHARDS_PER_INGESTOR`: Routing fanout used while seeding URLs. Default: `16`
- `INGEST_SERVICE`: Compose service name used to run init/seed/repair commands. Default: `scheduler_ingest`

Example:

```bash
make bootstrap URL_FILE=/app/seeds/urls.txt NUM_SHARDS=128 SHARDS_PER_INGESTOR=8
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

`make summarize-crawl` parameters:

- `RESULT_MINUTES`: Look back this many minutes from now. Default: `60`
- `RESULT_LABEL`: Label printed in summary output. Default: `run`

Example:

```bash
make summarize-crawl RESULT_MINUTES=15 RESULT_LABEL=smoke
```

## Lightweight Experiment Commands

Run a lightweight fixed-QPS experiment against a small seed subset without starting PostgreSQL or schedulers:

```bash
make experiment-fixed EXPERIMENT_LABEL=trial EXPERIMENT_LIMIT=100
```

Run the same lightweight experiment with AutoThrottle:

```bash
make experiment-autothrottle EXPERIMENT_LABEL=trial EXPERIMENT_LIMIT=100
```

Both commands:

1. Write seed URLs directly into a dedicated queue under `data/experiments/<label>/<mode>`
2. Start a single crawler process
3. Exit automatically when the queue is empty
4. Print a summary for just that run

Recommended usage:

```bash
make experiment-fixed \
  EXPERIMENT_LABEL=fixed-baseline \
  EXPERIMENT_LIMIT=100 \
  EXPERIMENT_BATCH_SIZE=25 \
  CRAWLER_CONCURRENT_REQUESTS=32 \
  CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN=4 \
  CRAWLER_DOWNLOAD_DELAY=0.5

make experiment-autothrottle \
  EXPERIMENT_LABEL=auto-baseline \
  EXPERIMENT_LIMIT=100 \
  EXPERIMENT_BATCH_SIZE=25 \
  CRAWLER_CONCURRENT_REQUESTS=32 \
  CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN=4 \
  CRAWLER_AUTOTHROTTLE_START_DELAY=0.5 \
  CRAWLER_AUTOTHROTTLE_MAX_DELAY=10 \
  CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY=2.0
```

Lightweight experiment parameters:

- `EXPERIMENT_LABEL`: Run label. Output is written under `data/experiments/<label>/<mode>`. Default: `quick`
- `EXPERIMENT_URL_FILE`: Host-side text file containing one URL per line. Default: `seeds/urls.txt`
- `EXPERIMENT_LIMIT`: Optional maximum number of unique URLs to queue. If omitted, all URLs in the file are used.
- `EXPERIMENT_BATCH_SIZE`: Number of URLs per queue file. Smaller values create more queue files; larger values reduce queue-file overhead. Default: `100`
- `EXPERIMENT_CRAWLER_ID`: Which crawler queue/result slot to use. Normally keep this at `0` for single-process experiments. Default: `0`
- `CRAWLER_CONCURRENT_REQUESTS`: Global maximum concurrent requests for the Scrapy process. Higher values increase aggregate pressure but also make results noisier. No explicit README default; crawler default is `128`
- `CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN`: Per-domain concurrency cap. Use this to keep one domain from dominating the run. In AutoThrottle mode the crawler default is `8`; in fixed-QPS mode the fallback comes from `domain_qps.json`
- `CRAWLER_DOWNLOAD_DELAY`: Delay between requests in the same download slot. This is the main fixed-QPS knob. In AutoThrottle mode the default is `0.25`; in fixed-QPS mode the fallback comes from `domain_qps.json`
- `CRAWLER_AUTOTHROTTLE_START_DELAY`: Initial delay used by AutoThrottle before it adapts. Only meaningful for `make experiment-autothrottle`. Crawler default: `1.0`
- `CRAWLER_AUTOTHROTTLE_MAX_DELAY`: Upper bound for AutoThrottle delay growth. Only meaningful for `make experiment-autothrottle`. Crawler default: `15.0`
- `CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY`: Target average concurrency AutoThrottle tries to maintain per remote server. Only meaningful for `make experiment-autothrottle`. Crawler default: `2.0`
- `CRAWLER_AUTOTHROTTLE_DEBUG`: Set to `true` to print AutoThrottle adjustment logs. Only meaningful for `make experiment-autothrottle`. Crawler default: `false`
- `CRAWLER_LOG_LEVEL`: Scrapy log level for the experiment run, for example `WARNING`, `INFO`, or `DEBUG`. Default: `WARNING`

Parameter selection guidance:

- To compare fixed QPS against AutoThrottle fairly, keep `EXPERIMENT_URL_FILE`, `EXPERIMENT_LIMIT`, `EXPERIMENT_BATCH_SIZE`, `CRAWLER_CONCURRENT_REQUESTS`, and `CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN` the same across both runs.
- Tune fixed-QPS runs mainly with `CRAWLER_DOWNLOAD_DELAY`.
- Tune AutoThrottle runs mainly with `CRAWLER_AUTOTHROTTLE_START_DELAY`, `CRAWLER_AUTOTHROTTLE_MAX_DELAY`, and `CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY`.
- Use a small `EXPERIMENT_LIMIT` first, for example `50` or `100`, to confirm the target sites are stable before increasing load.

## Repo Layout

- [`seeds/urls.txt`](/Users/enoch/Desktop/WebCrawler/seeds/urls.txt): tracked seed URLs
- [`data/ipc`](/Users/enoch/Desktop/WebCrawler/data/ipc): runtime crawl queues, results, stats
- [`data/postgres`](/Users/enoch/Desktop/WebCrawler/data/postgres): runtime PostgreSQL data directory
- [`data/experiments`](/Users/enoch/Desktop/WebCrawler/data/experiments): lightweight crawler-only experiment outputs

`data/ipc`, `data/postgres`, and `data/experiments` are runtime-only and are ignored by git.
