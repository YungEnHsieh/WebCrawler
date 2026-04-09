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

## Small Continuous Mode

If you want the crawler to keep running, keep discovering new URLs, and keep feeding them back into the pipeline like the original full project, do not use the one-shot `experiment-*` commands.

Use the small continuous mode instead:

```bash
make bootstrap-small
```

This mode still runs the full pipeline:

1. `offerer` keeps selecting crawl candidates from PostgreSQL
2. `crawler` keeps fetching pages
3. `router` keeps converting crawl results and discovered outlinks
4. `ingestor` keeps inserting new URLs and updating URL state
5. `feature_extractor` keeps computing content features
6. `stats_aggregator` keeps aggregating counters

The difference is that the resource envelope is reduced:

- PostgreSQL tuning is smaller
- `offerer` runs as a single worker
- `crawler` runs as a single worker by default
- `router`, `ingestor`, and `feature_extractor` each run as a single worker
- shard routing is collapsed so one worker can cover the whole dataset

Useful commands:

```bash
make bootstrap-small
make up-small
make crawler-fixed-small
make crawler-autothrottle-small
```

Small continuous mode parameters:

- `SMALL_URL_FILE`: Seed file used during bootstrap. Default: `/app/seeds/urls.txt`
- `SMALL_NUM_SHARDS`: Total shard count used for schema init. Keep this at `256` unless you are changing the schema layout too. Default: `256`
- `SMALL_SHARDS_PER_INGESTOR`: Shards assigned to each ingestor in small mode. `256` means all shards go to ingestor `0`. Default: `256`
- `SMALL_INGEST_SERVICE`: Service used for init/seed/repair during bootstrap. Default: `scheduler_ingest`
- `SMALL_POSTGRES_MAX_CONNECTIONS`: Reduced PostgreSQL connection cap. Default: `50`
- `SMALL_POSTGRES_MAX_WAL_SIZE`: Reduced PostgreSQL WAL budget. Default: `1GB`
- `SMALL_POSTGRES_CHECKPOINT_TIMEOUT`: Reduced PostgreSQL checkpoint timeout. Default: `5min`
- `SMALL_SCHEDULER_CONTROL_CONFIG`: Control-plane config used by `offerer` and `accounting_rolloff`. Default: `/app/containers/scheduler_control/config/control.small.yaml`
- `SMALL_SCHEDULER_INGEST_CONFIG`: Ingest-plane config used by `router`, `ingestor`, `feature_extractor`, and `stats_aggregator`. Default: `/app/containers/scheduler_ingest/config/ingest.small.yaml`
- `SMALL_SCHEDULER_CONTROL_OFFERER_PROCS`: Number of `offerer` workers. Default: `1`
- `SMALL_SCHEDULER_INGEST_ROUTER_PROCS`: Number of `router` workers. Default: `1`
- `SMALL_SCHEDULER_INGEST_INGESTOR_PROCS`: Number of `ingestor` workers. Default: `1`
- `SMALL_SCHEDULER_INGEST_EXTRACTOR_PROCS`: Number of `feature_extractor` workers. Default: `1`
- `SMALL_CRAWLER_NUMPROCS`: Number of Scrapy crawler workers. Default: `1`
- `SMALL_CRAWLER_CONCURRENT_REQUESTS`: Global Scrapy concurrency for each crawler worker. Default: `8`
- `SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN`: Per-domain concurrency cap. Default: `2`
- `SMALL_CRAWLER_DOWNLOAD_DELAY`: Fixed-mode delay between requests. Default: `1.0`
- `SMALL_CRAWLER_AUTOTHROTTLE_START_DELAY`: AutoThrottle initial delay. Default: `1.0`
- `SMALL_CRAWLER_AUTOTHROTTLE_MAX_DELAY`: AutoThrottle max delay. Default: `15.0`
- `SMALL_CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY`: AutoThrottle target concurrency. Default: `1.0`
- `SMALL_CRAWLER_LOG_LEVEL`: Scrapy log level. Default: `WARNING`

Example:

```bash
make bootstrap-small \
  SMALL_CRAWLER_NUMPROCS=1 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS=4 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN=1

make crawler-autothrottle-small \
  SMALL_CRAWLER_NUMPROCS=1 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS=4 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN=1 \
  SMALL_CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY=1.0
```

## Small Benchmark Commands

If you want a duration-based comparison run, use the benchmark commands instead of `bootstrap-small` or `experiment-*`.

These commands:

1. Create an isolated benchmark data directory under `data/benchmarks/<label>/<mode>`
2. Start a fresh small PostgreSQL + full small pipeline
3. Seed the same initial URL set
4. Run for the configured duration
5. Stop the isolated stack
6. Write a crawl summary, metadata file, compose log, and domain-QPS metrics path

Run a 6-hour fixed-QPS benchmark:

```bash
make benchmark-small-fixed BENCHMARK_LABEL=trial BENCHMARK_HOURS=6
```

Run a 6-hour AutoThrottle benchmark:

```bash
make benchmark-small-autothrottle BENCHMARK_LABEL=trial BENCHMARK_HOURS=6
```

Run both sequentially with isolated state for each mode:

```bash
make benchmark-small-compare BENCHMARK_LABEL=trial BENCHMARK_HOURS=6
```

`benchmark-small-compare` runs `fixed` first and `autothrottle` second, so total wall-clock time is roughly double the configured duration plus bootstrap time.

Benchmark parameters:

- `BENCHMARK_LABEL`: Benchmark run label. Output root becomes `data/benchmarks/<label>/<mode>`. If omitted, a UTC timestamp label is generated.
- `BENCHMARK_HOURS`: Benchmark duration in hours. Default: `6`
- `BENCHMARK_SECONDS`: Optional exact duration in seconds. If set, it overrides `BENCHMARK_HOURS`
- `BENCHMARK_RESET`: Set to `true` to overwrite an existing benchmark output directory for the same label and mode. Default: `false`
- `BENCHMARK_DOMAIN_QPS_LOG_ENABLED`: Whether to record per-domain QPS metrics during the benchmark. Default: `true`
- `BENCHMARK_DOMAIN_QPS_LOG_INTERVAL`: QPS sampling interval in seconds. Default: `1.0`
- `BENCHMARK_DOMAIN_QPS_WINDOW_SECONDS`: Rolling QPS window size in seconds. Default: `5.0`

Benchmark outputs:

- `data/benchmarks/<label>/<mode>/summary/crawl_summary.txt`: Summary generated from crawler result files inside the benchmark time window
- `data/benchmarks/<label>/<mode>/summary/benchmark_meta.txt`: Effective benchmark parameters and timestamps
- `data/benchmarks/<label>/<mode>/summary/compose.log`: Captured compose logs for the isolated stack
- `data/benchmarks/<label>/<mode>/ipc/metrics/.../domain_qps.jsonl`: Per-domain QPS time series for graphing

Recommended example:

```bash
make benchmark-small-fixed \
  BENCHMARK_LABEL=compare-6h \
  BENCHMARK_HOURS=6 \
  SMALL_CRAWLER_NUMPROCS=1 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS=4 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN=1 \
  SMALL_CRAWLER_DOWNLOAD_DELAY=1.0

make benchmark-small-autothrottle \
  BENCHMARK_LABEL=compare-6h \
  BENCHMARK_HOURS=6 \
  SMALL_CRAWLER_NUMPROCS=1 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS=4 \
  SMALL_CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN=1 \
  SMALL_CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY=1.0
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
- `CRAWLER_DOMAIN_QPS_LOG_ENABLED`: Whether to write per-domain QPS time series during the run. Experiment default: `true`
- `CRAWLER_DOMAIN_QPS_LOG_INTERVAL`: Sampling interval in seconds for the QPS time series. Smaller values give smoother charts but produce more samples. Experiment default: `1.0`
- `CRAWLER_DOMAIN_QPS_WINDOW_SECONDS`: Rolling window size used to compute instantaneous QPS. For example, with `5.0`, each sample reports `completed_in_window / 5.0`. Experiment default: `5.0`

Parameter selection guidance:

- To compare fixed QPS against AutoThrottle fairly, keep `EXPERIMENT_URL_FILE`, `EXPERIMENT_LIMIT`, `EXPERIMENT_BATCH_SIZE`, `CRAWLER_CONCURRENT_REQUESTS`, and `CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN` the same across both runs.
- Tune fixed-QPS runs mainly with `CRAWLER_DOWNLOAD_DELAY`.
- Tune AutoThrottle runs mainly with `CRAWLER_AUTOTHROTTLE_START_DELAY`, `CRAWLER_AUTOTHROTTLE_MAX_DELAY`, and `CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY`.
- Use a small `EXPERIMENT_LIMIT` first, for example `50` or `100`, to confirm the target sites are stable before increasing load.

Per-domain QPS metrics:

- Lightweight experiments now write a time-series file at `data/experiments/<label>/<mode>/metrics/crawler_<id>/domain_qps.jsonl`
- Each line is one sampled domain snapshot in JSONL format
- `sample_at`: Sample time in UTC ISO-8601
- `crawler_id`: Crawler worker ID used by this run
- `domain`: Registrable domain
- `window_seconds`: Rolling window size used for QPS calculation
- `interval_seconds`: Sampling interval used when writing the time series
- `completed_in_window`: Number of requests that finished inside the rolling window
- `qps`: Computed as `completed_in_window / window_seconds`
- `inflight_requests`: Number of requests currently in downloader flight for that domain
- `avg_latency_ms`: Average downloader latency observed inside the same rolling window

Example:

```bash
make experiment-autothrottle \
  EXPERIMENT_LABEL=auto-qps \
  EXPERIMENT_LIMIT=100 \
  CRAWLER_DOMAIN_QPS_LOG_INTERVAL=1.0 \
  CRAWLER_DOMAIN_QPS_WINDOW_SECONDS=5.0
```

## Repo Layout

- [`seeds/urls.txt`](/Users/enoch/Desktop/WebCrawler/seeds/urls.txt): tracked seed URLs
- [`data/ipc`](/Users/enoch/Desktop/WebCrawler/data/ipc): runtime crawl queues, results, stats
- [`data/postgres`](/Users/enoch/Desktop/WebCrawler/data/postgres): runtime PostgreSQL data directory
- [`data/experiments`](/Users/enoch/Desktop/WebCrawler/data/experiments): lightweight crawler-only experiment outputs

`data/ipc`, `data/postgres`, and `data/experiments` are runtime-only and are ignored by git.
