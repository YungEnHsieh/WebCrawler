DSN ?= postgresql+psycopg2://crawler:crawler@postgres:5432/crawlerdb
URL_FILE ?= /app/seeds/urls.txt
NUM_SHARDS ?= 256
SHARDS_PER_INGESTOR ?= 16
INGEST_SERVICE ?= scheduler_ingest
RESULT_MINUTES ?= 60
RESULT_LABEL ?= run
EXPERIMENT_LABEL ?= quick
EXPERIMENT_URL_FILE ?= seeds/urls.txt
EXPERIMENT_LIMIT ?=
EXPERIMENT_BATCH_SIZE ?= 100
EXPERIMENT_CRAWLER_ID ?= 0

.PHONY: bootstrap init-db seed-urls repair-crawl-flags up-postgres up crawler-fixed crawler-autothrottle summarize-crawl experiment-fixed experiment-autothrottle

bootstrap:
	bash scripts/bootstrap.sh

up-postgres:
	docker compose up -d postgres

init-db:
	docker compose run --rm $(INGEST_SERVICE) \
		python /app/scripts/init_db.py \
		--dsn "$(DSN)" \
		--num-shards "$(NUM_SHARDS)"

seed-urls:
	docker compose run --rm $(INGEST_SERVICE) \
		python /app/scripts/seed_urls.py \
		--dsn "$(DSN)" \
		--file "$(URL_FILE)" \
		--num-shards "$(NUM_SHARDS)" \
		--shards-per-ingestor "$(SHARDS_PER_INGESTOR)"

repair-crawl-flags:
	docker compose run --rm $(INGEST_SERVICE) \
		python /app/scripts/repair_crawl_flags.py \
		--dsn "$(DSN)" \
		--num-shards "$(NUM_SHARDS)"

up:
	docker compose up -d

crawler-fixed:
	CRAWLER_USE_AUTOTHROTTLE=false docker compose up -d --force-recreate crawler

crawler-autothrottle:
	CRAWLER_USE_AUTOTHROTTLE=true docker compose up -d --force-recreate crawler

summarize-crawl:
	python3 scripts/summarize_crawl_results.py \
		--minutes "$(RESULT_MINUTES)" \
		--label "$(RESULT_LABEL)"

experiment-fixed:
	EXPERIMENT_LABEL="$(EXPERIMENT_LABEL)" \
	URL_FILE="$(EXPERIMENT_URL_FILE)" \
	EXPERIMENT_LIMIT="$(EXPERIMENT_LIMIT)" \
	EXPERIMENT_BATCH_SIZE="$(EXPERIMENT_BATCH_SIZE)" \
	EXPERIMENT_CRAWLER_ID="$(EXPERIMENT_CRAWLER_ID)" \
	CRAWLER_CONCURRENT_REQUESTS="$(CRAWLER_CONCURRENT_REQUESTS)" \
	CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN="$(CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN)" \
	CRAWLER_DOWNLOAD_DELAY="$(CRAWLER_DOWNLOAD_DELAY)" \
	CRAWLER_AUTOTHROTTLE_START_DELAY="$(CRAWLER_AUTOTHROTTLE_START_DELAY)" \
	CRAWLER_AUTOTHROTTLE_MAX_DELAY="$(CRAWLER_AUTOTHROTTLE_MAX_DELAY)" \
	CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY="$(CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY)" \
	CRAWLER_AUTOTHROTTLE_DEBUG="$(CRAWLER_AUTOTHROTTLE_DEBUG)" \
	CRAWLER_LOG_LEVEL="$(CRAWLER_LOG_LEVEL)" \
	bash scripts/run_crawler_experiment.sh fixed

experiment-autothrottle:
	EXPERIMENT_LABEL="$(EXPERIMENT_LABEL)" \
	URL_FILE="$(EXPERIMENT_URL_FILE)" \
	EXPERIMENT_LIMIT="$(EXPERIMENT_LIMIT)" \
	EXPERIMENT_BATCH_SIZE="$(EXPERIMENT_BATCH_SIZE)" \
	EXPERIMENT_CRAWLER_ID="$(EXPERIMENT_CRAWLER_ID)" \
	CRAWLER_CONCURRENT_REQUESTS="$(CRAWLER_CONCURRENT_REQUESTS)" \
	CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN="$(CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN)" \
	CRAWLER_DOWNLOAD_DELAY="$(CRAWLER_DOWNLOAD_DELAY)" \
	CRAWLER_AUTOTHROTTLE_START_DELAY="$(CRAWLER_AUTOTHROTTLE_START_DELAY)" \
	CRAWLER_AUTOTHROTTLE_MAX_DELAY="$(CRAWLER_AUTOTHROTTLE_MAX_DELAY)" \
	CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY="$(CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY)" \
	CRAWLER_AUTOTHROTTLE_DEBUG="$(CRAWLER_AUTOTHROTTLE_DEBUG)" \
	CRAWLER_LOG_LEVEL="$(CRAWLER_LOG_LEVEL)" \
	bash scripts/run_crawler_experiment.sh autothrottle
