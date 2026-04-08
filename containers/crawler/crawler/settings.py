import os

BOT_NAME = "crawler"
LOG_LEVEL = os.getenv("CRAWLER_LOG_LEVEL", "WARNING")

SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = True

DUPEFILTER_CLASS = "scrapy.dupefilters.BaseDupeFilter" # Disable dupefilter

def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


CONCURRENT_REQUESTS = int(os.getenv("CRAWLER_CONCURRENT_REQUESTS", "128"))
USE_AUTOTHROTTLE = _env_bool("CRAWLER_USE_AUTOTHROTTLE", True)

if USE_AUTOTHROTTLE:
    CONCURRENT_REQUESTS_PER_DOMAIN = int(
        os.getenv("CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN", "8")
    )
    DOWNLOAD_DELAY = float(os.getenv("CRAWLER_DOWNLOAD_DELAY", "0.25"))

    AUTOTHROTTLE_ENABLED = True
    AUTOTHROTTLE_START_DELAY = float(
        os.getenv("CRAWLER_AUTOTHROTTLE_START_DELAY", "1.0")
    )
    AUTOTHROTTLE_MAX_DELAY = float(
        os.getenv("CRAWLER_AUTOTHROTTLE_MAX_DELAY", "15.0")
    )
    AUTOTHROTTLE_TARGET_CONCURRENCY = float(
        os.getenv("CRAWLER_AUTOTHROTTLE_TARGET_CONCURRENCY", "2.0")
    )
    AUTOTHROTTLE_DEBUG = _env_bool("CRAWLER_AUTOTHROTTLE_DEBUG", False)
else:
    import json

    with open("domain_qps.json") as f:
        _DOMAIN_QPS = json.load(f)
    _DEFAULT_QPS = _DOMAIN_QPS.pop("_default", {})

    CONCURRENT_REQUESTS_PER_DOMAIN = int(
        os.getenv(
            "CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN",
            str(_DEFAULT_QPS.get("concurrency", 4)),
        )
    )
    DOWNLOAD_DELAY = float(
        os.getenv("CRAWLER_DOWNLOAD_DELAY", str(_DEFAULT_QPS.get("delay", 1.0)))
    )
    DOWNLOAD_SLOTS = _DOMAIN_QPS
    AUTOTHROTTLE_ENABLED = False

DNS_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 15
DOWNLOAD_MAXSIZE = 10 * 1024 * 1024
RETRY_ENABLED = True
RETRY_TIMES = 1 # initial + retry = 2

REDIRECT_ENABLED = True
COOKIES_ENABLED = False

#HTTPCACHE_ENABLED = True
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_POLICY = "scrapy.extensions.httpcache.RFC2616Policy"

# Memory & stats logging
EXTENSIONS = {
    "scrapy.extensions.logstats.LogStats": 500,
    "scrapy.extensions.memusage.MemoryUsage": 500,
    "crawler.extensions.DomainQpsLoggerExtension": 510,
}

ITEM_PIPELINES = {
    "crawler.pipelines.JsonPipeline": 500,
}

URL_QUEUE_TEMPLATE = os.getenv(
    "CRAWLER_URL_QUEUE_TEMPLATE", "/data/ipc/url_queue/crawler_{id:02d}"
)
RESULT_DIR_TEMPLATE = os.getenv(
    "CRAWLER_RESULT_DIR_TEMPLATE", "/data/ipc/crawl_result/crawler_{id:02d}"
)
INTERVAL_MINUTES = int(os.getenv("CRAWLER_INTERVAL_MINUTES", "10"))
EXIT_ON_IDLE = _env_bool("CRAWLER_EXIT_ON_IDLE", False)
METRICS_DIR_TEMPLATE = os.getenv(
    "CRAWLER_METRICS_DIR_TEMPLATE", "/data/ipc/metrics/crawler_{id:02d}"
)
DOMAIN_QPS_LOG_ENABLED = _env_bool("CRAWLER_DOMAIN_QPS_LOG_ENABLED", False)
DOMAIN_QPS_LOG_INTERVAL = float(os.getenv("CRAWLER_DOMAIN_QPS_LOG_INTERVAL", "1.0"))
DOMAIN_QPS_WINDOW_SECONDS = float(
    os.getenv("CRAWLER_DOMAIN_QPS_WINDOW_SECONDS", "5.0")
)
