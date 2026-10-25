from celery import Celery

from app.config import get_settings

settings = get_settings()

celery_app = Celery("webhook_delivery", broker=settings.celery_broker_url, include=["app.worker.tasks"])

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_default_queue="webhooks",
    # Delivery state lives in PostgreSQL; nothing ever reads Celery results.
    task_ignore_result=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Each child process owns a single event loop (see tasks.WorkerContext), which is only
    # safe when one task runs at a time per process. Thread/greenlet pools would share it.
    worker_pool="prefork",
    task_soft_time_limit=settings.delivery_timeout_seconds + 20,
    task_time_limit=settings.delivery_timeout_seconds + 30,
    broker_connection_retry_on_startup=True,
    # With acks_late, Redis re-queues any message unacked past the visibility timeout.
    # Countdown retries sit unacked in the worker, so this must exceed the longest backoff.
    broker_transport_options={
        "visibility_timeout": int(settings.retry_backoff_max_seconds * 2 + 600),
    },
)
