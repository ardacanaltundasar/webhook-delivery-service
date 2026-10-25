import logging
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Response, status
from fastapi.concurrency import run_in_threadpool
from pydantic import AwareDatetime
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.subscriptions import get_subscription_or_404
from app.dependencies import (
    IdempotencyKeyHeader,
    IdempotencyStoreDep,
    PaginationDep,
    SessionDep,
    SettingsDep,
)
from app.models import Delivery, DeliveryAttempt, DeliveryStatus, Event, Subscription
from app.schemas import (
    DeliveryDetail,
    DeliveryRead,
    DeliveryStats,
    EventAccepted,
    EventCreate,
    EventRead,
)
from app.worker.tasks import deliver_webhook

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["events"])
deliveries_router = APIRouter(prefix="/deliveries", tags=["deliveries"])


def _publish(delivery_ids: list[uuid.UUID]) -> None:
    for delivery_id in delivery_ids:
        deliver_webhook.delay(str(delivery_id))


async def enqueue_deliveries(delivery_ids: list[uuid.UUID]) -> None:
    if not delivery_ids:
        return
    # Rows are committed before publishing, so a broker outage must not fail the request:
    # the deliveries stay PENDING and can be re-queued via POST /deliveries/{id}/retry.
    try:
        await run_in_threadpool(_publish, delivery_ids)
    except Exception:
        logger.exception("Failed to enqueue %d deliveries", len(delivery_ids))


async def _accepted(session: AsyncSession, event: Event) -> EventAccepted:
    scheduled = await session.scalar(
        select(func.count()).select_from(Delivery).where(Delivery.event_id == event.id)
    )
    return EventAccepted(
        event_id=event.id,
        event_type=event.event_type,
        deliveries_scheduled=scheduled or 0,
        created_at=event.created_at,
    )


async def _create_event(
    session: AsyncSession, body: EventCreate, idempotency_key: str | None
) -> tuple[EventAccepted, list[uuid.UUID]]:
    event = Event(event_type=body.event_type, payload=body.payload, idempotency_key=idempotency_key)
    session.add(event)
    await session.flush()

    subscription_ids = await session.scalars(
        select(Subscription.id).where(
            Subscription.is_active,
            Subscription.event_types.contains([body.event_type]),
        )
    )
    deliveries = [Delivery(event_id=event.id, subscription_id=sid) for sid in subscription_ids]
    session.add_all(deliveries)
    await session.commit()

    accepted = EventAccepted(
        event_id=event.id,
        event_type=event.event_type,
        deliveries_scheduled=len(deliveries),
        created_at=event.created_at,
    )
    return accepted, [delivery.id for delivery in deliveries]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventAccepted,
    responses={
        status.HTTP_200_OK: {"model": EventAccepted, "description": "Replay of a completed request"},
        status.HTTP_409_CONFLICT: {"description": "Same Idempotency-Key is still being processed"},
    },
)
async def ingest_event(
    body: EventCreate,
    response: Response,
    session: SessionDep,
    store: IdempotencyStoreDep,
    idempotency_key: IdempotencyKeyHeader = None,
) -> EventAccepted:
    if idempotency_key is None:
        accepted, delivery_ids = await _create_event(session, body, None)
        await enqueue_deliveries(delivery_ids)
        return accepted

    claim = await store.claim(idempotency_key)
    if claim.in_progress:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A request with this Idempotency-Key is still being processed",
        )
    if claim.event_id is not None:
        event = await session.get(Event, claim.event_id)
        if event is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key refers to an event that no longer exists",
            )
        response.status_code = status.HTTP_200_OK
        return await _accepted(session, event)

    try:
        accepted, delivery_ids = await _create_event(session, body, idempotency_key)
    except IntegrityError:
        # Redis lost the key (TTL or eviction) but the unique constraint still holds the event.
        await session.rollback()
        event = await session.scalar(select(Event).where(Event.idempotency_key == idempotency_key))
        if event is None:
            await store.release(idempotency_key)
            raise
        await store.complete(idempotency_key, event.id)
        response.status_code = status.HTTP_200_OK
        return await _accepted(session, event)
    except BaseException:
        await store.release(idempotency_key)
        raise

    await store.complete(idempotency_key, accepted.event_id)
    await enqueue_deliveries(delivery_ids)
    return accepted


@router.get("/{event_id}", response_model=EventRead)
async def get_event(event_id: uuid.UUID, session: SessionDep) -> Event:
    event = await session.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found")
    return event


@deliveries_router.get("", response_model=list[DeliveryDetail])
async def list_deliveries(
    session: SessionDep,
    pagination: PaginationDep,
    event_id: uuid.UUID | None = None,
    subscription_id: uuid.UUID | None = None,
    status_filter: Annotated[DeliveryStatus | None, Query(alias="status")] = None,
) -> list[Delivery]:
    stmt = (
        select(Delivery)
        .options(selectinload(Delivery.attempts))
        .order_by(Delivery.created_at.desc(), Delivery.id)
    )
    if event_id is not None:
        stmt = stmt.where(Delivery.event_id == event_id)
    if subscription_id is not None:
        stmt = stmt.where(Delivery.subscription_id == subscription_id)
    if status_filter is not None:
        stmt = stmt.where(Delivery.status == status_filter)

    result = await session.scalars(stmt.limit(pagination.limit).offset(pagination.offset))
    return list(result)


@deliveries_router.get("/stats", response_model=DeliveryStats)
async def delivery_stats(
    session: SessionDep,
    subscription_id: uuid.UUID | None = None,
    since: AwareDatetime | None = None,
) -> DeliveryStats:
    filters = []
    if subscription_id is not None:
        await get_subscription_or_404(session, subscription_id)
        filters.append(Delivery.subscription_id == subscription_id)
    if since is not None:
        filters.append(Delivery.created_at >= since)

    rows = await session.execute(
        select(Delivery.status, func.count()).where(*filters).group_by(Delivery.status)
    )
    counts: dict[DeliveryStatus, int] = {s: 0 for s in DeliveryStatus} | dict(rows.tuples().all())

    # Latency only covers attempts that got an HTTP response; timeouts would otherwise
    # pin the average to the client timeout and hide how fast receivers actually are.
    avg_latency, p95_latency = (
        await session.execute(
            select(
                func.avg(DeliveryAttempt.latency_ms),
                func.percentile_cont(0.95).within_group(DeliveryAttempt.latency_ms),
            )
            .join(Delivery, Delivery.id == DeliveryAttempt.delivery_id)
            .where(DeliveryAttempt.status_code.is_not(None), *filters)
        )
    ).one()

    delivered = counts[DeliveryStatus.DELIVERED]
    failed = counts[DeliveryStatus.FAILED]
    settled = delivered + failed
    return DeliveryStats(
        subscription_id=subscription_id,
        since=since,
        total=sum(counts.values()),
        pending=counts[DeliveryStatus.PENDING],
        retrying=counts[DeliveryStatus.RETRYING],
        delivered=delivered,
        failed=failed,
        success_rate=round(delivered / settled, 4) if settled else None,
        avg_latency_ms=round(float(avg_latency), 2) if avg_latency is not None else None,
        p95_latency_ms=round(float(p95_latency), 2) if p95_latency is not None else None,
    )


@deliveries_router.get("/{delivery_id}", response_model=DeliveryDetail)
async def get_delivery(delivery_id: uuid.UUID, session: SessionDep) -> Delivery:
    delivery = await session.get(Delivery, delivery_id, options=[selectinload(Delivery.attempts)])
    if delivery is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Delivery not found")
    return delivery


def _eligible_for_manual_retry(delivery: Delivery, stuck_after: timedelta) -> bool:
    if delivery.status == DeliveryStatus.FAILED:
        return True
    if delivery.status == DeliveryStatus.DELIVERED:
        return False
    # PENDING/RETRYING only count as stuck once well past their scheduled run; earlier than
    # that, the queued task is still expected to pick them up and a retry would race it.
    due_at = delivery.next_retry_at or delivery.updated_at
    return due_at < datetime.now(UTC) - stuck_after


@deliveries_router.post(
    "/{delivery_id}/retry", status_code=status.HTTP_202_ACCEPTED, response_model=DeliveryRead
)
async def retry_delivery(
    delivery_id: uuid.UUID, session: SessionDep, settings: SettingsDep
) -> Delivery:
    delivery = await session.get(Delivery, delivery_id, with_for_update=True)
    if delivery is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Delivery not found")

    stuck_after = timedelta(seconds=settings.stuck_delivery_threshold_seconds)
    if not _eligible_for_manual_retry(delivery, stuck_after):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Delivery is {delivery.status} and not eligible for a manual retry",
        )

    delivery.status = DeliveryStatus.RETRYING
    delivery.next_retry_at = datetime.now(UTC)
    await session.commit()

    await enqueue_deliveries([delivery.id])
    return delivery
