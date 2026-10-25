import uuid

from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import generate_secret
from app.dependencies import PaginationDep, SessionDep
from app.models import Subscription
from app.schemas import SubscriptionCreate, SubscriptionCreated, SubscriptionRead, SubscriptionUpdate

router = APIRouter(prefix="/subscriptions", tags=["subscriptions"])


async def get_subscription_or_404(session: AsyncSession, subscription_id: uuid.UUID) -> Subscription:
    subscription = await session.get(Subscription, subscription_id)
    if subscription is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Subscription not found")
    return subscription


@router.post("", status_code=status.HTTP_201_CREATED, response_model=SubscriptionCreated)
async def create_subscription(body: SubscriptionCreate, session: SessionDep) -> Subscription:
    subscription = Subscription(
        target_url=str(body.target_url),
        event_types=body.event_types,
        description=body.description,
        rate_limit_per_minute=body.rate_limit_per_minute,
        secret=generate_secret(),
    )
    session.add(subscription)
    await session.commit()
    return subscription


@router.get("", response_model=list[SubscriptionRead])
async def list_subscriptions(
    session: SessionDep,
    pagination: PaginationDep,
    is_active: bool | None = None,
    event_type: str | None = None,
) -> list[Subscription]:
    stmt = select(Subscription).order_by(Subscription.created_at.desc(), Subscription.id)
    if is_active is not None:
        stmt = stmt.where(Subscription.is_active == is_active)
    if event_type is not None:
        stmt = stmt.where(Subscription.event_types.contains([event_type]))

    result = await session.scalars(stmt.limit(pagination.limit).offset(pagination.offset))
    return list(result)


@router.get("/{subscription_id}", response_model=SubscriptionRead)
async def get_subscription(subscription_id: uuid.UUID, session: SessionDep) -> Subscription:
    return await get_subscription_or_404(session, subscription_id)


@router.patch("/{subscription_id}", response_model=SubscriptionRead)
async def update_subscription(
    subscription_id: uuid.UUID, body: SubscriptionUpdate, session: SessionDep
) -> Subscription:
    subscription = await get_subscription_or_404(session, subscription_id)

    changes = body.model_dump(exclude_unset=True)
    if "target_url" in changes:
        changes["target_url"] = str(body.target_url)
    for field, value in changes.items():
        setattr(subscription, field, value)

    await session.commit()
    return subscription


@router.delete("/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(subscription_id: uuid.UUID, session: SessionDep) -> None:
    subscription = await get_subscription_or_404(session, subscription_id)
    await session.delete(subscription)
    await session.commit()
