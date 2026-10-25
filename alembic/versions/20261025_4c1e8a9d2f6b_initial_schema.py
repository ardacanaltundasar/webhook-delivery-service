"""initial schema

Revision ID: 4c1e8a9d2f6b
Revises:
Create Date: 2026-10-25 18:30:00.000000
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "4c1e8a9d2f6b"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

delivery_status = postgresql.ENUM(
    "PENDING", "RETRYING", "DELIVERED", "FAILED", name="delivery_status", create_type=False
)


def upgrade() -> None:
    delivery_status.create(op.get_bind(), checkfirst=False)

    op.create_table(
        "subscriptions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("target_url", sa.String(length=2048), nullable=False),
        sa.Column("event_types", postgresql.ARRAY(sa.String(length=128)), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("secret", sa.String(length=128), nullable=False),
        sa.Column("rate_limit_per_minute", sa.Integer(), server_default="60", nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_subscriptions")),
    )
    op.create_index(
        "ix_subscriptions_event_types", "subscriptions", ["event_types"], postgresql_using="gin"
    )
    op.create_index(op.f("ix_subscriptions_is_active"), "subscriptions", ["is_active"])

    op.create_table(
        "events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_events")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_events_idempotency_key")),
    )
    op.create_index(op.f("ix_events_event_type"), "events", ["event_type"])

    op.create_table(
        "deliveries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("subscription_id", sa.Uuid(), nullable=False),
        sa.Column("status", delivery_status, server_default="PENDING", nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_status_code", sa.Integer(), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["events.id"],
            name=op.f("fk_deliveries_event_id_events"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["subscription_id"],
            ["subscriptions.id"],
            name=op.f("fk_deliveries_subscription_id_subscriptions"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_deliveries")),
        sa.UniqueConstraint(
            "event_id", "subscription_id", name=op.f("uq_deliveries_event_id_subscription_id")
        ),
    )
    op.create_index(op.f("ix_deliveries_status"), "deliveries", ["status"])
    op.create_index(
        "ix_deliveries_subscription_id_created_at", "deliveries", ["subscription_id", "created_at"]
    )

    op.create_table(
        "delivery_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("delivery_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", delivery_status, nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("response_snippet", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["delivery_id"],
            ["deliveries.id"],
            name=op.f("fk_delivery_attempts_delivery_id_deliveries"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_delivery_attempts")),
        sa.UniqueConstraint(
            "delivery_id",
            "attempt_number",
            name=op.f("uq_delivery_attempts_delivery_id_attempt_number"),
        ),
    )


def downgrade() -> None:
    op.drop_table("delivery_attempts")
    op.drop_index("ix_deliveries_subscription_id_created_at", table_name="deliveries")
    op.drop_index(op.f("ix_deliveries_status"), table_name="deliveries")
    op.drop_table("deliveries")
    op.drop_index(op.f("ix_events_event_type"), table_name="events")
    op.drop_table("events")
    op.drop_index(op.f("ix_subscriptions_is_active"), table_name="subscriptions")
    op.drop_index("ix_subscriptions_event_types", table_name="subscriptions")
    op.drop_table("subscriptions")
    delivery_status.drop(op.get_bind(), checkfirst=False)
