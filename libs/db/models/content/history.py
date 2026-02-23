from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, String
from sqlalchemy.sql import func

from libs.db.base import Base
from .mixins import ContentFeatureMixin


class ContentFeatureHistory(
    Base,
    ContentFeatureMixin,
):
    """
    Logical ORM definition for content_feature_history_XXX.

    Note:
      - PK is snapshot_id (sequence-backed in DB).
      - url is NOT NULL but not primary key.
      - snapshot_at default now().
    """
    __abstract__ = True

    snapshot_id = Column(BigInteger, primary_key=True)
    snapshot_at = Column(DateTime(timezone=True), server_default=func.now())

    url = Column(String, nullable=False)

