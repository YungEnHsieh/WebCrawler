from __future__ import annotations

from sqlalchemy import Column, String

from libs.db.base import Base
from .mixins import ContentFeatureMixin


class ContentFeatureCurrent(
    Base,
    ContentFeatureMixin,
):
    """
    Logical ORM definition for content_feature_current_XXX.
    """
    __abstract__ = True

    url = Column(String, primary_key=True)
