"""Entities for simulator"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


# FIXME Use better name for the entity used in simulator
class Message(BaseModel):
    """Message"""

    notify_time: datetime
    event_type: str
    event_data: str


class SimEvent(BaseModel):
    """Event from simulator"""

    event_data: dict[str, Any]


class PubSubResource(BaseModel):
    """Abstract class for PubSub resources"""

    project_id: str
    event_type: str


class Topic(PubSubResource):
    """Topic Resource"""

    def __str__(self):
        """Topic resource path as string representation"""
        return f"projects/{self.project_id}/topics/{self.event_type}"


class Subscription(PubSubResource):
    """Subscription Resource"""

    def __str__(self):
        """Subscription resource path as string representation"""
        return f"projects/{self.project_id}/subscriptions/{self.event_type}"
