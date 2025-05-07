"""Entities for simulator"""

from datetime import datetime

from pydantic import BaseModel


class Message(BaseModel):
    """Message"""

    notify_time: datetime
    event_type: str
    event_data: str


class TopicResource(BaseModel):
    """Topic Resource"""

    project_id: str
    event_type: str

    def __str__(self):
        """Topic resource path as string representation"""
        return f"projects/{self.project_id}/topics/{self.event_type}"
