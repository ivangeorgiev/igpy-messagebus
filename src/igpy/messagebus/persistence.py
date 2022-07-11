"""Message persistence module"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Mapping
from uuid import uuid4

from igpy.serialization.transcode import AbstractTranscoder
from igpy.serialization.utils import get_topic, resolve_topic


@dataclass(frozen=True)
class StoredMessage:
    """Repository item"""

    message_id: str
    posted_at: datetime
    topic: str
    state: bytes


class CursorFactory:
    """Factory for cursor results"""

    @classmethod
    def as_dict(cls, cursor, column_data: tuple[Any, ...]) -> Mapping:
        """Create dictionary-like object from cursor result"""
        column_names = list([col[0] for col in cursor.description])
        result = dict(zip(column_names, column_data))
        return result

    @classmethod
    def as_message(cls, cursor, column_data: tuple[Any, ...]) -> StoredMessage:
        """Create StoredMessage instance from cursor result"""
        row = cls.as_dict(cursor, column_data)
        return StoredMessage(**row)


class Mapper:
    """Map objects to/from StoredMessage"""

    def encode(self, subject: object) -> StoredMessage:
        """Encode an object"""
        return subject

    def decode(self, encoded: StoredMessage) -> object:
        """Reconstruct previously encoded object from StoredMessage"""
        return encoded


class TranscodingMapper(Mapper):
    """Map objects to/from StoredMessage using state transcoder(s)"""

    id_attr = "message_id"
    posted_at_attr = "posted_at"

    def __init__(self, transcoder: AbstractTranscoder = None):
        self.transcoders = []
        if transcoder:
            self.transcoders.append(transcoder)

    def encode(self, subject: object) -> StoredMessage:
        props = subject.__dict__.copy()
        message_id = props.pop(self.id_attr, None) or uuid4().hex
        posted_at = props.pop(self.posted_at_attr, None) or datetime.utcnow()
        topic = get_topic(type(subject))
        encoded_props = props
        for transcoder in self.transcoders:
            encoded_props = transcoder.encode(encoded_props)
        return StoredMessage(
            message_id=message_id,
            posted_at=posted_at,
            topic=topic,
            state=encoded_props,
        )

    def decode(self, encoded: StoredMessage) -> object:
        cls = resolve_topic(encoded.topic)
        message = object.__new__(cls)
        props: object = encoded.state
        for transcoder in reversed(self.transcoders):
            props = transcoder.decode(props)
        props[self.id_attr] = encoded.message_id
        props[self.posted_at_attr] = encoded.posted_at
        message.__dict__.update(props)
        return message


class Repository(ABC):
    """Repository for StoredMessage objects"""

    def __init__(self, mapper: Mapper = None):
        super().__init__()
        self.mapper = mapper or Mapper()

    def initialize(self):
        """Perform initial repository setup"""

    @abstractmethod
    def _insert(self, item: Any):
        """Insert item into the repository"""

    @abstractmethod
    def _select(self, batch_limit: int = None) -> List[Any]:
        """Select items for processing from the repository"""

    def _delete(self, item: StoredMessage):
        """Delete item from the repository"""

    def insert(self, item: Any):
        """Insert item into the repository"""
        self._insert(self.mapper.encode(item))

    def select(self, batch_limit: int = None) -> List[Any]:
        """Select items for processing from the repository"""
        return [
            self.mapper.decode(item) for item in self._select(batch_limit=batch_limit)
        ]

    def delete(self, item: Any):
        """Delete item from the repository"""
        self._delete(self.mapper.encode(item))
