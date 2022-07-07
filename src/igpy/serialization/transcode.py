"""Objecty transcoding library"""
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
import json
from typing import Any, Dict, Union, cast
from uuid import UUID


class AbstractTranscoder(ABC):
    """Abstract object transcoder"""

    @abstractmethod
    def encode(self, obj: Any) -> bytes:
        """Encode object as bytes"""

    @abstractmethod
    def decode(self, data: bytes) -> Any:
        """Decode object from bytes"""


class Transcoding(ABC):
    """Abstract type transcoding"""

    @property
    @abstractmethod
    def type(self) -> type:
        """Get the type supported by the transcoder"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get type name supported by the transcoder"""

    @abstractmethod
    def encode(self, obj: Any) -> Union[str, dict]:
        """Encode object"""

    @abstractmethod
    def decode(self, data: Union[str, dict]) -> Any:
        """Decode object"""


class JSONTranscoder(AbstractTranscoder):
    """JSON Transcoder"""

    TYPE_KEY = "@TYP"
    PAYLOAD_KEY = "@PL"

    def __init__(self):
        self.types: Dict[type, Transcoding] = {}
        self.names: Dict[str, Transcoding] = {}
        self.encoder = json.JSONEncoder(default=self._encode_dict)
        self.decoder = json.JSONDecoder(object_hook=self._decode_dict)

    def register(self, transcoding: Transcoding):
        """Register transcoding"""
        self.types[transcoding.type] = transcoding
        self.names[transcoding.name] = transcoding

    def encode(self, obj: Any) -> bytes:
        """Encode object"""
        return self.encoder.encode(obj).encode("utf8")

    def decode(self, data: bytes) -> Any:
        """Decode object"""
        return self.decoder.decode(data.decode("utf8"))

    def _encode_dict(self, obj: Any) -> Dict[str, Union[str, dict]]:
        try:
            transcoding = self.types[type(obj)]
        except KeyError as exc:
            raise TypeError(
                f"Object of type {obj.__class__.__name__} is not serializable"
            ) from exc
        return {
            self.TYPE_KEY: transcoding.name,
            self.PAYLOAD_KEY: transcoding.encode(obj)
        }

    def _decode_dict(self, data: Dict[str, Union[str,dict]]) -> Any:
        if set(data.keys()) == {self.TYPE_KEY, self.PAYLOAD_KEY,}:
            t = data[self.TYPE_KEY]
            t = cast(str, t)
            transcoding = self.names[t]
            return transcoding.decode(data[self.PAYLOAD_KEY])
        return data

class UUIDAsHex(Transcoding):
    """
    Transcoding that represents :class:`UUID` objects as hex values.
    """

    type = UUID
    name = "uuid_hex"

    def encode(self, obj: UUID) -> str:
        return obj.hex

    def decode(self, data: str) -> UUID:
        assert isinstance(data, str)
        return UUID(data)


class DecimalAsStr(Transcoding):
    """
    Transcoding that represents :class:`Decimal` objects as strings.
    """

    type = Decimal
    name = "decimal_str"

    def encode(self, obj: Decimal) -> str:
        return str(obj)

    def decode(self, data: str) -> Decimal:
        return Decimal(data)


class DatetimeAsISO(Transcoding):
    """
    Transcoding that represents :class:`datetime` objects as ISO strings.
    """

    type = datetime
    name = "datetime_iso"

    def encode(self, obj: datetime) -> str:
        return obj.isoformat()

    def decode(self, data: str) -> datetime:
        assert isinstance(data, str)
        return datetime.fromisoformat(data)
