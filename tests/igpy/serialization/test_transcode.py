"""Unit tests for transcode module"""
# pylint: disable=redefined-outer-name

from datetime import datetime
from decimal import Decimal
from json import JSONDecoder, JSONEncoder
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from igpy.serialization.transcode import DatetimeAsISO, DecimalAsStr, JSONTranscoder, UUIDAsHex


class DummyTranscoding(DecimalAsStr):
    """Dummy transcoding for testing"""


@pytest.fixture
def some_transcoding():
    """transcoding fixture"""
    return DummyTranscoding()


@pytest.fixture
def some_transcoder(some_transcoding):
    """JSONTranscoder fixture"""
    transcoder = JSONTranscoder()
    transcoder.register(some_transcoding)
    return transcoder


class TestJSONTranscoderClass:
    """Unit tests for JSONTranscoder class"""

    def test_new_instance_initialized(self):
        """New JSONTranscoder instance should be initialized correctly"""
        transcoder = JSONTranscoder()
        assert isinstance(transcoder, JSONTranscoder)
        assert not transcoder.names
        assert not transcoder.types
        assert isinstance(transcoder.decoder, JSONDecoder)
        assert isinstance(transcoder.encoder, JSONEncoder)

    def test_register_registers_transcoding(self):
        """register should register transcoding"""
        transcoder = JSONTranscoder()
        transcoding = DummyTranscoding()
        transcoder.register(transcoding)
        assert transcoding.name in transcoder.names
        assert transcoder.names[transcoding.name] is transcoding
        assert transcoding.type in transcoder.types
        assert transcoder.types[transcoding.type] is transcoding

    def test_encode_calls_encoder(self, some_transcoder: JSONTranscoder):
        """encode method should call encoder.encode(obj).encode("utf8") methods"""
        encoder = Mock()
        with patch.object(some_transcoder, "encoder", encoder):
            some_transcoder.encode("1234.567")
            encoder.encode.assert_called_once_with("1234.567")
            encoder.encode.return_value.encode.assert_called_once_with("utf8")

    def test_decode_calls_decoder(self, some_transcoder: JSONTranscoder):
        """decode method should call decoder.decode(data) methods"""
        decoder = Mock()
        data = Mock()
        with patch.object(some_transcoder, "decoder", decoder):
            some_transcoder.decode(data)
            data.decode.assset_called_once_with("utf8")
            decoder.decode.assert_called_once_with(data.decode.return_value)

    def test_can_encode_decimal(self, some_transcoder: JSONTranscoder):
        """Decimal type is correctly encoded"""
        data = Decimal("123.456")
        encoded = some_transcoder.encode(data)
        assert encoded == b'{"@TYP": "decimal_str", "@PL": "123.456"}'

    def test_can_decode_decimal(self, some_transcoder: JSONTranscoder):
        """Encoded Decimal is correctly decoded"""
        data = b'{"@TYP": "decimal_str", "@PL": "78.910"}'
        decoded = some_transcoder.decode(data)
        assert Decimal("78.910") == decoded

    def test_can_encode_dictionary(self, some_transcoder: JSONTranscoder):
        """Dictionary is correctly encoded"""
        data = {
            "an_integer": 1,
            "a_string": "Hello World!",
            "a_list": [1, 2, 3],
            "a_dict": {"x": "y"},
            "a_decimal": Decimal("78.910"),
        }
        actual = some_transcoder.encode(data)
        expected = (b'{"an_integer": 1, "a_string": "Hello World!", '
                    b'"a_list": [1, 2, 3], '
                    b'"a_dict": {"x": "y"}, '
                    b'"a_decimal": {"@TYP": "decimal_str", "@PL": "78.910"}}')
        assert expected == actual

    def test_can_decode_dictionary(self, some_transcoder: JSONTranscoder):
        """Encoded dictionary is correctly decoded"""
        data = (b'{"an_integer": 1, "a_string": "Hello World!", '
                b'"a_list": [1, 2, 3], '
                b'"a_dict": {"x": "y"}, '
                b'"a_decimal": {"@TYP": "decimal_str", "@PL": "78.910"}}')
        decoded = some_transcoder.decode(data)
        expected = {
            "an_integer": 1,
            "a_string": "Hello World!",
            "a_list": [1, 2, 3],
            "a_dict": {"x": "y"},
            "a_decimal": Decimal("78.910"),
        }
        assert expected == decoded

    def test_encode_raises_error_not_registered_type(self, some_transcoder: JSONTranscoder):
        """encode raises TypeError for type that is not registered"""
        data = datetime.now()
        with pytest.raises(TypeError):
            some_transcoder.encode(data)

class TestDatetimeAsISOClass:
    """Unit tests for DatetimeAsISO class"""

    def test_encode_converts_date_time_to_string(self):
        """encode should return ISO string representation of datatime object"""
        transcoding = DatetimeAsISO()
        data = datetime.now()
        actual = transcoding.encode(data)
        assert data.isoformat() == actual

    def test_decode_converts_string_to_datatime(self):
        """decode should convert ISO date-time string to datatime object"""
        transcoding = DatetimeAsISO()
        data = datetime.now()
        actual = transcoding.decode(data.isoformat())
        assert data == actual

class TestUUIDAsHexClass:
    """Unit tests for UUIDAsHex class"""

    def test_encode_converts_uuid_to_string(self):
        """encode should return hex string representation of UUID object"""
        transcoding = UUIDAsHex()
        data = uuid4()
        actual = transcoding.encode(data)
        assert data.hex == actual

    def test_decode_converts_string_to_uuid(self):
        """decode should convert hex-encoded UUID string to UUID object"""
        transcoding = UUIDAsHex()
        data = uuid4()
        actual = transcoding.decode(data.hex)
        assert data == actual

