"""Unit tests for the `messagebus` module"""
# pylint: disable=redefined-outer-name
import dataclasses
from datetime import datetime
from unittest.mock import Mock, patch
from attr import dataclass
import pytest
from igpy.messagebus import MessageBus
from igpy.messagebus.persistence import (
    Mapper,
    Repository,
    CursorFactory,
    StoredMessage,
    TranscodingMapper,
)


class SomeRepository(Repository):
    def _insert(self, subject):
        """dummy implmentation"""

    def _select(self, batch_limit: int = None):
        """dummy select"""

    def _delete(self, item: StoredMessage):
        """dummy delete"""


@dataclass(frozen=True)
class SomeMessage:
    """Simple message for testing"""

    message_id: int
    posted_at: datetime
    text: str


@pytest.fixture
def fetched_column_names():
    """List of column names returned by the test query"""
    return "message_id,posted_at,topic,state".split(",")


@pytest.fixture
def mock_cursor(fetched_column_names):
    """Mock cursor"""
    cursor = Mock()
    cursor.description = [(col,) for col in fetched_column_names]
    yield cursor


@pytest.fixture
def fetched_row():
    """Row as returned from cursor.fetchall"""
    return ["abcd123", datetime.now(), "the_topic", "yet-to-be-seen"]


@pytest.fixture
def fetched_dict(fetched_column_names, fetched_row):
    """Query result as dictionary"""
    return dict(zip(fetched_column_names, fetched_row))


@pytest.fixture
def repository_insert_mock():
    """Mock method for repository._insert"""
    mock = Mock()
    return mock


@pytest.fixture
def repository_delete_mock():
    """Mock method for repository._delete"""
    mock = Mock()
    return mock


@pytest.fixture
def repository_select_mock():
    """Mock method for repository._select"""
    mock = Mock()
    mock.return_value = ["world"]
    return mock


@pytest.fixture
def mock_repository(
    repository_insert_mock, repository_delete_mock, repository_select_mock
):
    """Mock repository"""
    mapper = Mock()
    repo = SomeRepository(mapper=mapper)
    with patch.object(repo, "_insert", repository_insert_mock):
        with patch.object(repo, "_delete", repository_delete_mock):
            with patch.object(repo, "_select", repository_select_mock):
                yield repo


@pytest.fixture
def given_message():
    """SomeMessage fixture"""
    return SomeMessage(1, datetime.utcnow(), "Hello world")


@pytest.fixture
def given_stored_message(given_message):
    """StoredMessage fixture"""
    state = b'{"text": "Hello world"}'
    message = StoredMessage(
        given_message.message_id,
        given_message.posted_at,
        topic="tests.igpy.messagebus.test_messagebus#SomeMessage",
        state=state,
    )
    return message


@pytest.fixture
def patched_topic_mock(given_message):
    """Patch resolve_topic"""
    mock = Mock()
    mock.value = given_message
    with patch("igpy.serialization.utils.resolve_topic", mock):
        yield mock


@pytest.fixture
@pytest.mark.usefixtures("patched_topic_mock")
def given_transcoder(given_stored_message):
    """transcoder fixture"""
    transcoder = Mock()
    transcoder.decode.return_value = {"text": "Hello world"}
    transcoder.encode.return_value = given_stored_message.state
    yield transcoder


@pytest.fixture
def given_mapper(given_transcoder):
    """mapper fixture"""
    mapper = TranscodingMapper(given_transcoder)
    yield mapper


class TestCursorFactoryClass:
    """Unit tests for CursorFactory class"""

    def test_as_dict_returns_dictionary_from_cursor_result(
        self, mock_cursor, fetched_row, fetched_dict
    ):
        """as_dict should return dictionary with values set"""
        print(mock_cursor.description)
        actual = CursorFactory.as_dict(mock_cursor, fetched_row)
        assert actual == fetched_dict

    def test_as_message_returns_stored_message(
        self, mock_cursor, fetched_row, fetched_dict
    ):
        """as_message should return StoredMessage with attributes set"""
        as_dict_patch = Mock(return_value=fetched_dict)
        with patch.object(CursorFactory, "as_dict", as_dict_patch):
            actual = CursorFactory.as_message(mock_cursor, fetched_row)
        assert isinstance(actual, StoredMessage)
        assert dataclasses.asdict(actual) == fetched_dict


class TestMapperClass:
    """Unit tests for Mapper class"""

    def test_encode_returns_passed_argument(self):
        """encode method should return passed argument"""
        mapper = Mapper()
        data = Mock()
        assert mapper.encode(data) is data

    def test_decode_returns_passed_argument(self):
        """decode should return passed argument"""
        mapper = Mapper()
        data = Mock()
        assert mapper.decode(data) is data


class TestTranscodingMapperClass:
    """Unit tests for TranscodingMapper class"""

    def test_initialized(self):
        """Transcoder instance should be initialized"""
        transcoder = Mock()
        mapper = TranscodingMapper(transcoder)
        assert mapper.transcoders == [
            transcoder,
        ]

    def test_encode_returns_message(
        self, given_message, given_stored_message, given_mapper, given_transcoder
    ):
        """encode should return StoredMessage for the passed object"""
        actual = given_mapper.encode(given_message)
        given_transcoder.encode.assert_called_once()
        assert actual == given_stored_message

    def test_decode_returns_reconstructed_object(
        self, given_stored_message, given_message, given_mapper, given_transcoder
    ):
        """decode should return object reconstructed from stored message"""
        actual = given_mapper.decode(given_stored_message)
        given_transcoder.decode.assert_called_once_with(given_stored_message.state)
        assert actual == given_message


class TestRepositoryClass:
    """Unit tests for Repository class"""

    def test_insert_calls_protected_insert_with_encoded_subject(
        self, given_message, mock_repository, repository_insert_mock
    ):
        """Public insert method should call protected _insert with encoded message"""
        mock_repository.insert(given_message)
        repository_insert_mock.assert_called_once_with(
            mock_repository.mapper.encode.return_value
        )

    def test_delete_calls_protected_delete_with_encoded_subject(
        self, given_message, mock_repository, repository_delete_mock
    ):
        """Public insert method should call protected _insert with encoded message"""
        mock_repository.delete(given_message)
        repository_delete_mock.assert_called_once_with(
            mock_repository.mapper.encode.return_value
        )

    def test_select_calls_protected_select_and_reruns_decoded_result(
        self, mock_repository, repository_select_mock
    ):
        """Public insert method should call protected _select and return decoded result"""
        actual = mock_repository.select(10)
        repository_select_mock.assert_called_once_with(
            batch_limit=10
        )  # call protected _select
        mock_repository.mapper.decode.assert_called_once_with(
            repository_select_mock.return_value[0]
        )  # result is decoded
        assert actual == [
            mock_repository.mapper.decode.return_value
        ]  # decoded result is returned
