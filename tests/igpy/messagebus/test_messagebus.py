"""Unit tests for the `messagebus` module"""
# pylint: disable=redefined-outer-name,missing-function-docstring,missing-class-docstring,invalid-name
from collections import defaultdict
from unittest.mock import Mock
import pytest
from igpy.messagebus import MessageBus, MappingMessageBus


class DummyMessage:
    """Dummy Message for testing"""


@pytest.fixture
def mock_handler1():
    return Mock()


@pytest.fixture
def mock_handler2():
    return Mock()


@pytest.fixture
def some_message():
    return DummyMessage()


@pytest.fixture
def message_bus_with_two_handlers(mock_handler1, mock_handler2):
    handler_map = {DummyMessage: [mock_handler1, mock_handler2]}
    return MappingMessageBus(handler_map)


class TestMappedMessageBus:
    def test_MappedMessageBus_creates_instance(self):
        mb = MappingMessageBus()
        assert isinstance(mb, MessageBus)
        assert isinstance(mb, MappingMessageBus)

    def test_defaultdict_is_default_handler_map(self):
        mb = MappingMessageBus()
        assert isinstance(mb.handler_map, defaultdict)

    def test_handler_map_is_set_to_passed_map(self):
        handler_map = Mock()
        mb = MappingMessageBus(handler_map)
        assert mb.handler_map is handler_map

    def test_handle_calls_all_event_handlers_for_the_event_type(
        self, mock_handler1, mock_handler2, message_bus_with_two_handlers, some_message
    ):
        message_bus_with_two_handlers.handle(some_message)
        mock_handler1.assert_called_once_with(some_message)
        mock_handler2.assert_called_once_with(some_message)
