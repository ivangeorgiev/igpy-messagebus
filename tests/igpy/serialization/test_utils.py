"""Unit tests for igpy.serialization.utils module"""
# pylint: disable=redefined-outer-name,import-self

from unittest.mock import patch, Mock
import pytest
import igpy.serialization.utils
import tests.igpy.serialization.test_utils
from igpy.serialization.utils import get_topic, resolve_attr, resolve_topic


@pytest.fixture
def mock_resolve_attr():
    """patch resolve_attr function and return spy mock object"""
    with patch.object(igpy.serialization.utils, "resolve_attr") as mock:
        yield mock


class TestGetTopicFunction:
    """Test cases for get_topic function"""

    def test_get_topic_returns_correct_topic_for_class_object(self):
        """get_topic should return correct topic for object type"""
        topic = get_topic(type(self))
        assert topic == "tests.igpy.serialization.test_utils#TestGetTopicFunction"

    def test_get_topic_returns_correct_topic_for_function(self):
        """get_topic should return correct topic for a function"""
        topic = get_topic(get_topic)
        assert topic == "igpy.serialization.utils#get_topic"


class TestResolveTopic:
    """Unit tests for resolve_topic function"""

    def test_resolve_topic_returns_class_for_class_topic(self):
        """resolve_topic should return class for class topic"""
        topic = "tests.igpy.serialization.test_utils#TestGetTopicFunction"
        cls = resolve_topic(topic)
        assert cls is TestGetTopicFunction

    def test_resolve_topic_returns_result_from_resolve_attr(
        self, mock_resolve_attr: Mock
    ):
        """resolve_topic should return the result from resolve_attr call"""
        topic = "tests.igpy.serialization.test_utils#TestResolveTopic.__name__"
        obj = resolve_topic(topic)
        mock_resolve_attr.assert_called_once_with(
            tests.igpy.serialization.test_utils, "TestResolveTopic.__name__"
        )
        assert obj is mock_resolve_attr.return_value

class TestResolveAttrFunction:
    """Unit tests for resolve_attr function"""

    def test_returns_direct_attribute(self):
        """resolve_attr should return attribute value for a direct path"""
        obj = Mock()
        attr_value = resolve_attr(obj, "a")
        assert attr_value is obj.a

    def test_returns_nested_attribute(self):
        """resolve_attr should return attribute value for deep path"""
        obj = Mock()
        attr_value = resolve_attr(obj, "a.b.c.d")
        assert attr_value is obj.a.b.c.d
