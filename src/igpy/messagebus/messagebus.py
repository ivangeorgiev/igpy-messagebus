"""Message Bus module
"""
# pylint: disable=too-few-public-methods
from collections import defaultdict
from typing import Any, Callable, Dict, List, Type


MessageHandler = Callable[[Any], None]
HandlerRegistry = Dict[Type, List[MessageHandler]]


class MessageBus:
    """Dummy Message Bus implementation"""

    def handle(self, message: Any) -> None:
        """Handle message"""
        for handler in self._get_handlers_for(message):
            handler(message)

    # pylint: disable=unused-argument
    def _get_handlers_for(
        self, message: Any
    ) -> List[MessageHandler]:
        """Get a list of message handlers for a message."""
        return []


class MappingMessageBus(MessageBus):
    """Message Bus with handler mapping."""

    handler_map: HandlerRegistry

    def __init__(
        self,
        handler_map: HandlerRegistry = None,
    ):
        self.handler_map = handler_map or defaultdict(set)

    def _get_handlers_for(self, message: Any) -> List[MessageHandler]:
        return self.handler_map[type(message)]
