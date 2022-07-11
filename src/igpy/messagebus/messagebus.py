"""Message Bus implementation
"""
from dataclasses import dataclass
from datetime import datetime
import time
from typing import Any
from .persistence import Repository

@dataclass(frozen=True)
class TextMessage:
    """Simple text message for testing"""

    body: str
    message_id: int=None
    posted_at: datetime=None


class MessageBus:
    """Message bus"""

    def __init__(self, repository: Repository):
        self.repository = repository

    def put(self, item: Any):
        """Put item into the bus"""
        self.repository.insert(item)

    def peek(self, batch_limit=None) -> Any:
        """Get items from the bus"""
        return self.repository.select(batch_limit)

    def remove(self, item: Any):
        """Remove an item from the bus"""
        self.repository.delete(item)


class Consumer:
    """Message bus consumer for running in a thread"""
    def __init__(self, message_bus: MessageBus):
        self.message_bus = message_bus
        self._stopped = False
        self._paused = False
        self.paused_sleep_interval = 1
        self.running_sleep_interval = 1
        self.peek_batch_size = 10

    def pause(self):
        """Pause the consumer"""
        self._paused = True

    def resume(self):
        """Resume previously paused consumer"""
        self._paused = False

    def stop(self):
        """Stop the consumer and exit the run() method"""
        self._stopped = True

    def run(self):
        """Run consumer polling loop"""
        while not self._stopped:
            if self._paused:
                # Some kind of throttling could be implemented
                time.sleep(self.paused_sleep_interval)
                continue
            messages = self.message_bus.peek(self.peek_batch_size)
            for message in messages:
                self.process(message)
                self.message_bus.remove(message)
            # Some kind of throttling could be implemented
            time.sleep(self.running_sleep_interval)

    def process(self, message: Any):
        """Process message"""
