"""BDD steps for message bus consumer"""
# pylint: disable=missing-function-docstring
import threading
import time
from typing import Any
from behave import given, when, then  # pylint: disable=no-name-in-module
from igpy.messagebus.messagebus import Consumer


class ConsumerSpy(Consumer):
    """Spy Consumer class"""

    def __init__(self, message_bus):
        super().__init__(message_bus)
        self.processed = []

    def process(self, message: Any):
        self.processed.append(message)


@given("message bus consumer")
def given_messagebus_consumer(ctx):
    assert (
        "messagebus" in ctx
    ), "'messagebus' should be in the context before calling this step"
    ctx.consumer = ConsumerSpy(ctx.messagebus)
    ctx.consumer.paused_sleep_interval = 0
    ctx.consumer.running_sleep_interval = 0


@given("message bus consumer is started")
def given_messagebus_consumer_started(ctx):
    assert "consumer" in ctx
    consumer_thread = threading.Thread(target=ctx.consumer.run, daemon=True)
    consumer_thread.start()
    ctx.consumer_thread = consumer_thread


@given("message bus consumer is paused after {delay} seconds")
def given_messagebus_consumer_paused(ctx, delay):
    assert "consumer" in ctx
    time.sleep(float(delay))
    ctx.consumer.pause()


@when("message bus consumer is paused after {delay} seconds")
def when_messagebus_consumer_paused(ctx, delay):
    assert "consumer" in ctx
    time.sleep(float(delay))
    ctx.consumer.pause()


@given("message bus consumer is resumed after {delay} seconds")
def given_messagebus_consumer_resumed(ctx, delay):
    assert "consumer" in ctx
    time.sleep(float(delay))
    ctx.consumer.resume()


@when("message bus consumer is resumed after {delay} seconds")
def when_messagebus_consumer_resumed(ctx, delay):
    assert "consumer" in ctx
    time.sleep(float(delay))
    ctx.consumer.resume()


@when("message bus consumer is stopped after {delay} seconds")
def when_message_bus_consumer_is_stopped(ctx, delay):
    assert "consumer" in ctx
    assert "consumer_thread" in ctx
    time.sleep(float(delay))
    ctx.consumer.stop()
    ctx.consumer_thread.join()


@then("{count} messages are processed by message bus consumer")
def assert_number_messages_processed_by_message_bus_consumer(ctx, count):
    assert "consumer" in ctx
    count = int(count)
    actual_count = len(ctx.consumer.processed)
    assert actual_count == count, f"Expected: {count}, Actual: {actual_count}"


@then("message #{index} processed by message bus consumer is '{body}'")
def assert_processed_message(ctx, index, body):
    assert "consumer" in ctx
    index = int(index)
    assert index <= len(
        ctx.consumer.processed
    ), f"requested message index exceeds number of processed messages {len(ctx.consumer.processed)}"
    actual = ctx.consumer.processed[index - 1].body
    assert actual == body, f"Expected: '{body}', Actual: '{actual}'"
