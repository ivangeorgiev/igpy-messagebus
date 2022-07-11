from dataclasses import dataclass
from datetime import datetime
from typing import Any
from behave import given, when, then
from igpy.messagebus.messagebus import MessageBus, TextMessage
from igpy.messagebus.sqlite import SQLiteRepository



class StepContext:
    repository: SQLiteRepository
    messagebus: MessageBus
    actual: Any


@given("empty SQLite message bus repository")
def given_empty_sqlite_repository(ctx: StepContext):
    """Create SQLite message bus repository with emtpty storage """
    ctx.repository = SQLiteRepository(db_name=":memory:")


@given("initialized SQLite message bus repository")
def given_initialized_empty_sqlite_repository(ctx: StepContext):
    """Initialize SQLite message bus repository"""
    if "repository" not in ctx:
        given_empty_sqlite_repository(ctx)
    ctx.repository.initialize()

@given("message bus")
def given_message_bus(ctx: StepContext):
    """Create messagebus with repository"""
    assert "repository" in ctx, "'repository' should be added to the test context before calling this step"
    messagebus = MessageBus(ctx.repository)
    ctx.messagebus = messagebus

@given("message '{body}' is placed on the message bus")
def given_message_is_placed_on_messagebus(ctx: StepContext, body: str):
    """Place a message on the message bus"""
    assert "messagebus" in ctx, "'messagebus' should be added to the test context before calling this step"
    message = TextMessage(body=body)
    ctx.messagebus.put(message)

@when("message '{body}' is placed on the message bus")
def when_message_is_placed_on_messagebus(ctx: StepContext, body: str):
    """Place a message on the message bus"""
    assert "messagebus" in ctx, "'messagebus' should be added to the test context before calling this step"
    message = TextMessage(body=body)
    ctx.messagebus.put(message)



@when("message bus repository is initialized")
def when_message_bus_repository_is_initialized(ctx: StepContext):
    """Invoke message bus repository initialize action"""
    assert "repository" in ctx, "'repository' should be added to the test context before calling this step"
    ctx.repository.initialize()


@when("message is peeked from message bus")
def when_message_is_peeked(ctx: StepContext):
    """peek a message from the message bus and place it in context as `actual`"""
    assert "messagebus" in ctx, "'messagebus' should be added to the test context before calling this step"
    ctx.actual = ctx.messagebus.peek()

@when("{count} message(s) are peeked from message bus")
def when_few_messages_are_peeked(ctx: StepContext, count):
    """peek a message from the message bus and place it in context as `actual`"""
    assert "messagebus" in ctx, "'messagebus' should be added to the test context before calling this step"
    count = int(count)
    ctx.actual = ctx.messagebus.peek(count)

@when("{1} peeked message(s) are deleted from message bus")
def when_peeked_messages_are_deleted(ctx: StepContext, count):
    """delete peeked messages"""
    assert "messagebus" in ctx, "'messagebus' should be added to the test context before calling this step"
    assert "actual" in ctx, "'actual' should be added to the test context before calling this step"
    count = int(count)
    assert len(ctx.actual) >= count, f"expected 'actual' to contain at least {count} items, but {len(ctx.actual)} found"
    for index in range(count):
        ctx.messagebus.remove(ctx.actual[index])


@then("message bus queue table '{table_name}' exists")
def assert_message_bus_table_exists(ctx: StepContext, table_name: str):
    """Assert that message bus table exists"""
    assert "repository" in ctx
    assert ctx.repository.table_exists(table_name)


@then("message bus queue lock table exists")
def assert_message_bus_queue_lock_table_exists(ctx: StepContext):
    """Assert that message bus table exists"""
    assert "repository" in ctx
    assert ctx.repository.table_exists("queue_item_lock")

@then("result contains {expected_count} items(s)")
def assert_number_of_messages(ctx, expected_count):
    assert "actual" in ctx, "'actual' should be added to the test context before calling this step"
    expected_count = int(expected_count)
    assert len(ctx.actual) == expected_count, f"Expected: {expected_count}, Actual: {len(ctx.actual)}"

@then("message body for result #{index} is '{expected_body}'")
def assert_message_body(ctx: StepContext, index, expected_body):
    assert "actual" in ctx, "'actual' should be added to the test context before calling this step"
    actual_body = ctx.actual[int(index) - 1].body 
    assert  actual_body == expected_body, f"Expected: {expected_body}, Actual: {actual_body}"

@then("table '{table_name}' contains {expected_count} rows")
def assert_number_of_rows_in_a_table(ctx: StepContext, table_name: str, expected_count: int):
    assert "repository" in ctx, "'repository' should be added to the test context before calling this step"
    expected_count = int(expected_count)
    actual_count = ctx.repository.row_count(table_name)
    assert actual_count == expected_count, f"Expected: {expected_count}, Actual: {actual_count}"