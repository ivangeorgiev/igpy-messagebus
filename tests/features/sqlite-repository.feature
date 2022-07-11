Feature: SQLite message bus repository


Scenario: Initialize repository
   Given empty SQLite message bus repository
   When message bus repository is initialized 
   Then message bus queue table 'queue_item' exists
    And message bus queue table 'queue_item_lock' exists

Scenario: Peek single message from the bus
   Given initialized SQLite message bus repository
     And message bus
     And message 'Hello world' is placed on the message bus
     And message 'Hello moon' is placed on the message bus
   When message is peeked from message bus
   Then result contains 1 items(s)
    And message body for result #1 is 'Hello world'
    And table 'queue_item' contains 2 rows
    And table 'queue_item_lock' contains 1 rows

Scenario: Peek multiple messages from the bus
   Given initialized SQLite message bus repository
     And message bus
     And message 'Hello world' is placed on the message bus
     And message 'Hello moon' is placed on the message bus
   When 10 message(s) are peeked from message bus
   Then result contains 2 items(s)
    And message body for result #1 is 'Hello world'
    And message body for result #2 is 'Hello moon'
    And table 'queue_item' contains 2 rows
    And table 'queue_item_lock' contains 2 rows

Scenario: Delete message from the bus
   Given initialized SQLite message bus repository
     And message bus
     And message 'Hello world' is placed on the message bus
     And message 'Hello moon' is placed on the message bus
   When 1 message(s) are peeked from message bus
    And 1 peeked message(s) are deleted from message bus
   Then table 'queue_item' contains 1 rows
    And table 'queue_item_lock' contains 0 rows
