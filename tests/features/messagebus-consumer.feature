Feature: Message bus consumer

Scenario: Paused consumer is not processing messages
   Given initialized SQLite message bus repository
     And message bus
     And message bus consumer
     And message bus consumer is started
    When message 'Hello world' is placed on the message bus
     And message bus consumer is paused after 0.01 seconds
     And message 'Hello moon' is placed on the message bus
     And message bus consumer is stopped after .05 seconds
    Then 1 messages are processed by message bus consumer
     And message #1 processed by message bus consumer is 'Hello world'

Scenario: Resumed consumer processes resumed messages
   Given initialized SQLite message bus repository
     And message bus
     And message bus consumer
     And message bus consumer is started
    When message 'Hello world' is placed on the message bus
     And message bus consumer is paused after 0.01 seconds
     And message 'Hello moon' is placed on the message bus
     And message bus consumer is resumed after .01 seconds
     And message bus consumer is stopped after .05 seconds
    Then 2 messages are processed by message bus consumer
     And message #1 processed by message bus consumer is 'Hello world'
     And message #2 processed by message bus consumer is 'Hello moon'
