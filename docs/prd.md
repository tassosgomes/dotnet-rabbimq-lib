Product Requirements Document (PRD): RabbitMQ Library with Quorum Queues, Exponential Retry, DLQ, and CloudEvents Support
1. Document Information

Document Title: RabbitMQ Library Design for Quorum Queues with Retry, DLQ, and CloudEvents
Version: 1.1
Date: February 07, 2026
Author: Grok 4 (AI Assistant)
Stakeholders: Development Team (C# and Java)
Approval Status: Draft (Pending Review)
Change Log:
Version 1.1: Added requirement for CloudEvents encapsulation of payloads, ensuring transparency to developers.


2. Introduction
2.1 Purpose
This PRD outlines the requirements for designing and implementing a cross-language library for interacting with RabbitMQ, focusing on quorum queues. The library will provide robust message handling capabilities, including connection management, exponential retry mechanisms for failed operations, and dead-letter queue (DLQ) integration. Additionally, it will use CloudEvents to encapsulate message payloads in a standardized format, but this will be handled transparently by the library without requiring developers to interact with CloudEvents directly.
The library aims to simplify RabbitMQ integration in distributed systems, ensuring high availability, fault tolerance, reliable message processing, and interoperability through event standardization.
2.2 Background
RabbitMQ is a popular open-source message broker that supports various queue types, including quorum queues for high durability and consistency in clustered environments. Quorum queues provide leader election and replication, making them suitable for mission-critical applications.
CloudEvents is a specification for describing event metadata and data in a common way, promoting interoperability across systems. By wrapping payloads in CloudEvents format, the library ensures messages are structured consistently, but this encapsulation is abstracted away from the end-user developer.
Key challenges addressed:

Reliable connections to RabbitMQ clusters.
Handling transient failures with exponential backoff retries.
Routing failed messages to a DLQ for later analysis or reprocessing.
Standardizing event payloads without adding complexity for developers.

This library will abstract these complexities, allowing developers to focus on business logic rather than boilerplate code or event formatting.
2.3 Scope

In Scope:
Connection establishment to RabbitMQ using quorum queues.
Publishing and consuming messages with exponential retry (up to 5 attempts).
Automatic DLQ configuration and message routing on failures.
Transparent use of CloudEvents for payload encapsulation (e.g., using official CloudEvents SDKs in C# and Java).
Support for basic acknowledgments (ACK/NACK).
Language-specific implementations: C# (using RabbitMQ.Client NuGet package) and Java (using com.rabbitmq.client library).

Out of Scope:
Advanced RabbitMQ features like federations, shovels, or plugins.
Authentication beyond basic username/password (e.g., OAuth, SSL client certs can be added as extensions).
Monitoring or metrics integration (e.g., Prometheus exporter).
Support for other queue types (e.g., classic or stream queues).
Full-fledged error logging framework (use language-standard logging).
Custom CloudEvents extensions beyond basic spec compliance.


2.4 Assumptions and Dependencies

RabbitMQ server version: 3.8+ (quorum queues introduced in 3.8).
Dependencies:
C#: RabbitMQ.Client (>=6.0), CloudNative.CloudEvents (>=2.0), .NET 6+.
Java: com.rabbitmq:amqp-client (>=5.0), io.cloudevents:cloudevents-java (>=2.0), Java 11+.

Network access to RabbitMQ cluster.
Developers have basic knowledge of AMQP concepts; no CloudEvents knowledge required.

3. Goals and Objectives
3.1 Business Goals

Reduce development time for RabbitMQ integrations by providing a reusable library.
Improve system reliability through built-in retries and DLQ handling.
Enhance interoperability by using CloudEvents for payloads, transparently.
Ensure cross-language consistency for teams using mixed tech stacks (e.g., microservices in C# and Java).

3.2 Technical Objectives

Connect to RabbitMQ using quorum queues for high availability.
Implement exponential retry logic: 5 attempts with backoff (e.g., delays of 1s, 2s, 4s, 8s, 16s).
Configure DLQ automatically for failed messages after max retries.
Encapsulate payloads in CloudEvents format during publish and extract during consume, without exposing this to developers.
Provide thread-safe, asynchronous APIs where possible.

3.3 Success Metrics

Library passes unit/integration tests with 100% coverage for core features, including CloudEvents handling.
Handles 99.9% of transient failures without message loss.
Adoption: Used in at least one production service within 3 months.

4. Functional Requirements
4.1 Core Features

Connection Management:
Establish connection to RabbitMQ server/cluster.
Support for connection strings (host, port, virtual host, username, password).
Automatic reconnection on failure.
Declare quorum queues with configurable parameters (e.g., quorum size, delivery limit).

Message Publishing:
Publish messages to a specified quorum queue.
Support for message properties (e.g., headers, priority).
Transparently encapsulate the developer-provided payload in a CloudEvents structure (e.g., with required attributes like id, source, type, time, and data).
Exponential retry on publish failures (network issues, queue unavailable).
After 5 retries, route to DLQ (preserving the CloudEvents wrapper).

Message Consumption:
Consume messages from a quorum queue.
Transparently extract the payload from the CloudEvents structure and pass only the original data to the developer's handler.
Support for basic ACK/NACK.
Exponential retry on processing failures (e.g., custom handler exceptions).
After 5 retries, NACK with requeue=false to send to DLQ (preserving the CloudEvents wrapper).

DLQ Integration:
Automatically declare a DLQ for each main queue (e.g., named "{queue_name}.dlq").
Configure dead-letter-exchange (DLX) and routing keys.
Messages exceeding retry limits are routed to DLQ with metadata (e.g., failure reason, original queue), maintaining CloudEvents format.

Configuration:
Builder pattern for setup (e.g., fluent API).
Options: Retry count (default 5), backoff base (default 1s), quorum settings, CloudEvents defaults (e.g., source URI, event type).
CloudEvents configuration is optional and defaults to library-managed values for transparency.


4.2 User Stories

As a developer, I want to connect to RabbitMQ and declare a quorum queue so that messages are durable.
As a developer, I want to publish a message with retries so that transient failures don't cause loss, and the payload is automatically wrapped in CloudEvents.
As a developer, I want failed messages to go to DLQ after max retries for auditing.
As a developer, I want the library in C# and Java to have similar APIs for easy porting, without needing to handle CloudEvents manually.
As a developer, I want to receive only the raw payload in my consumer handler, with CloudEvents unwrapped transparently.

5. Non-Functional Requirements
5.1 Performance

Throughput: Support at least 1,000 messages/second per connection.
Latency: <100ms for publish/consume in normal conditions (including CloudEvents overhead).
Scalability: Handle multiple connections/consumers.

5.2 Reliability

Fault Tolerance: Automatic recovery from connection drops.
Idempotency: Ensure retries don't duplicate messages (use correlation IDs, integrated with CloudEvents id).
Data Integrity: Use quorum queues for at-least-once delivery; validate CloudEvents structure.

5.3 Security

Secure connections: Support SSL/TLS (configurable).
Input Validation: Sanitize queue names, messages to prevent injection; ensure CloudEvents data is properly serialized.
Compliance: No storage of sensitive data; align with GDPR if messages contain PII.

5.4 Usability

Documentation: API docs, examples in README, including notes on transparent CloudEvents usage.
Error Handling: Meaningful exceptions with retry details; CloudEvents errors handled internally where possible.
Logging: Integrate with standard loggers (e.g., Microsoft.Extensions.Logging for C#, SLF4J for Java); log CloudEvents metadata optionally.

5.5 Maintainability

Code Quality: Follow SOLID principles, unit testable.
Versioning: Semantic versioning (e.g., 1.0.0).
Cross-Language Parity: 90% API similarity, with consistent CloudEvents abstraction.

6. Architecture and Design
6.1 High-Level Design

Components:
ConnectionFactory: Handles connections.
QueueManager: Declares queues/DLQs.
Publisher: Sends messages with retry, wrapping in CloudEvents.
Consumer: Receives and processes with retry, unwrapping CloudEvents.
CloudEventsWrapper: Internal utility for encapsulation/extraction.

Patterns:
Retry: Exponential backoff using Polly (C#) or Resilience4j (Java).
Async: Use Task/async-await (C#), CompletableFuture (Java).
Transparency: Developers pass/receive plain objects/bytes; library serializes to CloudEvents binary or structured mode (default: JSON-structured).


6.2 Language-Specific Considerations

C#:
NuGet Packages: RabbitMQ.Client, CloudNative.CloudEvents.
Retry Library: Polly for exponential backoff.
Example API: var client = new RabbitMqClient(builder => builder.WithConnection("amqp://user:pass@host").WithQueue("myQueue", quorumSize: 3)); client.Publish(myPayload); (CloudEvents handled internally).

Java:
Maven Dependencies: com.rabbitmq:amqp-client, io.cloudevents:cloudevents-java.
Retry Library: Resilience4j.
Example API: RabbitMqClient client = RabbitMqClient.builder().connection("amqp://user:pass@host").queue("myQueue", 3).build(); client.publish(myPayload); (CloudEvents handled internally).


6.3 Data Flow

App -> Library: Configure and connect.
Library -> RabbitMQ: Declare queue/DLQ.
Publish: App sends payload -> Library wraps in CloudEvents -> Retry loop -> Success or DLQ.
Consume: RabbitMQ delivers -> Library unwraps CloudEvents -> Pass payload to app handler -> ACK or DLQ.

7. Implementation Plan
7.1 Phases

Design: Finalize APIs, including CloudEvents integration (1 week).
Development: Implement C# version (2 weeks), then Java (2 weeks).
Testing: Unit/Integration (1 week), with CloudEvents validation.
Documentation and Release: README, packages (1 week).

7.2 Risks

Risk: Library incompatibilities. Mitigation: Pin versions.
Risk: Performance bottlenecks from CloudEvents serialization. Mitigation: Benchmark and optimize (e.g., use binary mode if needed).
Risk: Cross-language differences. Mitigation: Define common interface first.

8. Testing Requirements

Unit Tests: Cover all methods (e.g., retry logic, CloudEvents wrap/unwrap).
Integration Tests: Use Testcontainers for RabbitMQ instance; verify CloudEvents in messages.
Edge Cases: Connection failures, max retries, invalid configs, malformed CloudEvents.
Coverage: >90%.

9. Appendices

Glossary:
Quorum Queue: Replicated queue requiring majority acknowledgment.
DLQ: Queue for undeliverable messages.
Exponential Retry: Increasing delays between attempts.
CloudEvents: Specification for event data structure.

References:
RabbitMQ Docs: https://www.rabbitmq.com/quorum-queues.html
CloudEvents Spec: https://github.com/cloudevents/spec
Polly: https://github.com/App-vNext/Polly
Resilience4j: https://github.com/resilience4j/resilience4j


This PRD serves as a blueprint for development. Review and iterate as needed.