# Changelog

All notable changes to this library will be documented in this file.

## [Unreleased] - 2026-02-28

### Added

- Added `QueueOptions.PrefetchCount` to allow explicit RabbitMQ backpressure via `BasicQos`.
- Added validation test coverage for unsupported CloudEvents spec versions.
- Added topology coverage for `DlqOptions.Enabled = false`.
- Added publisher-channel creation with broker confirmations enabled.
- Added `RmqOptions.PublishConfirmTimeout` with a safe default timeout for broker publish confirmation.

### Changed

- Consumer handler execution now runs inside a fresh DI scope per message instead of resolving the handler once from the root container during hosted service startup.
- Publisher retry semantics now interpret `RetryOptions.MaxAttempts` as total attempts, aligning publisher behavior with consumer behavior and with the public documentation.
- Topic and direct consumers now apply `BasicQos` when `PrefetchCount` is configured.
- Public handler registration now includes the concrete handler type and maps `IRmqMessageHandler<T>` to that concrete registration for consistent scoped resolution.
- CloudEvents wrapping now validates `CloudEventsOptions.SpecVersion` instead of silently ignoring it.
- Queue declaration now honors `DlqOptions.Enabled` instead of always creating DLX and DLQ topology.
- Topic publish now uses `mandatory: true` and fails explicitly when the broker returns an unroutable message.
- Publish flow now waits for broker `ack`/`nack` instead of treating socket write completion as successful delivery.

### Fixed

- Fixed a lifetime defect where transient handlers were effectively promoted to long-lived instances by being captured during hosted service construction.
- Fixed a public API inconsistency where configurable options existed but did not govern runtime behavior.
- Fixed default queue option cloning to preserve explicit DLQ defaults alongside retry defaults when per-queue overrides are absent.

### Operational Impact

- Applications that depend on scoped services inside `IRmqMessageHandler<T>` now operate under a technically sound lifetime model.
- Publish retry exhaustion counts may be lower than before for the same `MaxAttempts` value because the previous implementation incorrectly treated the configured value as retry count, not total attempts.
- Consumers can now be constrained to a broker-level in-flight message limit by configuring `PrefetchCount`.
- Setting `DlqOptions.Enabled = false` now disables DLQ topology creation as the public API always implied.
- Setting `CloudEventsOptions.SpecVersion` to an unsupported value now fails fast instead of being silently ignored.
- Publish success now means broker confirmation was received, not merely that the client attempted to write the frame.
- Publish confirmation wait is now bounded even when the caller does not provide an explicit cancellation strategy.

### Migration Notes

- Review any configuration that relied on the old publisher retry bug. If previous behavior depended on `MaxAttempts = 2` producing three publish attempts, increase the value explicitly.
- If handlers previously resolved scoped dependencies from the root provider without failure, that behavior was architecturally incorrect; the new implementation is the only acceptable model for production use.
- If any deployment sets `CloudEventsOptions.SpecVersion` to a value other than `1.0`, adjust the configuration before rollout.

### Known Remaining Gaps

- Integration test execution remains dependent on local Docker/RabbitMQ availability.
