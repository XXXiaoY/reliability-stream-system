## 1. Overview

This project builds a reliability-focused stream processing system for connected vehicle telemetry.

The system is designed to process high-volume telemetry events under realistic failure conditions, including out-of-order delivery, delayed arrival, duplicated events, and schema changes.

The primary goal is not just to consume events, but to ensure that downstream state remains correct and observable under imperfect event streams.

## 2. Problem Statement

Connected vehicle telemetry pipelines operate under unstable network conditions.

Vehicles may temporarily disconnect, buffer events locally, and later replay them in bursts.

This causes three common challenges:

- events may arrive out of order
- events may arrive late relative to processing time
- events may be duplicated due to retries or reconnect behavior

If these issues are not handled carefully, downstream analytics and operational state can become inaccurate.

This project focuses on designing a stream processing system that explicitly handles these reliability problems.

## 3. Goals

The system should:

- ingest telemetry events from Kafka
- distinguish event time from ingest time
- process out-of-order events with event-time semantics
- detect and route late events correctly
- deduplicate repeated events using event IDs
- persist processed results to PostgreSQL
- expose reliability and data-quality metrics for debugging and validation

## 4. Non-Goals

This project does **not** aim to:

- build a full production-ready fleet platform
- implement end-to-end exactly-once semantics in phase 1
- optimize for large-scale cluster deployment
- support every possible telemetry type
- build a complex UI beyond operational dashboards

## 5. Scenario

The system simulates connected vehicle telemetry workloads.

Each vehicle periodically emits events such as:

- location updates
- battery status
- speed readings
- diagnostic codes

The event generator also injects controlled faults, including:

- bounded out-of-order delivery
- late-arriving events
- duplicated events
- mixed schema versions

This creates a realistic testbed for reliability-focused stream processing.

## 6. Architecture

### High-level components

- **Fleet Simulator**
    - generates telemetry events
    - injects disorder, delay, and duplicates
    - publishes to Kafka
- **Kafka**
    - receives raw telemetry events
    - partitions by `vehicle_id`
- **Flink Job**
    - consumes telemetry stream
    - assigns timestamps and watermarks
    - handles out-of-order and late events
    - performs deduplication
    - writes processed results to PostgreSQL
    - emits metrics
- **PostgreSQL**
    - stores processed telemetry state and aggregated outputs
- **Prometheus / Grafana**
    - monitors processing latency, late-event counts, duplicate drops, and throughput

## 7. Data Model

### Common Envelope

- `event_id`: UUID
- `vehicle_id`: string
- `event_time`: epoch millis
- `ingest_time`: epoch millis
- `schema_version`: string
- `event_type`: enum
- `payload`: type-specific object

### Event Types

### Location

- latitude
- longitude
- altitude
- heading

### Battery

- soc_percent
- voltage
- temperature_c
- charging

### Speed

- speed_kmh
- acceleration

### Diagnostic

- code
- severity
- description

## 8. Reliability Design

### 8.1 Out-of-Order Events

The system uses Flink event-time processing and watermarking to tolerate bounded disorder.

Initial strategy: bounded out-of-orderness watermark with a configurable delay threshold.

### 8.2 Late Events

Events that arrive after the watermark threshold are treated as late events.

The system routes them to a dedicated side output for inspection and metric tracking.

### 8.3 Duplicate Events

The system uses keyed state with TTL to track recently seen `event_id` values and suppress duplicate downstream writes.

## 9. Storage and Sink Strategy

Phase 1 uses PostgreSQL as the main sink for processed outputs.

The sink design emphasizes replay-safe writes and downstream correctness.

Initial approach:

- at-least-once processing
- idempotent or deduplicated write behavior
- correctness over maximal throughput

## 10. Observability

The system should expose at least the following metrics:

- total events received
- events processed successfully
- duplicate events dropped
- late events detected
- watermark lag
- sink write latency
- end-to-end processing latency

The fleet simulator should also record injected-fault statistics so that generator-side counts can be compared against processing-side counts.

## 11. Success Metrics / SLO Draft

Initial validation targets:

- duplicate suppression works for injected duplicate rate up to X%
- late-event detection accuracy above Y%
- bounded watermark lag under normal load
- processed results remain stable under replay and reconnect scenarios

Exact thresholds will be refined after baseline measurements.

## 12. Tech Stack

- Java 17
- Apache Flink 1.18.x
- Maven
- Kafka
- PostgreSQL
- Python fleet simulator
- Docker Compose
- Prometheus + Grafana

## 13. Phase Plan

### Phase 1: Skeleton

- bring up Kafka + Flink + PostgreSQL locally
- generate one telemetry event type
- consume and print events in Flink

### Phase 2: Event-Time Foundation

- assign timestamps
- implement watermark strategy
- validate out-of-order handling

### Phase 3: Late Event Handling

- define lateness threshold
- route late events to side output
- record late-event metrics

### Phase 4: Deduplication

- add keyed state with TTL
- suppress repeated `event_id`
- validate duplicate drop behavior

### Phase 5: Persistent Sink

- write processed results to PostgreSQL
- verify replay-safe correctness

### Phase 6: Observability

- add Prometheus metrics
- build Grafana dashboard
- compare injected vs processed fault counts

### Phase 7: Optional Extensions

- schema evolution
- DLQ
- richer chaos testing

## 14. Risks and Scope Control

Main risks:

- over-engineering too early
- spending too much time on infrastructure instead of core reliability logic
- adding advanced semantics before the baseline system is working

To control scope, the project prioritizes:

1. correctness under disorder
2. measurable validation
3. clear operational visibility

before optional features.
before optional features.