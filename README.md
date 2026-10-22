# Fault-Tolerant Distributed Stream Processing with Consistent Snapshots and Adaptive Checkpointing

## Overview

This project explores fault tolerance in distributed stream-processing systems using coordinated checkpoints, consistent snapshots, failure recovery, and adaptive processing techniques.

The implementation uses Java 21, TCP socket communication, and Maven. It combines a working distributed processing pipeline with controlled experiments and simulations to evaluate different fault-tolerance mechanisms.

### Key Features

- **Distributed stream processing:** A TCP-based pipeline consisting of a Source and three Workers.
- **Coordinated checkpointing:** A central coordinator initiates checkpoint barriers and tracks worker acknowledgments.
- **Fault recovery:** Controlled worker failures, durable checkpoint restoration, and deterministic event replay.
- **Adaptive checkpointing:** Comparison of fixed and adaptive checkpoint intervals using a simulation.
- **Straggler analysis:** Measurement of processing delays, throughput, and end-to-end latency.
- **Consistent snapshots:** Experiments comparing aligned and unaligned checkpointing in multi-input processing.
- **Recovery algorithms:** Educational experiments exploring cascading rollback and communication-induced checkpointing.
- **Checkpoint durability:** Atomic checkpoint replacement and recovery from an interrupted checkpoint write.
- **Performance benchmarking:** Repeated experimental trials measuring correctness and performance.

## Technology Stack

- Java 21
- Maven
- TCP sockets
- JUnit 5
- Local filesystem checkpoint storage

## Project Status

Stages 1–13 have been implemented and experimentally evaluated. Stage 14 focuses on automated testing, documentation, reproducibility, and final presentation.


## System Architecture

The system uses a distributed processing pipeline with a centralized checkpoint coordinator.

```text
                  Checkpoint Coordinator
                    (TCP Port 6060)
                    /   |   |   \
                   /    |   |    \
                Source  W1  W2   W3
                   |
                   | DATA / BARRIER
                   v
              +----------+
              | Worker-1 |
              | Port 5050|
              +----------+
                   |
                   v
              +----------+
              | Worker-2 |
              +----------+
                   |
                   v
              +----------+
              | Worker-3 |
              +----------+
```

### Main Components

**Source:** Generates events with monotonically increasing IDs and sends them through the processing pipeline. It injects checkpoint barriers when instructed by the coordinator.

**Workers:** Process events, maintain local frequency counts, suppress duplicate or older event IDs, and persist their state when checkpoint barriers arrive.

**Checkpoint Coordinator:** Initiates checkpoints, collects acknowledgments from the three workers, and declares a global checkpoint complete after receiving all required acknowledgments.

### Coordinated Checkpoint Protocol

1. The coordinator sends a checkpoint request to the Source.
2. The Source inserts a barrier into the ordered event stream.
3. Each Worker saves its local state when the barrier arrives.
4. Each Worker forwards the barrier downstream and acknowledges the checkpoint.
5. The coordinator marks the checkpoint complete after receiving acknowledgments from all three Workers.

Worker checkpoints are stored on the local filesystem using temporary files and atomic replacement.

## Getting Started

### Prerequisites

- Java Development Kit (JDK) 21
- Apache Maven
- Git
- macOS, Linux, or another environment supporting Java TCP sockets

### Clone the Repository

```bash
git clone https://github.com/sadhvikoli/distributed-stream-processing.git
cd distributed-stream-processing/stream-processor
```

### Build the Project

Compile the project and execute its automated tests:

```bash
mvn clean test
```

Package the application:

```bash
mvn package
```

### Automated Tests

The project uses JUnit 5 to validate core worker behavior.

The current test suite verifies:

1. Duplicate events do not modify the worker's state.
2. Older event IDs are ignored.
3. Checkpoint files preserve event counts and the last processed event ID.

These tests complement the distributed recovery and performance experiments, which are executed separately.

### Experimental Validation

The project includes experiments covering worker failures, checkpoint recovery, straggler behavior, multi-input snapshots, and checkpoint-write failures.

Stage 13 provides a repeatable benchmark runner. In the previously recorded benchmark execution, all 25 experimental trials passed their correctness checks.

Benchmark results are generated locally and are excluded from Git version control.


## Running the Distributed Pipeline

The application uses a TCP-based pipeline with three Workers and a centralized Checkpoint Coordinator.

### Start the Components

First, build the project:

```bash
cd stream-processor
mvn clean package
```

Open five separate terminals in the `stream-processor` directory. Start the components in the following order.

**Terminal 1 — Checkpoint Coordinator**

```bash
java -cp target/classes edu.project.stream.CheckpointCoordinator
```

**Terminal 2 — Worker-3**

```bash
java -cp target/classes edu.project.stream.Worker Worker-3 5052 none
```

**Terminal 3 — Worker-2**

```bash
java -cp target/classes edu.project.stream.Worker Worker-2 5051 5052
```

**Terminal 4 — Worker-1**

```bash
java -cp target/classes edu.project.stream.Worker Worker-1 5050 5051
```

**Terminal 5 — Source**

```bash
java -cp target/classes edu.project.stream.Source
```

Start each component before moving to the next. The downstream Workers must be listening before upstream Workers attempt to connect.

### Initiate a Global Checkpoint

Once all participants are registered, return to the Coordinator terminal and press **Enter**.

The Coordinator sends a checkpoint request to the Source, which injects a barrier into the ordered event stream.

Each Worker saves its local state, forwards the barrier downstream, and acknowledges the checkpoint.

A successful checkpoint produces output similar to:

```text
Starting checkpoint 1
Worker-1 acknowledged checkpoint 1
Worker-2 acknowledged checkpoint 1
Worker-3 acknowledged checkpoint 1
GLOBAL CHECKPOINT 1 COMPLETED
```

The checkpoint files are written to the local `checkpoints/` directory.

### Stop the System

Press `Ctrl+C` in each terminal to stop the processes.

### Important Notes

- The pipeline uses localhost TCP connections.
- The Coordinator listens on port `6060`.
- The three Workers listen on ports `5050`, `5051`, and `5052`.
- The Source generates 12 sample events and remains connected for additional checkpoint requests.
- Checkpoint files are generated runtime artifacts and are excluded from Git version control.

## Reproducing the Benchmarks

The Stage 13 benchmark runner evaluates fault tolerance and performance across five experimental implementations.

### Run the Benchmarks

From the `stream-processor` directory, compile the project:

```bash
mvn clean package
```

Run the default benchmark with five trials per experiment:

```bash
java -cp target/classes edu.project.stream.Stage13Benchmark
```

For a shorter validation run with one trial per experiment:

```bash
java -cp target/classes edu.project.stream.Stage13Benchmark 1
```

The benchmark supports between 1 and 30 trials per experiment.

### Experiments Included

| Stage | Experiment | Evaluation Focus |
|---|---|---|
| 6 | StragglerExperiment | Throughput and checkpoint latency under worker slowdowns |
| 7 | StragglerDetectionExperiment | Event latency, throughput, and adaptive flow control |
| 8 | BarrierAlignmentExperiment | Aligned and unaligned checkpoint barrier behavior |
| 9 | DurableMultiInputRecoveryExperiment | Durable recovery with aligned and unaligned snapshots |
| 12 | CheckpointFailureExperiment | Recovery following an interrupted checkpoint write |

### Generated Results

Benchmark artifacts are written to:

`stream-processor/stage13-results/`

These include raw execution metrics, summary statistics, individual experiment logs, five SVG performance charts, and a README describing measurement limitations.

### Previously Observed Results

A five-trial benchmark execution completed all 25 experiment runs successfully.

Notable observations include:

- Adaptive flow control reduced average event latency compared with monitor-only operation, but also reduced throughput.
- Severe worker slowdowns significantly reduced pipeline throughput.
- Both aligned and unaligned durable recovery experiments passed their correctness checks.
- Recovery following an interrupted checkpoint write passed all tested trials.

### Measurement Limitations

Performance results are specific to the experimental setup and machine conditions.

Stage 6 and Stage 7 use wall-clock measurements. Stage 8 measures the time between barrier arrivals, not the time required to initiate an unaligned snapshot.

The durability experiments do not establish power-loss or storage `fsync` guarantees. The results should not be interpreted as production-scale performance measurements.