# Kafka Streams Lineage Tracking Application

A simple Kafka Streams application in Scala that demonstrates data lineage tracking capabilities.

## Features

- **Data Processing**: Processes events from input topics
- **Lineage Tracking**: Captures and stores lineage information for each processed event
- **State Management**: Uses Kafka Streams state stores to maintain lineage data
- **JSON Serialization**: Uses Circe for JSON serialization/deserialization

## Architecture

```
input-events -> LineageTrackingProcessor -> processed-events
                                    -> lineage-events
```

The application tracks:
- Source and target topics
- Processing timestamps
- Transformation types
- Metadata about processing nodes

## Quick Start

1. Start Kafka infrastructure:
```bash
docker-compose up -d
```

2. Build the project:
```bash
sbt assembly
```

3. Run the application:
```bash
java -jar target/scala-2.13/kafka-streams-lineage-0.1.0-SNAPSHOT.jar
```

4. Run the sample producer:
```bash
sbt "runMain com.lineage.kafka.producer.SampleDataProducer"
```

## Topics

- `input-events`: Raw data events
- `processed-events`: Processed data with lineage information
- `lineage-events`: Lineage metadata events

## Data Models

### DataEvent
```scala
case class DataEvent(
  eventId: String,
  payload: Map[String, Any],
  timestamp: Instant,
  lineage: Option[LineageEvent] = None
)
```

### LineageEvent
```scala
case class LineageEvent(
  eventId: String,
  sourceTopic: String,
  targetTopic: String,
  processingTimestamp: Instant,
  originalEventId: Option[String],
  transformationType: String,
  metadata: Map[String, String]
)
```

## Configuration

The application uses the following Kafka Streams configuration:
- Application ID: `lineage-tracking-app`
- Bootstrap servers: `localhost:9092`
- Processing guarantee: `exactly_once_v2`