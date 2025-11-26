package com.lineage.kafka.model

import java.time.Instant
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

case class LineageEvent(
  eventId: String,
  sourceTopic: String,
  targetTopic: String,
  processingTimestamp: Instant,
  originalEventId: Option[String] = None,
  transformationType: String,
  metadata: Map[String, String] = Map.empty
)

object LineageEvent {
  implicit val encoder: Encoder[LineageEvent] = deriveEncoder[LineageEvent]
  implicit val decoder: Decoder[LineageEvent] = deriveDecoder[LineageEvent]
}

case class DataEvent(
  eventId: String,
  payload: Map[String, String],
  timestamp: Instant,
  lineage: Option[LineageEvent] = None
)

object DataEvent {
  implicit val encoder: Encoder[DataEvent] = deriveEncoder[DataEvent]
  implicit val decoder: Decoder[DataEvent] = deriveDecoder[DataEvent]
}

case class ProcessedEvent(
  originalEventId: String,
  processedPayload: Map[String, String],
  processingTimestamp: Instant,
  lineageInfo: LineageEvent
)

object ProcessedEvent {
  implicit val encoder: Encoder[ProcessedEvent] = deriveEncoder[ProcessedEvent]
  implicit val decoder: Decoder[ProcessedEvent] = deriveDecoder[ProcessedEvent]
}