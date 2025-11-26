package com.lineage.kafka.processor

import com.lineage.kafka.model.{DataEvent, LineageEvent, ProcessedEvent}
import com.lineage.kafka.serialization.{LineageEventSerde, ProcessedEventSerde}
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Instant
import java.util.UUID

class LineageTrackingProcessor extends AbstractProcessor[String, DataEvent] {
  
  private var lineageStore: KeyValueStore[String, LineageEvent] = _
  
  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    this.lineageStore = context.getStateStore("lineage-store").asInstanceOf[KeyValueStore[String, LineageEvent]]
  }
  
  override def process(key: String, dataEvent: DataEvent): Unit = {
    val processingTimestamp = Instant.now()
    
    // Create lineage event
    val lineageEvent = LineageEvent(
      eventId = UUID.randomUUID().toString,
      sourceTopic = context.topic(),
      targetTopic = "processed-events",
      processingTimestamp = processingTimestamp,
      originalEventId = Some(dataEvent.eventId),
      transformationType = "data-enrichment",
      metadata = Map(
        "processor" -> "LineageTrackingProcessor",
        "processing-node" -> context.applicationId(),
        "partition" -> context.partition().toString
      )
    )
    
    // Store lineage information
    lineageStore.put(dataEvent.eventId, lineageEvent)
    
    // Process the data (example transformation)
    val processedPayload = enrichPayload(dataEvent.payload)
    
    // Create processed event
    val processedEvent = ProcessedEvent(
      originalEventId = dataEvent.eventId,
      processedPayload = processedPayload,
      processingTimestamp = processingTimestamp,
      lineageInfo = lineageEvent
    )
    
    // Forward processed event to ProcessedSink
    context.forward(dataEvent.eventId, processedEvent, org.apache.kafka.streams.processor.To.child("ProcessedSink"))
    
    // Forward lineage event to LineageSink
    context.forward(dataEvent.eventId, lineageEvent, org.apache.kafka.streams.processor.To.child("LineageSink"))
  }
  
  private def enrichPayload(originalPayload: Map[String, String]): Map[String, String] = {
    originalPayload ++ Map(
      "processed_at" -> Instant.now().toString,
      "enrichment_version" -> "1.0",
      "data_quality_score" -> calculateQualityScore(originalPayload).toString
    )
  }
  
  private def calculateQualityScore(payload: Map[String, String]): Double = {
    val requiredFields = Set("id", "name", "timestamp")
    val presentFields = payload.keySet.intersect(requiredFields).size
    presentFields.toDouble / requiredFields.size
  }
  

}