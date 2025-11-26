package com.lineage.kafka.processor

import com.lineage.kafka.model.{DataEvent, ProcessedEvent}
import com.lineage.kafka.openlineage.SimpleOpenLineageClient
import com.lineage.kafka.serialization.ProcessedEventSerde
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Instant
import java.util.UUID

class OpenLineageTrackingProcessor extends AbstractProcessor[String, DataEvent] {
  
  private var lineageStore: KeyValueStore[String, String] = _
  
  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    this.lineageStore = context.getStateStore("lineage-store").asInstanceOf[KeyValueStore[String, String]]
  }
  
  override def process(key: String, dataEvent: DataEvent): Unit = {
    val processingTimestamp = Instant.now()
    val runId = UUID.randomUUID().toString
    
    // Emit OpenLineage event
    SimpleOpenLineageClient.emitLineageEvent(
      runId = runId,
      jobName = "lineage-processor",
      inputs = List(context.topic()),
      outputs = List("processed-events")
    )
    
    // Store lineage reference
    lineageStore.put(dataEvent.eventId, runId)
    
    // Process the data
    val processedPayload = enrichPayload(dataEvent.payload)
    
    // Create processed event
    val processedEvent = ProcessedEvent(
      originalEventId = dataEvent.eventId,
      processedPayload = processedPayload,
      processingTimestamp = processingTimestamp,
      lineageRunId = Some(runId)
    )
    
    // Forward processed event
    context.forward(dataEvent.eventId, processedEvent, org.apache.kafka.streams.processor.To.child("ProcessedSink"))
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