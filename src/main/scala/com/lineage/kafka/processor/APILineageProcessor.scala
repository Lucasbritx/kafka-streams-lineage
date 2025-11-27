package com.lineage.kafka.processor

import com.lineage.kafka.model.{DataEvent, ProcessedEvent}
import com.lineage.kafka.openlineage.SimpleOpenLineageClient
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Instant
import java.util.UUID

class APILineageProcessor extends AbstractProcessor[String, DataEvent] {
  
  private var lineageStore: KeyValueStore[String, String] = _
  
  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    this.lineageStore = context.getStateStore("lineage-store").asInstanceOf[KeyValueStore[String, String]]
  }
  
  override def process(key: String, dataEvent: DataEvent): Unit = {
    val processingTimestamp = Instant.now()
    val runId = UUID.randomUUID().toString
    
    // Extract API metadata from payload
    val apiEndpoint = dataEvent.payload.getOrElse("api_endpoint", "unknown")
    val httpMethod = dataEvent.payload.getOrElse("http_method", "GET")
    val statusCode = dataEvent.payload.getOrElse("status_code", "200")
    val contentType = dataEvent.payload.getOrElse("content_type", "application/json")
    
    // Emit OpenLineage event with API-specific metadata
    SimpleOpenLineageClient.emitLineageEvent(
      runId = runId,
      jobName = "api-ingest-processor",
      inputs = List(s"https://$apiEndpoint"),
      outputs = List("processed-events")
    )
    
    // Store lineage reference
    lineageStore.put(dataEvent.eventId, runId)
    
    // Process the data with API-specific enrichment
    val processedPayload = enrichAPIPayload(dataEvent.payload)
    
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
  
  private def enrichAPIPayload(originalPayload: Map[String, String]): Map[String, String] = {
    originalPayload ++ Map(
      "source_type" -> "api",
      "processed_at" -> Instant.now().toString,
      "enrichment_version" -> "1.0",
      "data_quality_score" -> calculateAPIQualityScore(originalPayload).toString
    )
  }
  
  private def calculateAPIQualityScore(payload: Map[String, String]): Double = {
    val requiredFields = Set("api_endpoint", "http_method", "status_code")
    val presentFields = payload.keySet.intersect(requiredFields).size
    presentFields.toDouble / requiredFields.size
  }
}
