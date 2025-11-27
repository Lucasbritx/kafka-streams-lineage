package com.lineage.kafka.processor

import com.lineage.kafka.model.{DataEvent, ProcessedEvent}
import com.lineage.kafka.openlineage.SimpleOpenLineageClient
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Instant
import java.util.UUID

class DatabaseLineageProcessor extends AbstractProcessor[String, DataEvent] {
  
  private var lineageStore: KeyValueStore[String, String] = _
  
  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    this.lineageStore = context.getStateStore("lineage-store").asInstanceOf[KeyValueStore[String, String]]
  }
  
  override def process(key: String, dataEvent: DataEvent): Unit = {
    val processingTimestamp = Instant.now()
    val runId = UUID.randomUUID().toString
    
    // Extract database metadata from payload
    val schema = dataEvent.payload.getOrElse("schema", "public")
    val table = dataEvent.payload.getOrElse("table", "unknown")
    val dbType = dataEvent.payload.getOrElse("db_type", "postgresql")
    val dbHost = dataEvent.payload.getOrElse("db_host", "localhost")
    
    // Emit OpenLineage event with database-specific metadata
    SimpleOpenLineageClient.emitLineageEvent(
      runId = runId,
      jobName = "database-ingest-processor",
      inputs = List(s"$dbType://$dbHost/$schema.$table"),
      outputs = List("processed-events")
    )
    
    // Store lineage reference
    lineageStore.put(dataEvent.eventId, runId)
    
    // Process the data with database-specific enrichment
    val processedPayload = enrichDatabasePayload(dataEvent.payload)
    
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
  
  private def enrichDatabasePayload(originalPayload: Map[String, String]): Map[String, String] = {
    originalPayload ++ Map(
      "source_type" -> "database",
      "processed_at" -> Instant.now().toString,
      "enrichment_version" -> "1.0",
      "data_quality_score" -> calculateDatabaseQualityScore(originalPayload).toString
    )
  }
  
  private def calculateDatabaseQualityScore(payload: Map[String, String]): Double = {
    val requiredFields = Set("id", "schema", "table", "db_type")
    val presentFields = payload.keySet.intersect(requiredFields).size
    presentFields.toDouble / requiredFields.size
  }
}
