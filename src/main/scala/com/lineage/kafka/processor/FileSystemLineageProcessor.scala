package com.lineage.kafka.processor

import com.lineage.kafka.model.{DataEvent, ProcessedEvent}
import com.lineage.kafka.openlineage.SimpleOpenLineageClient
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Instant
import java.util.UUID

class FileSystemLineageProcessor extends AbstractProcessor[String, DataEvent] {
  
  private var lineageStore: KeyValueStore[String, String] = _
  
  override def init(context: ProcessorContext): Unit = {
    super.init(context)
    this.lineageStore = context.getStateStore("lineage-store").asInstanceOf[KeyValueStore[String, String]]
  }
  
  override def process(key: String, dataEvent: DataEvent): Unit = {
    val processingTimestamp = Instant.now()
    val runId = UUID.randomUUID().toString
    
    // Extract file metadata from payload
    val filePath = dataEvent.payload.getOrElse("filepath", "unknown")
    val fileType = dataEvent.payload.getOrElse("file_type", "csv")
    val fileSize = dataEvent.payload.getOrElse("file_size", "0")
    val encoding = dataEvent.payload.getOrElse("encoding", "UTF-8")
    
    // Emit OpenLineage event with file-specific metadata
    SimpleOpenLineageClient.emitLineageEvent(
      runId = runId,
      jobName = "file-ingest-processor",
      inputs = List(s"file://$filePath"),
      outputs = List("processed-events")
    )
    
    // Store lineage reference
    lineageStore.put(dataEvent.eventId, runId)
    
    // Process the data with file-specific enrichment
    val processedPayload = enrichFilePayload(dataEvent.payload)
    
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
  
  private def enrichFilePayload(originalPayload: Map[String, String]): Map[String, String] = {
    originalPayload ++ Map(
      "source_type" -> "filesystem",
      "processed_at" -> Instant.now().toString,
      "enrichment_version" -> "1.0",
      "data_quality_score" -> calculateFileQualityScore(originalPayload).toString
    )
  }
  
  private def calculateFileQualityScore(payload: Map[String, String]): Double = {
    val requiredFields = Set("filepath", "file_type", "file_size")
    val presentFields = payload.keySet.intersect(requiredFields).size
    presentFields.toDouble / requiredFields.size
  }
}
