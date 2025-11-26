package com.lineage.kafka.consumer

import com.lineage.kafka.model.{LineageEvent, ProcessedEvent}
import com.lineage.kafka.serialization.{LineageEventDeserializer, ProcessedEventDeserializer}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import com.typesafe.scalalogging.LazyLogging

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

object LineageEventConsumer extends LazyLogging {
  
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "lineage-consumer-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.lineage.kafka.serialization.ProcessedEventDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    
    val consumer = new KafkaConsumer[String, ProcessedEvent](props)
    
    try {
      consumer.subscribe(Collections.singletonList("processed-events"))
      logger.info("Started consuming processed events...")
      
      while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        
        for (record <- records.asScala) {
          displayProcessedEvent(record)
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error consuming events: ${e.getMessage}")
    } finally {
      consumer.close()
    }
  }
  
  private def displayProcessedEvent(record: ConsumerRecord[String, ProcessedEvent]): Unit = {
    val event = record.value()
    
    logger.info("=" * 60)
    logger.info(s"PROCESSED EVENT: ${event.originalEventId}")
    logger.info(s"Processing Time: ${event.processingTimestamp}")
    logger.info(s"Quality Score: ${event.processedPayload.getOrElse("data_quality_score", "N/A")}")
    logger.info(s"Processed Payload: ${event.processedPayload}")
    logger.info("-" * 30)
    logger.info(s"LINEAGE INFO:")
    event.lineageRunId match {
      case Some(runId) => 
        logger.info(s"  OpenLineage Run ID: $runId")
        logger.info(s"  Lineage tracked in Marquez: http://localhost:3000")
      case None => 
        logger.info(s"  No lineage information available")
    }
    logger.info("=" * 60)
  }
}