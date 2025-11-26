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
    val lineage = event.lineageInfo
    
    logger.info("=" * 60)
    logger.info(s"PROCESSED EVENT: ${event.originalEventId}")
    logger.info(s"Processing Time: ${event.processingTimestamp}")
    logger.info(s"Quality Score: ${event.processedPayload.getOrElse("data_quality_score", "N/A")}")
    logger.info(s"Processed Payload: ${event.processedPayload}")
    logger.info("-" * 30)
    logger.info(s"LINEAGE INFO:")
    logger.info(s"  Lineage Event ID: ${lineage.eventId}")
    logger.info(s"  Source Topic: ${lineage.sourceTopic}")
    logger.info(s"  Target Topic: ${lineage.targetTopic}")
    logger.info(s"  Transformation: ${lineage.transformationType}")
    logger.info(s"  Processing Node: ${lineage.metadata.getOrElse("processing-node", "N/A")}")
    logger.info(s"  Partition: ${lineage.metadata.getOrElse("partition", "N/A")}")
    logger.info("=" * 60)
  }
}