package com.lineage.kafka.producer

import com.lineage.kafka.model.DataEvent
import com.lineage.kafka.serialization.DataEventSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant
import java.util.{Properties, UUID}
import scala.jdk.CollectionConverters._

object SampleDataProducer extends LazyLogging {
  
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.lineage.kafka.serialization.DataEventSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    
    val producer = new KafkaProducer[String, DataEvent](props)
    
    try {
      logger.info("Starting to produce sample data events...")
      
      // Produce sample events
      for (i <- 1 to 10) {
        val event = createSampleEvent(i)
        val record = new ProducerRecord[String, DataEvent]("input-events", event.eventId, event)
        
        producer.send(record).get()
        logger.info(s"Produced event: ${event.eventId}")
        
        Thread.sleep(1000) // Wait 1 second between events
      }
      
      logger.info("Finished producing sample events")
    } catch {
      case e: Exception =>
        logger.error(s"Error producing events: ${e.getMessage}")
    } finally {
      producer.close()
    }
  }
  
  private def createSampleEvent(sequenceNumber: Int): DataEvent = {
    val eventId = s"event-$sequenceNumber-${UUID.randomUUID().toString.take(8)}"
    
    val payload = Map(
      "id" -> sequenceNumber.toString,
      "name" -> s"Sample Event $sequenceNumber",
      "category" -> Seq("transaction", "user-action", "system-event")(sequenceNumber % 3),
      "value" -> (sequenceNumber * 10.5).toString,
      "timestamp" -> Instant.now().toString,
      "user_id" -> s"user-${(sequenceNumber % 5) + 1}",
      "region" -> Seq("us-east", "us-west", "eu-west", "ap-southeast")(sequenceNumber % 4),
      "metadata" -> Map(
        "source" -> "sample-producer",
        "version" -> "1.0",
        "priority" -> (sequenceNumber % 3 + 1).toString
      ).toString
    )
    
    DataEvent(
      eventId = eventId,
      payload = payload,
      timestamp = Instant.now()
    )
  }
}