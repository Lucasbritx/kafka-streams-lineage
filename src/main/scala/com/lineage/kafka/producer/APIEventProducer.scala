package com.lineage.kafka.producer

import com.lineage.kafka.model.DataEvent
import com.lineage.kafka.serialization.DataEventSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Random

object APIEventProducer {
  
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    
    val producer = new KafkaProducer[String, Array[Byte]](props)
    val serde = new DataEventSerde()
    
    println("Producing API events to 'api-events' topic...")
    
    // Simulate REST API ingestion events
    val endpoints = List("api.example.com/users", "api.example.com/orders", "api.example.com/products")
    val methods = List("GET", "POST")
    val statusCodes = List("200", "201", "304")
    
    (1 to 10).foreach { i =>
      val endpoint = endpoints(Random.nextInt(endpoints.length))
      val method = methods(Random.nextInt(methods.length))
      val statusCode = statusCodes(Random.nextInt(statusCodes.length))
      
      val event = DataEvent(
        eventId = s"api-event-${UUID.randomUUID()}",
        payload = Map(
          "id" -> s"api-call-$i",
          "api_endpoint" -> endpoint,
          "http_method" -> method,
          "status_code" -> statusCode,
          "content_type" -> "application/json",
          "response_time_ms" -> Random.nextInt(500).toString,
          "record_count" -> Random.nextInt(100).toString,
          "request_timestamp" -> Instant.now().toString
        ),
        timestamp = Instant.now()
      )
      
      val record = new ProducerRecord[String, Array[Byte]](
        "api-events",
        event.eventId,
        serde.serializer().serialize("api-events", event)
      )
      
      producer.send(record)
      println(s"Sent API event $i: $method $endpoint -> $statusCode")
      Thread.sleep(500)
    }
    
    producer.close()
    println("API event producer finished!")
  }
}
