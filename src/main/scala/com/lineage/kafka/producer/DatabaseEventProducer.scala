package com.lineage.kafka.producer

import com.lineage.kafka.model.DataEvent
import com.lineage.kafka.serialization.DataEventSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Random

object DatabaseEventProducer {
  
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    
    val producer = new KafkaProducer[String, Array[Byte]](props)
    val serde = new DataEventSerde()
    
    println("Producing database events to 'db-events' topic...")
    
    // Simulate database CDC events
    val tables = List("users", "orders", "products", "transactions")
    val operations = List("INSERT", "UPDATE", "DELETE")
    
    (1 to 10).foreach { i =>
      val table = tables(Random.nextInt(tables.length))
      val operation = operations(Random.nextInt(operations.length))
      
      val event = DataEvent(
        eventId = s"db-event-${UUID.randomUUID()}",
        payload = Map(
          "id" -> s"record-$i",
          "db_type" -> "postgresql",
          "db_host" -> "localhost:5432",
          "schema" -> "public",
          "table" -> table,
          "operation" -> operation,
          "data" -> s"""{"col1": "value$i", "col2": "${Random.nextInt(1000)}"}""",
          "timestamp_db" -> Instant.now().toString
        ),
        timestamp = Instant.now()
      )
      
      val record = new ProducerRecord[String, Array[Byte]](
        "db-events",
        event.eventId,
        serde.serializer().serialize("db-events", event)
      )
      
      producer.send(record)
      println(s"Sent DB event $i: table=$table, operation=$operation")
      Thread.sleep(500)
    }
    
    producer.close()
    println("Database event producer finished!")
  }
}
