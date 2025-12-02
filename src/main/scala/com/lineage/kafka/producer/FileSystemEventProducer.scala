package com.lineage.kafka.producer

import com.lineage.kafka.model.DataEvent
import com.lineage.kafka.serialization.DataEventSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Random

object FileSystemEventProducer {
  
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    
    val producer = new KafkaProducer[String, Array[Byte]](props)
    val serde = new DataEventSerde()
    
    println("Producing file system events to 'file-events' topic...")
    
    // Simulate file ingestion events
    val fileTypes = List("csv", "json", "parquet", "avro")
    val directories = List("/data/raw", "/data/staging", "/data/archive")
    
    (1 to 10).foreach { i =>
      val fileType = fileTypes(Random.nextInt(fileTypes.length))
      val directory = directories(Random.nextInt(directories.length))
      val fileName = s"data_file_$i.$fileType"
      val fileSize = (Random.nextInt(900) + 100).toString // 100-1000 KB
      
      val event = DataEvent(
        eventId = s"file-event-${UUID.randomUUID()}",
        payload = Map(
          "id" -> s"file-$i",
          "filepath" -> s"$directory/$fileName",
          "file_type" -> fileType,
          "file_size" -> s"${fileSize}KB",
          "encoding" -> "UTF-8",
          "row_count" -> Random.nextInt(10000).toString,
          "last_modified" -> Instant.now().toString
        ),
        timestamp = Instant.now()
      )
      
      val record = new ProducerRecord[String, Array[Byte]](
        "file-events",
        event.eventId,
        serde.serializer().serialize("file-events", event)
      )
      
      producer.send(record)
      println(s"Sent File event $i: $directory/$fileName ($fileSize KB)")
      Thread.sleep(500)
    }
    
    producer.close()
    println("File system event producer finished!")
  }
}
