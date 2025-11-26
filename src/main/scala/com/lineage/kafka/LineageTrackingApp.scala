package com.lineage.kafka

import com.lineage.kafka.model.DataEvent
import com.lineage.kafka.processor.OpenLineageTrackingProcessor
import com.lineage.kafka.serialization.{DataEventSerde, ProcessedEventSerde}
import com.lineage.kafka.openlineage.SimpleOpenLineageClient
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.state.{KeyValueStore, Stores}
import org.apache.kafka.streams.processor.ProcessorSupplier
import com.typesafe.scalalogging.LazyLogging

import java.util.Properties
import scala.jdk.CollectionConverters._

object LineageTrackingApp extends LazyLogging {
  
  def main(args: Array[String]): Unit = {
    // Initialize Marquez datasets and jobs
    initializeMarquez()
    
    val topology = buildTopology()
    
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "lineage-tracking-app")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde")
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "com.lineage.kafka.serialization.DataEventSerde")
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
    
    val streams = new KafkaStreams(topology, props)
    
    // Add shutdown hook
    sys.ShutdownHookThread {
      logger.info("Shutting down Kafka Streams application...")
      streams.close()
    }
    
    logger.info("Starting Kafka Streams application...")
    streams.start()
    
    // Keep the application running
    while (true) {
      Thread.sleep(1000)
    }
  }
  
  def buildTopology(): Topology = {
    val builder = new Topology()
    
    // Add state store for lineage tracking (now stores run IDs)
    val lineageStore = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("lineage-store"),
      new org.apache.kafka.common.serialization.Serdes.StringSerde(),
      new org.apache.kafka.common.serialization.Serdes.StringSerde()
    )
    
    builder.addStateStore(lineageStore)
    
    // Add source processor
    builder.addSource("Source", "input-events")
    
    // Add OpenLineage tracking processor
    val processorSupplier = new ProcessorSupplier[String, DataEvent] {
      override def get() = new OpenLineageTrackingProcessor()
    }
    builder.addProcessor("LineageProcessor", processorSupplier, "Source")
    
    // Connect the state store to the processor
    builder.connectProcessorAndStateStores("LineageProcessor", "lineage-store")
    
    // Add sink for processed events
    builder.addSink(
      "ProcessedSink",
      "processed-events",
      new org.apache.kafka.common.serialization.Serdes.StringSerde().serializer(),
      new ProcessedEventSerde().serializer(),
      "LineageProcessor"
    )
    
    // Lineage events are now handled by OpenLineage, no need for separate sink
    
    builder
  }
  
  private def initializeMarquez(): Unit = {
    logger.info("Initializing Marquez datasets...")
    
    // Create datasets
    SimpleOpenLineageClient.createDataset("input-events", "Input Kafka topic for data events")
    SimpleOpenLineageClient.createDataset("processed-events", "Output Kafka topic for processed events")
  }
}