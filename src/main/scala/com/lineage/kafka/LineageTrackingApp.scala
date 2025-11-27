package com.lineage.kafka

import com.lineage.kafka.model.DataEvent
import com.lineage.kafka.processor.{OpenLineageTrackingProcessor, DatabaseLineageProcessor, FileSystemLineageProcessor, APILineageProcessor}
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
    
    // Add state store for lineage tracking (shared across all processors)
    val lineageStore = Stores.keyValueStoreBuilder(
      Stores.persistentKeyValueStore("lineage-store"),
      new org.apache.kafka.common.serialization.Serdes.StringSerde(),
      new org.apache.kafka.common.serialization.Serdes.StringSerde()
    )
    
    builder.addStateStore(lineageStore)
    
    // ==================== MULTIPLE SOURCES ====================
    
    // SOURCE 1: Generic input events (original)
    builder.addSource("GenericSource", "input-events")
    
    // SOURCE 2: Database events (from Debezium or JDBC Connector)
    builder.addSource("DBSource", "db-events")
    
    // SOURCE 3: File system events (from File Source Connector)
    builder.addSource("FileSource", "file-events")
    
    // SOURCE 4: API/Web Service events (from HTTP Source Connector)
    builder.addSource("APISource", "api-events")
    
    // ==================== PROCESSORS ====================
    
    // Generic processor for input-events
    val genericProcessor = new ProcessorSupplier[String, DataEvent] {
      override def get() = new OpenLineageTrackingProcessor()
    }
    builder.addProcessor("GenericProcessor", genericProcessor, "GenericSource")
    
    // Database-specific processor
    val dbProcessor = new ProcessorSupplier[String, DataEvent] {
      override def get() = new DatabaseLineageProcessor()
    }
    builder.addProcessor("DBProcessor", dbProcessor, "DBSource")
    
    // File system-specific processor
    val fileProcessor = new ProcessorSupplier[String, DataEvent] {
      override def get() = new FileSystemLineageProcessor()
    }
    builder.addProcessor("FileProcessor", fileProcessor, "FileSource")
    
    // API-specific processor
    val apiProcessor = new ProcessorSupplier[String, DataEvent] {
      override def get() = new APILineageProcessor()
    }
    builder.addProcessor("APIProcessor", apiProcessor, "APISource")
    
    // ==================== CONNECT STATE STORES ====================
    
    builder.connectProcessorAndStateStores("GenericProcessor", "lineage-store")
    builder.connectProcessorAndStateStores("DBProcessor", "lineage-store")
    builder.connectProcessorAndStateStores("FileProcessor", "lineage-store")
    builder.connectProcessorAndStateStores("APIProcessor", "lineage-store")
    
    // ==================== SINK ====================
    
    // Single sink for all processed events (all sources converge here)
    builder.addSink(
      "ProcessedSink",
      "processed-events",
      new org.apache.kafka.common.serialization.Serdes.StringSerde().serializer(),
      new ProcessedEventSerde().serializer(),
      "GenericProcessor", "DBProcessor", "FileProcessor", "APIProcessor"
    )
    
    // Lineage events are handled by OpenLineage/Marquez
    
    builder
  }
  
  private def initializeMarquez(): Unit = {
    logger.info("Initializing Marquez datasets for multiple sources...")
    
    // Create datasets for all input sources
    SimpleOpenLineageClient.createDataset("input-events", "Generic input Kafka topic for data events")
    SimpleOpenLineageClient.createDataset("db-events", "Database events from Debezium/JDBC connectors")
    SimpleOpenLineageClient.createDataset("file-events", "File system events from File connectors")
    SimpleOpenLineageClient.createDataset("api-events", "API/Web service events from HTTP connectors")
    
    // Output dataset
    SimpleOpenLineageClient.createDataset("processed-events", "Unified output topic for all processed events")
    
    logger.info("Marquez datasets initialized for 4 source types")
  }
}