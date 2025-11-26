package com.lineage.kafka.serialization

import com.lineage.kafka.model.{DataEvent, LineageEvent, ProcessedEvent}
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class DataEventSerializer extends Serializer[DataEvent] {
  import DataEvent._
  override def serialize(topic: String, data: DataEvent): Array[Byte] = {
    data.asJson.noSpaces.getBytes("UTF-8")
  }
}

class DataEventDeserializer extends Deserializer[DataEvent] {
  import DataEvent._
  override def deserialize(topic: String, data: Array[Byte]): DataEvent = {
    val json = new String(data, "UTF-8")
    decode[DataEvent](json) match {
      case Right(event) => event
      case Left(error) => throw new RuntimeException(s"Failed to deserialize DataEvent: $error")
    }
  }
}

class LineageEventSerializer extends Serializer[LineageEvent] {
  import LineageEvent._
  override def serialize(topic: String, data: LineageEvent): Array[Byte] = {
    data.asJson.noSpaces.getBytes("UTF-8")
  }
}

class LineageEventDeserializer extends Deserializer[LineageEvent] {
  import LineageEvent._
  override def deserialize(topic: String, data: Array[Byte]): LineageEvent = {
    val json = new String(data, "UTF-8")
    decode[LineageEvent](json) match {
      case Right(event) => event
      case Left(error) => throw new RuntimeException(s"Failed to deserialize LineageEvent: $error")
    }
  }
}

class ProcessedEventSerializer extends Serializer[ProcessedEvent] {
  import ProcessedEvent._
  override def serialize(topic: String, data: ProcessedEvent): Array[Byte] = {
    data.asJson.noSpaces.getBytes("UTF-8")
  }
}

class ProcessedEventDeserializer extends Deserializer[ProcessedEvent] {
  import ProcessedEvent._
  override def deserialize(topic: String, data: Array[Byte]): ProcessedEvent = {
    val json = new String(data, "UTF-8")
    decode[ProcessedEvent](json) match {
      case Right(event) => event
      case Left(error) => throw new RuntimeException(s"Failed to deserialize ProcessedEvent: $error")
    }
  }
}

class DataEventSerde extends Serde[DataEvent] {
  override def serializer(): Serializer[DataEvent] = new DataEventSerializer
  override def deserializer(): Deserializer[DataEvent] = new DataEventDeserializer
}

class LineageEventSerde extends Serde[LineageEvent] {
  override def serializer(): Serializer[LineageEvent] = new LineageEventSerializer
  override def deserializer(): Deserializer[LineageEvent] = new LineageEventDeserializer
}

class ProcessedEventSerde extends Serde[ProcessedEvent] {
  override def serializer(): Serializer[ProcessedEvent] = new ProcessedEventSerializer
  override def deserializer(): Deserializer[ProcessedEvent] = new ProcessedEventDeserializer
}