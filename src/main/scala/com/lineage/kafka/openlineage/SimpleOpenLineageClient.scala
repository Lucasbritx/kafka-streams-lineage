package com.lineage.kafka.openlineage

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.client.methods.{HttpPost, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import com.typesafe.scalalogging.LazyLogging

import java.time.Instant

case class OpenLineageEvent(
  eventType: String,
  eventTime: String,
  run: RunInfo,
  job: JobInfo,
  inputs: List[DatasetInfo],
  outputs: List[DatasetInfo],
  producer: String
)

case class RunInfo(
  runId: String
)

case class JobInfo(
  name: String,
  namespace: String
)

case class DatasetInfo(
  name: String,
  namespace: String,
  facets: Option[Map[String, Any]] = None
)

object SimpleOpenLineageClient extends LazyLogging {
  
  private val marquezUrl = sys.env.getOrElse("MARQUEZ_URL", "http://localhost:5002")
  private val namespace = sys.env.getOrElse("OPENLINEAGE_NAMESPACE", "kafka-streams")
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  
  def emitLineageEvent(
    runId: String,
    jobName: String,
    inputs: List[String],
    outputs: List[String]
  ): Unit = {
    
    val event = OpenLineageEvent(
      eventType = "START",
      eventTime = Instant.now().toString,
      run = RunInfo(runId),
      job = JobInfo(jobName, namespace),
      inputs = inputs.map(name => DatasetInfo(name, namespace)),
      outputs = outputs.map(name => DatasetInfo(name, namespace)),
      producer = "kafka-streams-lineage"
    )
    
    try {
      val httpClient: CloseableHttpClient = HttpClients.createDefault()
      val httpPost = new HttpPost(s"$marquezUrl/api/v1/lineage")
      
      httpPost.setHeader("Content-Type", "application/json")
      val json = mapper.writeValueAsString(event)
      httpPost.setEntity(new StringEntity(json))
      
      val response = httpClient.execute(httpPost)
      val statusCode = response.getStatusLine.getStatusCode
      
      if (statusCode >= 200 && statusCode < 300) {
        logger.info(s"Successfully emitted lineage event for run: $runId")
      } else {
        logger.warn(s"Failed to emit lineage event. Status: $statusCode")
      }
      
      httpClient.close()
      
    } catch {
      case e: Exception =>
        logger.error(s"Error emitting lineage event: ${e.getMessage}")
    }
  }
  
  def createDataset(name: String, description: String = ""): Unit = {
    try {
      val httpClient: CloseableHttpClient = HttpClients.createDefault()
      val httpPut = new HttpPut(s"$marquezUrl/api/v1/namespaces/$namespace/datasets/$name")
      
      httpPut.setHeader("Content-Type", "application/json")
      val datasetJson = s"""
        {
          "type": "KAFKA",
          "description": "$description",
          "name": "$name",
          "namespace": "$namespace"
        }
        """
      httpPut.setEntity(new StringEntity(datasetJson))
      
      val response = httpClient.execute(httpPut)
      httpClient.close()
      
      logger.info(s"Created dataset: $name")
      
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to create dataset $name: ${e.getMessage}")
    }
  }
}