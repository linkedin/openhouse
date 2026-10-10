package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper
import com.linkedin.openhouse.spark.OpenHouseCatalog
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import scala.collection.JavaConverters._

private[datasources] object ReplicationDdlForwarder {
  private val PoliciesKey = "policies"
  private val IsReplicatedKey = "openhouse.isTableReplicated"
  val CascadeConfig = "spark.openhouse.replication.ddl.cascade"
  private val CatalogConfigPrefix = "spark.sql.catalog."
  private val ClusterConfigSuffix = ".cluster"
  private val mapper = new ObjectMapper()

  def isOpenHouseCatalog(catalog: TableCatalog): Boolean =
    IcebergCatalogMapper.toIcebergCatalog(catalog).isInstanceOf[OpenHouseCatalog]

  def cascadeEnabled(configuredValue: String): Boolean = configuredValue.toBoolean

  def identifierForDestination(sourceCatalogName: String, identifier: Identifier): Identifier = {
    val namespace = identifier.namespace()
    if (namespace.headOption.contains(sourceCatalogName)) {
      Identifier.of(namespace.drop(1), identifier.name())
    } else {
      identifier
    }
  }

  def replicationDestinations(properties: java.util.Map[String, String]): Seq[String] = {
    if (Option(properties.get(IsReplicatedKey)).exists(_.equalsIgnoreCase("true"))) {
      return Seq.empty
    }
    Option(properties.get(PoliciesKey)).filter(_.nonEmpty) match {
      case None => Seq.empty
      case Some(policyJson) =>
        val config = mapper.readTree(policyJson).path("replication").path("config")
        if (config.isMissingNode || config.isNull) {
          Seq.empty
        } else if (!config.isArray) {
          throw new IllegalArgumentException(
            "Invalid OpenHouse replication policy: replication.config must be an array")
        } else {
          config.elements().asScala.map { entry =>
            val destination = entry.path("destination")
            if (!destination.isTextual) {
              throw new IllegalArgumentException(
                "Invalid OpenHouse replication policy: destination must be a non-empty string")
            }
            val rawDestination = destination.asText().trim
            val clusterId =
              if (
                rawDestination.length >= 2 &&
                  ((rawDestination.head == '\'' && rawDestination.last == '\'') ||
                    (rawDestination.head == '"' && rawDestination.last == '"'))
              ) {
                rawDestination.substring(1, rawDestination.length - 1).trim
              } else {
                rawDestination
              }
            if (clusterId.isEmpty) {
              throw new IllegalArgumentException(
                "Invalid OpenHouse replication policy: destination must be a non-empty string")
            }
            clusterId
          }.toSeq.distinct
        }
    }
  }

  def destinationCatalog(
      spark: SparkSession,
      sourceCatalog: TableCatalog,
      clusterId: String): TableCatalog = {
    val sourceCluster = IcebergCatalogMapper.toIcebergCatalog(sourceCatalog) match {
      case catalog: OpenHouseCatalog => catalog.properties().getOrDefault("cluster", "local")
      case _ =>
        throw new IllegalArgumentException(
          "Cannot forward replication DDL from a non-OpenHouse catalog")
    }
    if (Option(sourceCluster).exists(_.equalsIgnoreCase(clusterId))) {
      throw new IllegalArgumentException(
        s"Replication destination cluster '$clusterId' is the source cluster")
    }

    val settings = spark.sparkContext.getConf.getAll.toMap
    val aliases =
      settings.keysIterator
        .filter(key => key.startsWith(CatalogConfigPrefix) && key.endsWith(ClusterConfigSuffix))
        .map(key => key.stripPrefix(CatalogConfigPrefix).stripSuffix(ClusterConfigSuffix))
        .filter(
          alias =>
            settings(CatalogConfigPrefix + alias + ClusterConfigSuffix)
              .equalsIgnoreCase(clusterId))
        .filter(
          alias =>
            settings.get(CatalogConfigPrefix + alias + ".catalog-impl")
              .contains(classOf[OpenHouseCatalog].getName))
        .toSeq
        .sorted

    aliases.iterator
      .map { alias =>
        val resolved =
          Spark3Util.catalogAndIdentifier(
            spark,
            Seq(alias, "replication_lookup", "replication_lookup").asJava)
        resolved.catalog match {
          case catalog: TableCatalog => Some(catalog)
          case _ => None
        }
      }
      .collectFirst { case Some(catalog) => catalog }
      .getOrElse {
        throw new IllegalStateException(
          s"No OpenHouse catalog configured for replication destination cluster '$clusterId'")
      }
  }
}
