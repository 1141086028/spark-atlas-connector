/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas.types

import java.io.File
import java.net.{URI, URISyntaxException}
import java.util.Date

import com.hortonworks.spark.atlas.sql.KafkaTopicInformation
import scala.collection.JavaConverters._
import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.commons.lang.RandomStringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}
import com.hortonworks.spark.atlas.{AtlasUtils, SACAtlasEntityReference, SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.hortonworks.spark.atlas.utils.{JdbcUtils, SparkUtils}
import org.apache.spark.sql.SparkSession


object external {
  // External metadata types used to link with external entities

  // ================ File system entities ======================
  val FS_PATH_TYPE_STRING = "fs_path"
  val HDFS_PATH_TYPE_STRING = "hdfs_path"
  val S3_OBJECT_TYPE_STRING = "aws_s3_object"
  val S3_PSEUDO_DIR_TYPE_STRING = "aws_s3_pseudo_dir"
  val S3_BUCKET_TYPE_STRING = "aws_s3_bucket"

  private def isS3Schema(schema: String): Boolean = schema.matches("s3[an]?")

  private def extractS3Entity(uri: URI, fsPath: Path): SACAtlasEntityWithDependencies = {
    val path = Path.getPathWithoutSchemeAndAuthority(fsPath).toString

    val bucketName = uri.getAuthority
    val bucketQualifiedName = s"s3://${bucketName}"
    val dirName = path.replaceFirst("[^/]*$", "")
    val dirQualifiedName = bucketQualifiedName + dirName
    val objectName = path.replaceFirst("^.*/", "")
    val objectQualifiedName = dirQualifiedName + objectName

    // bucket
    val bucketEntity = new AtlasEntity(S3_BUCKET_TYPE_STRING)
    bucketEntity.setAttribute("name", bucketName)
    bucketEntity.setAttribute("qualifiedName", bucketQualifiedName)

    // pseudo dir
    val dirEntity = new AtlasEntity(S3_PSEUDO_DIR_TYPE_STRING)
    dirEntity.setAttribute("name", dirName)
    dirEntity.setAttribute("qualifiedName", dirQualifiedName)
    dirEntity.setAttribute("objectPrefix", dirQualifiedName)
    dirEntity.setAttribute("bucket", AtlasUtils.entityToReference(bucketEntity))

    // object
    val objectEntity = new AtlasEntity(S3_OBJECT_TYPE_STRING)
    objectEntity.setAttribute("name", objectName)
    objectEntity.setAttribute("path", path)
    objectEntity.setAttribute("qualifiedName", objectQualifiedName)
    objectEntity.setAttribute("pseudoDirectory", AtlasUtils.entityToReference(dirEntity))

    // dir entity depends on bucket entity
    val dirEntityWithDeps = new SACAtlasEntityWithDependencies(dirEntity,
      Seq(SACAtlasEntityWithDependencies(bucketEntity)))

    // object entity depends on dir entity
    new SACAtlasEntityWithDependencies(objectEntity, Seq(dirEntityWithDeps))
  }

  def pathToEntity(path: String): SACAtlasEntityWithDependencies = {
    val uri = resolveURI(path)
    val fsPath = new Path(uri)
    if (uri.getScheme == "hdfs") {
      val entity = new AtlasEntity(HDFS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)
      entity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, uri.getAuthority)

      SACAtlasEntityWithDependencies(entity)
    } else if (isS3Schema(uri.getScheme)) {
      extractS3Entity(uri, fsPath)
    } else {
      val entity = new AtlasEntity(FS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)

      SACAtlasEntityWithDependencies(entity)
    }
  }

  private def qualifiedPath(path: String): Path = {
    val p = new Path(path)
    val fs = p.getFileSystem(SparkUtils.sparkSession.sparkContext.hadoopConfiguration)
    p.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  private def resolveURI(path: String): URI = {
    val uri = new URI(path)
    if (uri.getScheme() != null) {
      return uri
    }

    val qUri = qualifiedPath(path).toUri

    // Path.makeQualified always converts the path as absolute path, but for file scheme
    // it provides different prefix.
    // (It provides prefix as "file" instead of "file://", which both are actually valid.)
    // Given we have been providing it as "file://", the logic below changes the scheme of
    // type "file" to "file://".
    if (qUri.getScheme == "file") {
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (qUri.getFragment() != null) {
        val absoluteURI = new File(qUri.getPath()).getAbsoluteFile().toURI()
        new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      } else {
        new File(path).getAbsoluteFile().toURI()
      }
    } else {
      qUri
    }
  }

  // ================ HBase entities ======================
  val HBASE_NAMESPACE_STRING = "hbase_namespace"
  val HBASE_TABLE_STRING = "hbase_table"
  val HBASE_COLUMNFAMILY_STRING = "hbase_column_family"
  val HBASE_COLUMN_STRING = "hbase_column"
  val HBASE_TABLE_QUALIFIED_NAME_FORMAT = "%s:%s@%s"

  def hbaseTableToEntity(
      cluster: String,
      tableName: String,
      nameSpace: String): SACAtlasEntityWithDependencies = {
    val hbaseEntity = new AtlasEntity(HBASE_TABLE_STRING)
    hbaseEntity.setAttribute("qualifiedName",
      getTableQualifiedName(cluster, nameSpace, tableName))
    hbaseEntity.setAttribute("name", tableName.toLowerCase)
    hbaseEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    hbaseEntity.setAttribute("uri", nameSpace.toLowerCase + ":" + tableName.toLowerCase)

    SACAtlasEntityWithDependencies(hbaseEntity)
  }

  private def getTableQualifiedName(
      clusterName: String,
      nameSpace: String,
      tableName: String): String = {
    if (clusterName == null || nameSpace == null || tableName == null) {
      null
    } else {
      String.format(HBASE_TABLE_QUALIFIED_NAME_FORMAT, nameSpace.toLowerCase,
        tableName.toLowerCase.substring(tableName.toLowerCase.indexOf(":") + 1), clusterName)
    }
  }

  // ================ Kafka entities =======================
  val KAFKA_TOPIC_STRING = "kafka_topic"

  def kafkaToEntity(
      cluster: String,
      topic: KafkaTopicInformation): SACAtlasEntityWithDependencies = {
    val topicName = topic.topicName.toLowerCase
    val clusterName = topic.clusterName match {
      case Some(customName) => customName
      case None => cluster
    }

    val kafkaEntity = new AtlasEntity(KAFKA_TOPIC_STRING)
    kafkaEntity.setAttribute("qualifiedName", topicName + '@' + clusterName)
    kafkaEntity.setAttribute("name", topicName)
    kafkaEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName)
    kafkaEntity.setAttribute("uri", topicName)
    kafkaEntity.setAttribute("topic", topicName)

    SACAtlasEntityWithDependencies(kafkaEntity)
  }

  // ================ RDBMS based entities ======================
  val RDBMS_TABLE = "rdbms_table"

  /**
   * Converts JDBC RDBMS properties into Atlas entity
   *
   * @param url
   * @param tableName
   * @return
   */
  def rdbmsTableToEntity(url: String, tableName: String): SACAtlasEntityWithDependencies = {
    val jdbcEntity = new AtlasEntity(RDBMS_TABLE)

    val databaseName = JdbcUtils.getDatabaseName(url)
    jdbcEntity.setAttribute("qualifiedName", getRdbmsQualifiedName(databaseName, tableName))
    jdbcEntity.setAttribute("name", tableName)

    SACAtlasEntityWithDependencies(jdbcEntity)
  }

  /**
   * Constructs the the full qualified name of the databse
   *
   * @param databaseName
   * @param tableName
   * @return
   */
  private def getRdbmsQualifiedName(databaseName: String, tableName: String): String =
    s"${databaseName.toLowerCase}.${tableName.toLowerCase}"

  // ================== Hive Catalog entities =====================
  val HIVE_TABLE_TYPE_STRING = "hive_table"

  // scalastyle:off
  /**
   * This is based on the logic how Hive Hook defines qualifiedName for Hive DB (borrowed from Apache Atlas v1.1).
   * https://github.com/apache/atlas/blob/release-1.1.0-rc2/addons/hive-bridge/src/main/java/org/apache/atlas/hive/bridge/HiveMetaStoreBridge.java#L833-L841
   *
   * As we cannot guarantee same qualifiedName for temporary table, we just don't support
   * temporary table in SAC.
   */
  // scalastyle:on
  def hiveTableUniqueAttribute(
      cluster: String,
      db: String,
      table: String): String = {
    s"${db.toLowerCase}.${table.toLowerCase}@$cluster"
  }

  def hiveTableToReference(
      tblDefinition: CatalogTable,
      clusterName: String,
      mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = SparkUtils.getDatabaseName(tableDefinition)
    val table = SparkUtils.getTableName(tableDefinition)
    val dbDefinition = mockDbDefinition.getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntity = hiveDbToEntity(dbDefinition, clusterName, tableDefinition.owner)
    val sdEntity = hiveStorageFormatToEntity(tableDefinition.storage, db, table, clusterName)

    val tblEntity = new AtlasEntity(metadata.TABLE_TYPE_STRING)

    tblEntity.setAttribute("qualifiedName",
      hiveTableUniqueAttribute(db, table, clusterName))
    tblEntity.setAttribute("name", table)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("schemaDesc", tableDefinition.schema.simpleString)
    tblEntity.setAttribute("provider", tableDefinition.provider.getOrElse(""))
    if (tableDefinition.tracksPartitionsInCatalog) {
      tblEntity.setAttribute("partitionProvider", "Catalog")
    }
    tblEntity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => tblEntity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("ownerType", "USER")
    tblEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tblEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    tblEntity.setRelationshipAttribute("db", dbEntity.asObjectId)
    tblEntity.setRelationshipAttribute("sd", sdEntity.asObjectId)

    new SACAtlasEntityWithDependencies(tblEntity, Seq(dbEntity, sdEntity))

  }

  def hiveTableToReference(
      db: String,
      table: String,
      cluster: String): SACAtlasReferenceable = {
    val tblDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
    hiveTableToReference(tblDefinition, cluster)
  }

  def hiveDbToEntity(dbDefinition: CatalogDatabase,
                       cluster: String,
                       owner: String): SACAtlasEntityWithDependencies = {
    val dbEntity = new AtlasEntity(metadata.DB_TYPE_STRING)

    dbEntity.setAttribute(
      "qualifiedName", hiveDbUniqueAttribute(dbDefinition.name, cluster))
    dbEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    dbEntity.setAttribute("name", dbDefinition.name)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("location", dbDefinition.locationUri.toString)
    dbEntity.setAttribute("parameters", dbDefinition.properties.asJava)
    dbEntity.setAttribute("owner", owner)
    dbEntity.setAttribute("ownerType", "USER")

    SACAtlasEntityWithDependencies(dbEntity)
  }

  def hiveDbUniqueAttribute(db: String,
                             cluster: String): String = {
    s"${db.toLowerCase}@$cluster"
  }

  def hiveStorageFormatToEntity(storageFormat: CatalogStorageFormat,
                                  db: String,
                                  table: String,
                                  cluster: String): SACAtlasEntityWithDependencies = {
    val sdEntity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)

    sdEntity.setAttribute("qualifiedName",
      hiveStorageFormatUniqueAttribute(db, table, cluster))
    storageFormat.locationUri.foreach(uri => sdEntity.setAttribute("location", uri.toString))
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)

    SACAtlasEntityWithDependencies(sdEntity)
  }

  def hiveStorageFormatUniqueAttribute(
                                         db: String,
                                         table: String,
                                         cluster: String): String = {
    s"${db.toLowerCase}.${table.toLowerCase}@${cluster}_storage"
  }

//  def hiveTableToReference(
//      tblDefinition: CatalogTable,
//      cluster: String,
//      mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
//    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
//    val db = SparkUtils.getDatabaseName(tableDefinition)
//    val table = SparkUtils.getTableName(tableDefinition)
//    hiveTableToReference(db, table, cluster)
//  }

//  def hiveTableToReference(
//      db: String,
//      table: String,
//      cluster: String): SACAtlasReferenceable = {
//    val qualifiedName = hiveTableUniqueAttribute(cluster, db, table)
//    SACAtlasEntityReference(
//      new AtlasObjectId(HIVE_TABLE_TYPE_STRING, "qualifiedName", qualifiedName))
//  }
}
