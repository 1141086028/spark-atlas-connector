/*
 * test
 */
package com.hortonworks.spark.atlas.types

import java.util
import java.util.{Collections, Map}

import com.hortonworks.spark.atlas.{SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.hortonworks.spark.atlas.sql.KafkaTopicInformation
import com.hortonworks.spark.atlas.utils.{JdbcUtils, Logging, SparkUtils}
import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId, AtlasStruct}
import org.apache.atlas.utils.HdfsNameServiceResolver
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.{Database, FieldSchema, SerDeInfo, StorageDescriptor}
import org.apache.hadoop.hive.ql.metadata.{Hive, Table}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, CatalogTableType}

import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConversions._


/**
 *
 * @author wang_zh
 * @date 2020/6/9
 */
object external extends Logging {

  def pathToEntity(path: String): SACAtlasEntityWithDependencies = {
    val pathEntity = getPathEntity(new Path(path))
    SACAtlasEntityWithDependencies(pathEntity)
  }

  def hbaseTableToEntity(
    cluster: String,
    tableName: String,
    nameSpace: String): SACAtlasEntityWithDependencies = {
    val hbaseEntity = toReferencedHBaseTable(cluster, tableName, nameSpace)
    SACAtlasEntityWithDependencies(hbaseEntity)
  }


  def kafkaToEntity(cluster: String,
    topic: KafkaTopicInformation): SACAtlasEntityWithDependencies = {
    val topicName = topic.topicName.toLowerCase
    val clusterName = topic.clusterName match {
      case Some(customName) => customName
      case None => cluster
    }

    val kafkaEntity = new AtlasEntity(KAFKA_TOPIC_STRING)
    kafkaEntity.setAttribute("qualifiedName", getTopicQualifiedName(clusterName, topicName))
    kafkaEntity.setAttribute("name", topicName)
    kafkaEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, clusterName)
    kafkaEntity.setAttribute("uri", topicName)
    kafkaEntity.setAttribute("topic", topicName)
    kafkaEntity.setAttribute("description", topicName)

    SACAtlasEntityWithDependencies(kafkaEntity)
  }

  def rdbmsTableToEntity(url: String, tableName: String): SACAtlasEntityWithDependencies = {
    val jdbcEntity = new AtlasEntity(RDBMS_TABLE)

    val databaseName = JdbcUtils.getDatabaseName(url)
    jdbcEntity.setAttribute(
      "qualifiedName",
      getRdbmsQualifiedName(getClusterName(), databaseName, tableName))
    jdbcEntity.setAttribute("name", tableName)
    SACAtlasEntityWithDependencies(jdbcEntity)
  }

  def hiveTableToReference(
    tblDefinition: CatalogTable,
    cluster: String,
    mockDbDefinition: Option[CatalogDatabase] = None)
  : SACAtlasReferenceable = {
    //    val db = SparkUtils.getDatabaseName(tblDefinition)
    //    val table = SparkUtils.getTableName(tblDefinition)
    val db: String = tblDefinition.database
    val table = tblDefinition.identifier.table

    hiveTableToReference(db, table, cluster)
  }

  def hiveTableToReference(db: String, table: String, cluster: String): SACAtlasReferenceable = {
    val hiveTable = getHiveTable(db, table)
    val (entity, dependencies) = toTableEntity(hiveTable)
    new SACAtlasEntityWithDependencies(entity, dependencies)
  }

  private def toDbEntity(db: Database): AtlasEntity = {
    val dbQualifiedName = getQualifiedName(db)
    val ret = new AtlasEntity(HIVE_TYPE_DB)
    // if this DB was sent in an earlier notification, set 'guid' to null - which will:
    //  - result in this entity to be not included in 'referredEntities'
    //  - cause Atlas server to resolve the entity by its qualifiedName
    ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, dbQualifiedName)
    ret.setAttribute(ATTRIBUTE_NAME, db.getName.toLowerCase)
    ret.setAttribute(ATTRIBUTE_DESCRIPTION, db.getDescription)
    ret.setAttribute(ATTRIBUTE_OWNER, db.getOwnerName)
    ret.setAttribute(ATTRIBUTE_CLUSTER_NAME, getClusterName)
    ret.setAttribute(
      ATTRIBUTE_LOCATION,
      HdfsNameServiceResolver.getPathWithNameServiceID(db.getLocationUri))
    ret.setAttribute(ATTRIBUTE_PARAMETERS, db.getParameters)
    if (db.getOwnerType != null) {
      ret.setAttribute(ATTRIBUTE_OWNER_TYPE, OWNER_TYPE_TO_ENUM_VALUE.get(db.getOwnerType.getValue))
    }
    ret
  }

  private def toTableEntity(table: Table): Tuple2[AtlasEntity, Seq[SACAtlasReferenceable]] = {
    val tblQualifiedName = getQualifiedName(table)
    val dependencies = new ArrayBuffer[SACAtlasReferenceable]()

    // add db entity
    val db = getHiveDatabase(table.getDbName)
    val dbEntity = toDbEntity(db)

    val ret = new AtlasEntity(HIVE_TYPE_TABLE)
    // if this table was sent in an earlier notification, set 'guid' to null - which will:
    //  - result in this entity to be not included in 'referredEntities'
    //  - cause Atlas server to resolve the entity by its qualifiedName
    val createTime = getTableCreateTime(table)
    val lastAccessTime = if (table.getLastAccessTime > 0) {
      table.getLastAccessTime * MILLIS_CONVERT_FACTOR
    }
    else createTime
    ret.setAttribute(ATTRIBUTE_DB, getObjectId(dbEntity))
    ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tblQualifiedName)
    ret.setAttribute(ATTRIBUTE_NAME, table.getTableName.toLowerCase)
    ret.setAttribute(ATTRIBUTE_OWNER, table.getOwner)
    ret.setAttribute(ATTRIBUTE_CREATE_TIME, createTime)
    ret.setAttribute(ATTRIBUTE_LAST_ACCESS_TIME, lastAccessTime)
    ret.setAttribute(ATTRIBUTE_RETENTION, table.getRetention)
    ret.setAttribute(ATTRIBUTE_PARAMETERS, table.getParameters)
    ret.setAttribute(ATTRIBUTE_COMMENT, table.getParameters.get(ATTRIBUTE_COMMENT))
    ret.setAttribute(ATTRIBUTE_TABLE_TYPE, table.getTableType.name)
    ret.setAttribute(ATTRIBUTE_TEMPORARY, table.isTemporary)
    if (table.getViewOriginalText != null) {
      ret.setAttribute(ATTRIBUTE_VIEW_ORIGINAL_TEXT, table.getViewOriginalText)
    }
    if (table.getViewExpandedText != null) {
      ret.setAttribute(ATTRIBUTE_VIEW_EXPANDED_TEXT, table.getViewExpandedText)
    }
    val tableId = getObjectId(ret)
    val sd = getStorageDescEntity(tableId, table)
    val partitionKeys = getColumnEntities(tableId, table, table.getPartitionKeys)
    val columns = getColumnEntities(tableId, table, table.getCols)
    ret.setAttribute(ATTRIBUTE_STORAGEDESC, getObjectId(sd))
    ret.setAttribute(ATTRIBUTE_PARTITION_KEYS, getObjectIds(partitionKeys))
    ret.setAttribute(ATTRIBUTE_COLUMNS, getObjectIds(columns))
    // add exts
    dependencies += SACAtlasEntityWithDependencies(dbEntity)
    dependencies += SACAtlasEntityWithDependencies(sd)
    dependencies ++= partitionKeys.map(SACAtlasEntityWithDependencies(_))
    dependencies ++= columns.map(SACAtlasEntityWithDependencies(_))
    (ret, dependencies.seq)
  }

  protected def getStorageDescEntity(tableId: AtlasObjectId, table: Table): AtlasEntity = {
    val sdQualifiedName = getQualifiedName(table, table.getSd)
    val isKnownTable = tableId.getGuid == null
    var ret = new AtlasEntity(HIVE_TYPE_STORAGEDESC)
    // if sd's table was sent in an earlier notification, set 'guid' to null - which will:
    //  - result in this entity to be not included in 'referredEntities'
    //  - cause Atlas server to resolve the entity by its qualifiedName
    if (isKnownTable) ret.setGuid(null)
    val sd = table.getSd
    ret.setAttribute(ATTRIBUTE_TABLE, tableId)
    ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, sdQualifiedName)
    ret.setAttribute(ATTRIBUTE_PARAMETERS, sd.getParameters)
    ret.setAttribute(
      ATTRIBUTE_LOCATION, HdfsNameServiceResolver.getPathWithNameServiceID(sd.getLocation))
    ret.setAttribute(ATTRIBUTE_INPUT_FORMAT, sd.getInputFormat)
    ret.setAttribute(ATTRIBUTE_OUTPUT_FORMAT, sd.getOutputFormat)
    ret.setAttribute(ATTRIBUTE_COMPRESSED, sd.isCompressed)
    ret.setAttribute(ATTRIBUTE_NUM_BUCKETS, sd.getNumBuckets)
    ret.setAttribute(ATTRIBUTE_STORED_AS_SUB_DIRECTORIES, sd.isStoredAsSubDirectories)
    if (sd.getBucketCols != null && sd.getBucketCols.size > 0) {
      ret.setAttribute(ATTRIBUTE_BUCKET_COLS, sd.getBucketCols)
    }
    if (sd.getSerdeInfo != null) {
      val serdeInfo = new AtlasStruct(HIVE_TYPE_SERDE)
      val sdSerDeInfo = sd.getSerdeInfo
      serdeInfo.setAttribute(ATTRIBUTE_NAME, sdSerDeInfo.getName)
      serdeInfo.setAttribute(ATTRIBUTE_SERIALIZATION_LIB, sdSerDeInfo.getSerializationLib)
      serdeInfo.setAttribute(ATTRIBUTE_PARAMETERS, sdSerDeInfo.getParameters)
      ret.setAttribute(ATTRIBUTE_SERDE_INFO, serdeInfo)
    }
    if (CollectionUtils.isNotEmpty(sd.getSortCols)) {
      val sortCols = new util.ArrayList[AtlasStruct](sd.getSortCols.size)
      for (sdSortCol <- sd.getSortCols) {
        val sortcol = new AtlasStruct(HIVE_TYPE_ORDER)
        sortcol.setAttribute("col", sdSortCol.getCol)
        sortcol.setAttribute("order", sdSortCol.getOrder)
        sortCols.add(sortcol)
      }
      ret.setAttribute(ATTRIBUTE_SORT_COLS, sortCols)
    }
    ret
  }

  protected def getColumnEntities(
    tableId: AtlasObjectId,
    table: Table,
    fieldSchemas: util.List[FieldSchema]
  ): util.List[AtlasEntity] = {
    val ret = new util.ArrayList[AtlasEntity]
    val isKnownTable = tableId.getGuid == null
    var columnPosition = 0
    if (CollectionUtils.isNotEmpty(fieldSchemas)) {
      for (fieldSchema <- fieldSchemas) {
        val colQualifiedName = getQualifiedName(table, fieldSchema)
        var column = new AtlasEntity(HIVE_TYPE_COLUMN)
        // if column's table was sent in an earlier notification, set 'guid' to null - which will:
        if (isKnownTable) column.setGuid(null)
        column.setAttribute(ATTRIBUTE_TABLE, tableId)
        column.setAttribute(ATTRIBUTE_QUALIFIED_NAME, colQualifiedName)
        column.setAttribute(ATTRIBUTE_NAME, fieldSchema.getName)
        column.setAttribute(ATTRIBUTE_OWNER, table.getOwner)
        column.setAttribute(ATTRIBUTE_COL_TYPE, fieldSchema.getType)
        column.setAttribute(ATTRIBUTE_COL_POSITION, {
          columnPosition += 1;
          columnPosition - 1
        })
        column.setAttribute(ATTRIBUTE_COMMENT, fieldSchema.getComment)
        ret.add(column)
      }
    }
    ret
  }

  private def getTableCreateTime(table: Table) = if (table.getTTable != null) {
    (table.getTTable.getCreateTime * MILLIS_CONVERT_FACTOR).toLong
  } else {
    System.currentTimeMillis
  }


  protected def getQualifiedName(
    db: Database): String =
    (db.getName + QNAME_SEP_CLUSTER_NAME).toLowerCase + getClusterName

  private def getQualifiedName(table: Table) = {
    var tableName = table.getTableName
    if (table.isTemporary) {
      if (SessionState.get != null && SessionState.get.getSessionId != null) {
        tableName = tableName + TEMP_TABLE_PREFIX + SessionState.get.getSessionId
      }
      else tableName = tableName + TEMP_TABLE_PREFIX + RandomStringUtils.random(10)
    }
    (table.getDbName
      + QNAME_SEP_ENTITY_NAME
      + tableName
      + QNAME_SEP_CLUSTER_NAME).toLowerCase + getClusterName
  }

  protected def getQualifiedName(
    table: Table,
    sd: StorageDescriptor
  ): String = getQualifiedName(table) + "_storage"

  protected def getQualifiedName(table: Table, column: FieldSchema): String = {
    val tblQualifiedName = getQualifiedName(table)
    val sepPos = tblQualifiedName.lastIndexOf(QNAME_SEP_CLUSTER_NAME)
    if (sepPos == -1) tblQualifiedName + QNAME_SEP_ENTITY_NAME + column.getName.toLowerCase
    else tblQualifiedName.substring(0, sepPos) + QNAME_SEP_ENTITY_NAME + column.getName
      .toLowerCase +
      tblQualifiedName.substring(sepPos)
  }

  private def getHiveTable(
    databaseName: String,
    tableName: String
  ): Table = Hive.get(SessionState.getSessionConf).getTable(databaseName, tableName)

  private def getHiveDatabase(
    databaseName: String
  ): Database = Hive.get(SessionState.getSessionConf).getDatabase(databaseName)

  private def getRdbmsQualifiedName(cluster: String, databaseName: String, tableName: String) =
    s"${databaseName.toLowerCase}.${tableName.toLowerCase}@$cluster"

  private def getTopicQualifiedName(
    clusterName: String,
    topic: String
  ) =
    String.format(FORMAT_KAKFA_TOPIC_QUALIFIED_NAME, topic.toLowerCase, clusterName)

  private def toReferencedHBaseTable(cluster: String, tableName: String, nameSpace: String) = {
    var ret: AtlasEntity = null
    val hbaseNameSpace = nameSpace
    val hbaseTableName = tableName
    if (hbaseTableName != null) {
      val nsEntity = new AtlasEntity(HBASE_TYPE_NAMESPACE)
      nsEntity.setAttribute(ATTRIBUTE_NAME, hbaseNameSpace)
      nsEntity.setAttribute(ATTRIBUTE_CLUSTER_NAME, cluster)
      nsEntity.setAttribute(
        ATTRIBUTE_QUALIFIED_NAME, getHBaseNameSpaceQualifiedName(cluster, hbaseNameSpace))
      ret = new AtlasEntity(HBASE_TYPE_TABLE)
      ret.setAttribute(ATTRIBUTE_NAME, hbaseTableName)
      ret.setAttribute(ATTRIBUTE_URI, hbaseTableName)
      ret.setAttribute(ATTRIBUTE_NAMESPACE, getObjectId(nsEntity))
      ret.setAttribute(
        ATTRIBUTE_QUALIFIED_NAME,
        getHBaseTableQualifiedName(
          cluster, hbaseNameSpace, hbaseTableName))
    }
    ret
  }

  private def getHBaseTableQualifiedName(
    clusterName: String,
    nameSpace: String,
    tableName: String
  ) =
    String.format("%s:%s@%s",
      nameSpace.toLowerCase,
      tableName.toLowerCase,
      clusterName)

  private def getHBaseNameSpaceQualifiedName(
    clusterName: String,
    nameSpace: String
  ) = String.format(
    "%s@%s", nameSpace.toLowerCase, clusterName)

  private def getPathEntity(path: Path) = {
    var ret: AtlasEntity = null
    var strPath = path.toString
    if (strPath.startsWith(HDFS_PATH_PREFIX)) strPath = strPath.toLowerCase
    if (isS3Path(strPath)) {
      val bucketName = path.toUri.getAuthority
      val bucketQualifiedName = (
        path.toUri.getScheme
          + SCHEME_SEPARATOR
          + path.toUri.getAuthority
          + QNAME_SEP_CLUSTER_NAME).toLowerCase + getClusterName
      val pathQualifiedName = (strPath + QNAME_SEP_CLUSTER_NAME).toLowerCase + getClusterName
      var bucketEntity = new AtlasEntity(AWS_S3_BUCKET)
      bucketEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, bucketQualifiedName)
      bucketEntity.setAttribute(ATTRIBUTE_NAME, bucketName)
      ret = new AtlasEntity(AWS_S3_PSEUDO_DIR)
      ret.setAttribute(ATTRIBUTE_BUCKET, getObjectId(bucketEntity))
      ret.setAttribute(
        ATTRIBUTE_OBJECT_PREFIX, Path.getPathWithoutSchemeAndAuthority(path).toString.toLowerCase)
      ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, pathQualifiedName)
      ret.setAttribute(
        ATTRIBUTE_NAME, Path.getPathWithoutSchemeAndAuthority(path).toString.toLowerCase)
    }
    else {
      val nameServiceID = HdfsNameServiceResolver.getNameServiceIDForPath(strPath)
      val attrPath = if (StringUtils.isEmpty(nameServiceID)) strPath
      else HdfsNameServiceResolver.getPathWithNameServiceID(strPath)
      val pathQualifiedName = getQualifiedName(attrPath)
      ret = new AtlasEntity(HDFS_TYPE_PATH)
      if (StringUtils.isNotEmpty(nameServiceID)) {
        ret.setAttribute(ATTRIBUTE_NAMESERVICE_ID, nameServiceID)
      }
      var name = Path.getPathWithoutSchemeAndAuthority(path).toString
      if (strPath.startsWith(HDFS_PATH_PREFIX)) name = name.toLowerCase
      ret.setAttribute(ATTRIBUTE_PATH, attrPath)
      ret.setAttribute(ATTRIBUTE_QUALIFIED_NAME, pathQualifiedName)
      ret.setAttribute(ATTRIBUTE_NAME, name)
      ret.setAttribute(ATTRIBUTE_CLUSTER_NAME, getClusterName)
    }
    ret
  }

  private def getQualifiedName(path: String): String = {
    if (path.startsWith(HdfsNameServiceResolver.HDFS_SCHEME)) {
      return path + QNAME_SEP_CLUSTER_NAME + getClusterName
    }
    path.toLowerCase
  }

  def getObjectId(entity: AtlasEntity): AtlasObjectId = {
    val qualifiedName = entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME).asInstanceOf[String]
    val ret = new AtlasObjectId(entity.getGuid, entity.getTypeName, Collections.singletonMap
    (ATTRIBUTE_QUALIFIED_NAME, qualifiedName).asInstanceOf[util.Map[String, AnyRef]])
    ret
  }

  def getObjectIds(entities: util.List[AtlasEntity]): util.List[AtlasObjectId] = {
    var ret: util.ArrayList[AtlasObjectId] = null
    if (CollectionUtils.isNotEmpty(entities)) {
      ret = new util.ArrayList[AtlasObjectId](entities.size)
      for (entity <- entities) {
        ret.add(getObjectId(entity))
      }
    }
    else ret = new util.ArrayList[AtlasObjectId]()
    ret
  }

  private def getClusterName(): String = DEFAULT_CLUSTER_NAME

  private def isS3Path(
    strPath: String) = strPath != null && (strPath.startsWith(S3_SCHEME) || strPath.startsWith
  (S3A_SCHEME))

  val TEMP_TABLE_PREFIX = "_temp-"
  val RDBMS_TABLE = "rdbms_table"
  val KAFKA_TOPIC_STRING = "kafka_topic"
  val FORMAT_KAKFA_TOPIC_QUALIFIED_NAME = "%s@%s"
  val QNAME_SEP_CLUSTER_NAME = '@'
  val QNAME_SEP_ENTITY_NAME = '.'
  val QNAME_SEP_PROCESS = ':'
  val DEFAULT_CLUSTER_NAME = "primary"
  val HIVE_TYPE_DB = "hive_db"
  val HIVE_TYPE_TABLE = "hive_table"
  val HIVE_TYPE_STORAGEDESC = "hive_storagedesc"
  val HIVE_TYPE_COLUMN = "hive_column"
  val HIVE_TYPE_PROCESS = "hive_process"
  val HIVE_TYPE_COLUMN_LINEAGE = "hive_column_lineage"
  val HIVE_TYPE_SERDE = "hive_serde"
  val HIVE_TYPE_ORDER = "hive_order"
  val HDFS_TYPE_PATH = "hdfs_path"
  val HBASE_TYPE_TABLE = "hbase_table"
  val HBASE_TYPE_NAMESPACE = "hbase_namespace"
  val AWS_S3_BUCKET = "aws_s3_bucket"
  val AWS_S3_PSEUDO_DIR = "aws_s3_pseudo_dir"
  val AWS_S3_OBJECT = "aws_s3_object"

  val SCHEME_SEPARATOR = "://"
  val S3_SCHEME: String = "s3" + SCHEME_SEPARATOR
  val S3A_SCHEME: String = "s3a" + SCHEME_SEPARATOR

  val ATTRIBUTE_QUALIFIED_NAME = "qualifiedName"
  val ATTRIBUTE_NAME = "name"
  val ATTRIBUTE_DESCRIPTION = "description"
  val ATTRIBUTE_OWNER = "owner"
  val ATTRIBUTE_CLUSTER_NAME = "clusterName"
  val ATTRIBUTE_LOCATION = "location"
  val ATTRIBUTE_PARAMETERS = "parameters"
  val ATTRIBUTE_OWNER_TYPE = "ownerType"
  val ATTRIBUTE_COMMENT = "comment"
  val ATTRIBUTE_CREATE_TIME = "createTime"
  val ATTRIBUTE_LAST_ACCESS_TIME = "lastAccessTime"
  val ATTRIBUTE_VIEW_ORIGINAL_TEXT = "viewOriginalText"
  val ATTRIBUTE_VIEW_EXPANDED_TEXT = "viewExpandedText"
  val ATTRIBUTE_TABLE_TYPE = "tableType"
  val ATTRIBUTE_TEMPORARY = "temporary"
  val ATTRIBUTE_RETENTION = "retention"
  val ATTRIBUTE_DB = "db"
  val ATTRIBUTE_STORAGEDESC = "sd"
  val ATTRIBUTE_PARTITION_KEYS = "partitionKeys"
  val ATTRIBUTE_COLUMNS = "columns"
  val ATTRIBUTE_INPUT_FORMAT = "inputFormat"
  val ATTRIBUTE_OUTPUT_FORMAT = "outputFormat"
  val ATTRIBUTE_COMPRESSED = "compressed"
  val ATTRIBUTE_BUCKET_COLS = "bucketCols"
  val ATTRIBUTE_NUM_BUCKETS = "numBuckets"
  val ATTRIBUTE_STORED_AS_SUB_DIRECTORIES = "storedAsSubDirectories"
  val ATTRIBUTE_TABLE = "table"
  val ATTRIBUTE_SERDE_INFO = "serdeInfo"
  val ATTRIBUTE_SERIALIZATION_LIB = "serializationLib"
  val ATTRIBUTE_SORT_COLS = "sortCols"
  val ATTRIBUTE_COL_TYPE = "type"
  val ATTRIBUTE_COL_POSITION = "position"
  val ATTRIBUTE_PATH = "path"
  val ATTRIBUTE_NAMESERVICE_ID = "nameServiceId"
  val ATTRIBUTE_INPUTS = "inputs"
  val ATTRIBUTE_OUTPUTS = "outputs"
  val ATTRIBUTE_OPERATION_TYPE = "operationType"
  val ATTRIBUTE_START_TIME = "startTime"
  val ATTRIBUTE_USER_NAME = "userName"
  val ATTRIBUTE_QUERY_TEXT = "queryText"
  val ATTRIBUTE_QUERY_ID = "queryId"
  val ATTRIBUTE_QUERY_PLAN = "queryPlan"
  val ATTRIBUTE_END_TIME = "endTime"
  val ATTRIBUTE_RECENT_QUERIES = "recentQueries"
  val ATTRIBUTE_QUERY = "query"
  val ATTRIBUTE_DEPENDENCY_TYPE = "depenendencyType"
  val ATTRIBUTE_EXPRESSION = "expression"
  val ATTRIBUTE_ALIASES = "aliases"
  val ATTRIBUTE_URI = "uri"
  val ATTRIBUTE_STORAGE_HANDLER = "storage_handler"
  val ATTRIBUTE_NAMESPACE = "namespace"
  val ATTRIBUTE_OBJECT_PREFIX = "objectPrefix"
  val ATTRIBUTE_BUCKET = "bucket"

  val HBASE_STORAGE_HANDLER_CLASS = "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
  val HBASE_DEFAULT_NAMESPACE = "default"
  val HBASE_NAMESPACE_TABLE_DELIMITER = ":"
  val HBASE_PARAM_TABLE_NAME = "hbase.table.name"
  val MILLIS_CONVERT_FACTOR = 1000
  val HDFS_PATH_PREFIX = "hdfs://"

  val OWNER_TYPE_TO_ENUM_VALUE = new util.HashMap[Integer, String] {
    {
      put(1, "USER")
      put(2, "ROLE")
      put(3, "GROUP")
    }
  }
}
