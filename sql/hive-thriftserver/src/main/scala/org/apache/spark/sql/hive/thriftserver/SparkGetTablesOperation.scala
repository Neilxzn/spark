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

package org.apache.spark.sql.hive.thriftserver


import java.util
import java.util.{List => JList, UUID }

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveOperationType, HivePrivilegeObjectUtils}
import org.apache.hive.service.AbstractService
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.GetTablesOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils.invoke



/**
 * Spark's own GetTablesOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a HiveSession from SessionManager
 * @param catalogName catalog name. NULL if not applicable.
 * @param schemaName database name, null or a concrete database name
 * @param tableName table name pattern
 * @param tableTypes list of allowed table types, e.g. "TABLE", "VIEW"
 */
private[hive] class SparkGetTablesOperation(
    sqlContext: SQLContext,
    parentSession: HiveSession,
    catalogName: String,
    schemaName: String,
    tableName: String,
    tableTypes: JList[String])
  extends GetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes)
  with Logging {

  val catalog: SessionCatalog = sqlContext.sessionState.catalog

  private final val RESULT_SET_SCHEMA = new TableSchema()
    .addStringColumn("TABLE_CAT", "Catalog name. NULL if not applicable.")
    .addStringColumn("TABLE_SCHEM", "Schema name.")
    .addStringColumn("TABLE_NAME", "Table name.")
    .addStringColumn("TABLE_TYPE", "The table type, e.g. \"TABLE\", \"VIEW\", etc.")
    .addStringColumn("REMARKS", "Comments about the table.")

  private val rowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion)

  private val sparkToClientMapping = Map(EXTERNAL -> "TABLE", MANAGED -> "TABLE", VIEW -> "VIEW")

  if (tableTypes != null) {
    this.tableTypes.addAll(tableTypes)
  }

  private var statementId: String = _

  override def close(): Unit = {
    logInfo(s"Close get tables with $statementId")
    setState(OperationState.CLOSED)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    logInfo(s"Getting tables with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    try {

      val schemaPattern: String = convertSchemaPattern(schemaName)
      val matchingDbs = catalog.listDatabases(schemaPattern)
      if (isAuthV2Enabled) {
        val privObjs =
          HivePrivilegeObjectUtils.getHivePrivDbObjects(seqAsJavaListConverter(matchingDbs).asJava)
        val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName"
        authorizeMetaGets(HiveOperationType.GET_TABLES, privObjs, cmdStr)
      }
      val tablePattern = convertIdentifierPattern(tableName, true)
      for (dbName <- matchingDbs) {
        val tableNames = catalog.listTables(dbName, tablePattern)
        val tablenamestrs = tableNames.map{ o => o.table}

        catalog.getTablesMetadata(dbName,
          catalog.listTables(dbName, tablePattern).map(o => o.table).toList)
          .foreach{ catalogTable =>
            val tableType = sparkToClientMapping(catalogTable.tableType)
            if (tableTypes == null || tableTypes.isEmpty || tableTypes.contains(tableType)) {
              val rowData = Array[AnyRef](
                "",
                catalogTable.database,
                catalogTable.identifier.table,
                tableType,
                catalogTable.comment.getOrElse(""))
              rowSet.addRow(rowData)
            }
        }
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: Exception =>
        setState(OperationState.ERROR)
        throw new HiveSQLException(e)
    }
  }

  override def getNextRowSet(order: FetchOrientation, maxRows: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    if (order.equals(FetchOrientation.FETCH_FIRST)) {
      rowSet.setStartOffset(0)
    }
    rowSet.extractSubset(maxRows.toInt)
  }

  override def cancel(): Unit = {
    logInfo(s"Cancel get tables with $statementId")
    setState(OperationState.CANCELED)
  }
}
