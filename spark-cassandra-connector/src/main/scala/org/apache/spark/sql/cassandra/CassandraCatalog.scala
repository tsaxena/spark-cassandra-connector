package org.apache.spark.sql.cassandra

import java.io.IOException

import com.google.common.cache.{LoadingCache, CacheBuilder, CacheLoader}

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, Catalog}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.types.StructType

import CassandraDefaultSource._

private[cassandra] class CassandraCatalog(cc: CassandraSQLContext) extends Catalog with Logging {

  val caseSensitive: Boolean = true
  val metaStore: MetaStore = new DataSourceMetaStore(cc)

  // Create metastore keyspace and table if they don't exist
  metaStore.init()

  /** A cache of Spark SQL data source tables that have been accessed. Cache is thread safe.*/
  private[cassandra] val cachedDataSourceTables: LoadingCache[Seq[String], LogicalPlan] = {
    val cacheLoader = new CacheLoader[Seq[String], LogicalPlan]() {
      override def load(tableIdent: Seq[String]): LogicalPlan = {
        logDebug(s"Creating new cached data source for $tableIdent")
        synchronized {
          metaStore.getTable(tableIdentFrom(tableIdent))
        }
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  /** Obtain the Relation for a Cassandra table */
  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val tableName = id(0).getOrElse(throw new IOException(s"Missing table name"))
    val relation = cachedDataSourceTables.get(tableIdentifier)
    alias.map(a => Subquery(a, relation)).getOrElse(Subquery(tableName, relation))
  }

  /**
   * Only register table to local cache. To register table in metastore, use
   * registerTable(tableIdent, source, schema, options) method
   */
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    val fullTableIdent = fullTableIdentFrom(tableIdentifier)
    cachedDataSourceTables.put(fullTableIdent, plan)
  }

  /** Register a customized table meta data to local cache and metastore */
  def registerTable(
      tableIdent: TableIdent,
      source: String,
      schema: Option[StructType],
      options: Map[String, String]): Unit = {

    val fullOptions =
      if (source == CassandraDataSourceProviderName ||
        source == CassandraDataSourceProviderFullName) {
        cc.optionsWithTableIdent(tableIdent, options)
      } else {
        options
      }
    synchronized {
      metaStore.storeTable(tableIdent, source, schema, fullOptions)
    }
  }

  /** Unregister table from local cache and metastore. */
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val fullTableIdent = fullTableIdentFrom(tableIdentifier)
    cachedDataSourceTables.invalidate(fullTableIdent)
    synchronized {
      metaStore.removeTable(tableIdentFrom(tableIdentifier))
    }
  }

  /** Unregister table from local cache and metastore. */
  def unregisterTable(tableIdent: TableIdent): Unit = {
    unregisterTable(catalystTableIdentFrom(tableIdent))
  }

  /** Unregister database from local cache and metastore. */
  def unregisterDatabase(database: String, cluster: Option[String]): Unit = {
    unregisterDatabaseFromCache(database, cluster)
    synchronized {
      metaStore.removeDatabase(database, cluster)
    }
  }

  private def unregisterDatabaseFromCache(database: String, cluster: Option[String]): Unit = {
    val tables = getTables(Option(database), cluster)
    val dbAndCluster = if (cluster.nonEmpty) Seq(cluster.get, database) else Seq(database)
    for (table <- tables) {
      val tableIdentifier = dbAndCluster ++ Seq(table)
      cachedDataSourceTables.invalidate(tableIdentifier)
    }
  }
  /** Unregister cluster from local cache and metastore. */
  def unregisterCluster(cluster: String): Unit = {
    val databases = getDatabases(Option(cluster))
    for (database <- databases) {
      unregisterDatabaseFromCache(database, Option(cluster))
    }
    synchronized {
      metaStore.removeCluster(cluster)
    }
  }

  /** Unregister all tables from local cache and metastore. */
  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
    synchronized {
      metaStore.removeAllTables()
    }
  }

  /** Check whether table exists */
  override def tableExists(tableIdentifier: Seq[String]): Boolean = synchronized {
    try {
      cachedDataSourceTables.get(tableIdentifier) != null
    } catch {
      case _: NoSuchTableException => false
    }
    false
  }

  /** Check whether table exists */
  def tableExists(tableIdent: TableIdent): Boolean = synchronized {
    tableExists(catalystTableIdentFrom(tableIdent))
  }

  /** Check whether table is stored in metastore */
  def tableExistsInMetastore(tableIdent: TableIdent): Boolean = synchronized {
    metaStore.getTableFromMetastore(tableIdent).nonEmpty
  }

  /** All tables are not temporary tables */
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    getTables(databaseName, None)
  }

  /** Get all tables for given database name and cluster */
  def getTables(databaseName: Option[String], cluster: Option[String] = None): Seq[(String, Boolean)] = synchronized {
    metaStore.getAllTables(databaseName, cluster)
  }

  /** Get all tables for given database name and cluster */
  def getDatabases(cluster: Option[String] = None): Seq[String] = synchronized {
    metaStore.getAllDatabases(cluster)
  }

  /** Get all tables for given database name and cluster */
  def getClusters(): Seq[String] = synchronized {
    metaStore.getAllClusters()
  }

  /** Create a database in metastore */
  def createDatabase(database: String, cluster: Option[String]): Unit = synchronized {
    metaStore.storeDatabase(database, cluster)
  }

  /** Create a cluster in metastore */
  def createCluster(cluster: String): Unit = synchronized {
    metaStore.storeCluster(cluster)
  }

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  def refreshTable(tableIdent: TableIdent): Unit = {
    cc.refreshCassandraSchema(tableIdent.cluster.getOrElse(cc.getCluster))
    cachedDataSourceTables.refresh(catalystTableIdentFrom(tableIdent))
  }

  /** Refresh CassandraContext schema cache, then refresh table in local cache */
  override def refreshTable(databaseName: String, tableName: String): Unit = {
    cc.refreshCassandraSchema(cc.getCluster)
    cachedDataSourceTables.refresh(Seq(cc.getCluster, databaseName, tableName))
  }

  /** Return table metadata */
  def getTableMetadata(tableIdent : TableIdent) : Option[TableMetaData] = synchronized {
    metaStore.getTableMetaData(tableIdent)
  }

  /** Convert Catalyst tableIdentifier to TableIdent */
  def tableIdentFrom(tableIdentifier: Seq[String]) : TableIdent = {
    val id = processTableIdentifier(tableIdentifier).reverse.lift
    val clusterName = id(2).getOrElse(cc.getCluster).trim
    val keyspaceName = id(1).getOrElse(cc.getKeyspace).trim
    val tableName = id(0).getOrElse(throw new IOException(s"Missing table name"))
    TableIdent(tableName.trim, keyspaceName, Option(clusterName))
  }

  /** Convert TableIdent to Catalyst tableIdentifier */
  def catalystTableIdentFrom(tableIdent: TableIdent) : Seq[String] =
    Seq(tableIdent.cluster.getOrElse(cc.getCluster), tableIdent.keyspace, tableIdent.table)

  private[this] def fullTableIdentFrom(tableIdentifier: Seq[String]) : Seq[String] = {
    catalystTableIdentFrom(tableIdentFrom(tableIdentifier))
  }
}

object CassandraCatalog {
  val CassandraSQLSourceProviderDisableProperty = "spark.cassandra.sql.sources.disable"
}