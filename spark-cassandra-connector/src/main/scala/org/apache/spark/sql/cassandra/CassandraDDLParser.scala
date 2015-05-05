package org.apache.spark.sql.cassandra

import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.DDLException

private [cassandra] class CassandraDDLParser(parseQuery: String => LogicalPlan) extends AbstractSparkSQLParser {

  def apply(input: String, exceptionOnError: Boolean): Option[LogicalPlan] = {
    try {
      Some(apply(input))
    } catch {
      case ddlException: DDLException => throw ddlException
      case _ if !exceptionOnError => None
      case x: Throwable => throw x
    }
  }

  protected val USE  = Keyword("USE")
  protected val SHOW  = Keyword("SHOW")
  protected val DROP  = Keyword("DROP")
  protected val CREATE  = Keyword("CREATE")
  protected val ALTER  = Keyword("ALTER")
  protected val RENAME  = Keyword("RENAME")
  protected val IN  = Keyword("IN")
  protected val CLUSTER  = Keyword("CLUSTER")
  protected val DATABASE  = Keyword("DATABASE")
  protected val TABLE  = Keyword("TABLE")
  protected val CLUSTERS  = Keyword("CLUSTERS")
  protected val DATABASES  = Keyword("DATABASES")
  protected val TABLES  = Keyword("TABLES")
  protected val TO  = Keyword("TO")

  protected lazy val start: Parser[LogicalPlan] =
    useCluster | useDatabase |
    showDatabases | showTables | showClusters |
    createDatabase | createCluster |
    dropDatabase | dropCluster | dropTable |
    renameTable

  private lazy val useCluster: Parser[LogicalPlan] =
    USE ~ CLUSTER ~> restInput ^^ {
      case input => UseCluster(input.trim)
    }

  private lazy val useDatabase: Parser[LogicalPlan] =
    USE ~ DATABASE ~> restInput ^^ {
      case input => UseDatabase(input.trim)
    }

  private lazy val showClusters: Parser[LogicalPlan] =
    SHOW ~ CLUSTERS ^^ {
      case input => ShowClusters()
    }

  private lazy val showDatabases: Parser[LogicalPlan] =
    SHOW ~ DATABASES ~ opt(IN) ~> repsep(ident, ".") ^^ {
      case input => ShowDatabases(input)
    }

  private lazy val showTables: Parser[LogicalPlan] =
    SHOW ~ TABLES  ~ opt(IN) ~> repsep(ident, ".") ^^ {
      case input => ShowTables(input)
    }

  private lazy val createDatabase: Parser[LogicalPlan] =
    CREATE ~ DATABASE ~> repsep(ident, ".") ^^ {
      case input => CreateDatabase(input)
    }

  private lazy val createCluster: Parser[LogicalPlan] =
    CREATE ~ CLUSTER  ~> restInput ^^ {
      case input => CreateCluster(input.trim)
    }

  private lazy val dropDatabase: Parser[LogicalPlan] =
    DROP ~ DATABASE ~> repsep(ident, ".") ^^ {
      case input => DropDatabase(input)
    }

  private lazy val dropCluster: Parser[LogicalPlan] =
    DROP ~ CLUSTER  ~> restInput ^^ {
      case input => DropCluster(input.trim)
    }

  private lazy val dropTable: Parser[LogicalPlan] =
    DROP ~ TABLE ~> repsep(ident, ".") ^^ {
      case input => DropTable(input)
    }

  private lazy val renameTable: Parser[LogicalPlan] =
    (ALTER ~ TABLE ~> repsep(ident, ".")) ~ (RENAME ~ TO ~> restInput) ^^ {
      case oldName ~ newName => RenameTable(oldName, newName.trim)
    }
}
