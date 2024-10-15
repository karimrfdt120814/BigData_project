import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object TableEnvRecon {

  def main(args:Array[String]):Unit= {
    val jdbcHostname = "localhost" // MySQL host
    val jdbcPort = 3306 // MySQL port (default is 3306)
    val jdbcDatabase = "olc_sql" // Your MySQL database name
    val jdbcUsername = "root" // Your MySQL username
    val jdbcPassword = "root" // Your MySQL password

    val uat_url = s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"
    val sit_url = s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

    val sc = new SparkContext("local[*]", "mysql_jdbc")

    val spark = SparkSession.builder()
      .appName("MySparkApp") // Set your application name
      .config("spark.some.config.option", "config-value") // Optional: Add any Spark configurations
      .getOrCreate()

    // List of tables to compare
    val tables = List("employee", "department") // Add your table names here

    // Prepare an empty DataFrame to store results
    import spark.implicits._
    var resultsDF = Seq.empty[(String, Long, Long)].toDF("table_name", "env1_count", "env2_count")

    for (table <- tables) {
      // Count from env1
      val uat_Count = getCountFromMySQL(spark, uat_url, jdbcUsername, jdbcPassword, table)

      // Count from env2
      val sit_count = getCountFromMySQL(spark, sit_url, jdbcUsername, jdbcPassword, table)

      // Append to results DataFrame
      resultsDF = resultsDF.union(Seq((table, uat_Count, sit_count)).toDF("table_name", "env1_count", "env2_count"))
    }


    val outputTable = "count_reconciliation"
    resultsDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/olc_sql")
      .option("dbtable", outputTable)
      .option("user", "root")
      .option("password", "root")
      .mode("overwrite") // or "append" if you want to add to existing table
      .save()
  }

    def getCountFromMySQL(spark: SparkSession, url: String, user: String, password: String, table: String): Long = {
      val df: DataFrame = spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .load()

      df.count()
    }
  }

