import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{avg, col, lit, sum, when}

object reading_mysql_tables {


  def main(args:Array[String]):Unit={

    val sc = new SparkContext("local[*]","mysql_jdbc")

    val spark = SparkSession.builder()
      .appName("MySparkApp") // Set your application name
      .config("spark.some.config.option", "config-value") // Optional: Add any Spark configurations
      .getOrCreate()

    // MySQL database connection properties
    val jdbcHostname = "localhost" // MySQL host
    val jdbcPort = 3306 // MySQL port (default is 3306)
    val jdbcDatabase = "karim" // Your MySQL database name
    val jdbcUsername = "root" // Your MySQL username
    val jdbcPassword = "root" // Your MySQL password

    val jdbcUrl = s"jdbc:mysql://$jdbcHostname:$jdbcPort/$jdbcDatabase"

    val tableName = "employees"
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .option("driver", "com.mysql.cj.jdbc.Driver") // MySQL JDBC driver
      .load()

    df.dropDuplicates("emp_id").orderBy("emp_id").show(false)

  }




}
