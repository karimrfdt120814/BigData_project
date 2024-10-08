import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{avg, col, lit, sum, when}


object first {

  def main(args:Array[String]):Unit= {
    //println("Hello Word")

    val sc = new SparkContext("local[*]","demo")

//val jdbcDf = spark.read.format("jdbc").jdbc("")
    // Create a Spark Session
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

    df.show(false)

    val file = spark.read.option("header","true").csv("C:\\Users\\Asus\\Desktop\\UK_Txns.csv")
    file.printSchema()

    file.show(30,false)

    file.rdd.getNumPartitions;

    file.write.saveAsTable("uk_data");

    //file.write.format("json").json("C:\\Users\\Asus\\Desktop\\JsonOut1")
    val rdd = sc.parallelize(1 to 100)

    var a= 10;

   a=20;
   // println("*****************printing a value: " +a)



   // println("printing ********** :" +rdd.collect.foreach(println))
   // rdd.reduce(_+_)

    //val df = spark.sql("select * from orders")
    //df.printSchema()

  }
}

