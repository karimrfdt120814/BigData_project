import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

object ComplexJsonProcessing {

  def main(args: Array[String]):Unit ={


    val sc = new SparkContext("local[*]", "mysql_jdbc")

    val spark = SparkSession.builder()
      .appName("MySparkApp") // Set your application name
      .config("spark.some.config.option", "config-value") // Optional: Add any Spark configurations
      .getOrCreate()

    val input_df = spark.read.option("header","true").csv("C:\\Users\\Asus\\Desktop\\pokemonDB_dataset.csv")
      .withColumn("table_update_time",current_timestamp())

   // input_df.show(1216,false)

    val outputTable = "pokeman_table"
    input_df.write
      .format("jdbc").mode("overwrite")
      .option("url", "jdbc:mysql://localhost:3306/olc_sql")
      .option("dbtable", outputTable)
      .option("user", "root")
      .option("password", "root")
      .mode("overwrite") // or "append" if you want to add to existing table
      .save()









  }

}
