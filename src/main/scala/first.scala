import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object first {

  def main(args:Array[String]):Unit= {
    //println("Hello Word")

    val sc = new SparkContext("local[*]","demo")


    // Create a Spark Session
    val spark = SparkSession.builder()
      .appName("MySparkApp") // Set your application name
      .config("spark.some.config.option", "config-value") // Optional: Add any Spark configurations
      .getOrCreate()

    val rdd = sc.parallelize(1 to 100)

    println("printing ********** :" +rdd.collect.foreach(println))
    rdd.reduce(_+_)

    //val df = spark.sql("select * from orders")
    //df.printSchema()

  }
}

