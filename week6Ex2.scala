package org.apache.spark.examples.sql
import org.apache.spark.sql.SaveMode

import org.apache.spark.sql.SparkSession

case class Record(key: Int, value: String)

object RDDRelation {

  val insuranceFilePath = "C:\\Temp\\insurance.csv"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Spark Examples")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Hide unnecessary log messages
    spark.sparkContext.setLogLevel("ERROR")

    // (1) Read insurance.csv file
    import spark.implicits._
    val df = spark.read.format("csv").option("header", "true").load(insuranceFilePath)
    df.createOrReplaceTempView("insurance")

    // (2) Print the size (count(*))
    val count = spark.sql("SELECT COUNT(*) FROM insurance").collect().head.getLong(0)
    println(s"Size via COUNT(*): $count")


    // (3) Print sex and count of sex (use group by in sql)
    val rddFromSql1 = spark.sql("SELECT sex, count(*) FROM insurance GROUP BY sex")
    println("===== Sex and count of sex =====")
    rddFromSql1.rdd.map(row => s"Sex: ${row(0)}, Count: ${row(1)}").collect().foreach(println)

    // (4) Filter smoker=yes and print again the sex,count of sex
    val rddFromSql2 = spark.sql("SELECT sex, count(*) FROM insurance WHERE smoker='yes' GROUP BY sex")
    println("===== Sex and count of sex (smoker=yes) =====")
    rddFromSql2.rdd.map(row => s"Sex: ${row(0)}, Count: ${row(1)}").collect().foreach(println)

    // (5) Group by region and sum the charges, then print rows by DESC
    val rddFromSql3 = spark.sql("SELECT region, SUM(charges) FROM insurance GROUP BY region ORDER BY SUM(charges) DESC")
    println("===== Region and sum of charges =====")
    rddFromSql3.rdd.map(row => s"Region: ${row(0)}, SUM(charges): ${row(1)}").collect().foreach(println)

    spark.stop()
  }
}