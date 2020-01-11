package com.sai.exec_programs

import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object YelpUserScdFirstRun {
  def main(args: Array[String]): Unit = {

    println("SCD First run started")

    val proc_dt = args(0)

    val inputFormat = new SimpleDateFormat("yyyyMMdd")
    val reqFormat = new SimpleDateFormat("yyyy-MM-dd")

    // Convert the effective and expiry date in yyyy-MM-dd format
    val eff_dt = reqFormat.format(inputFormat.parse(proc_dt))
    val exp_dt = reqFormat.format(reqFormat.parse("2099-12-31"))

    val histPath = new Path("file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_hist/")

    val histTabPath = "file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_hist/"

    val input_file = s"file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_user_$proc_dt.csv"

    val conf = new SparkConf().setAppName("Yelp-user-scd-2-first-run").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    sc.setLogLevel("ERROR")

    sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    sqlContext.setConf("spark.sql.orc.enabled","true")

    // Check if the HIST table directory present for first run, else create directory
    if (!fs.exists(histPath)) {
      println("History table directory does not exist, creating directory")
      fs.mkdirs(histPath)

    } else {
            println("History table directory exist")
            }

    // Read Yelp User data from the file and set effective and expiry date
    val yelp_data = sqlContext.read.format("com.databricks.spark.csv").
                                    option("header", "true").
                                    option("inferSchema", "true").
                                    option("quote", "\"").
                                    option("ignoreLeadingWhiteSpace", true).
                                    load(input_file)

    val yelp_data_hist = yelp_data.withColumn("eff_dt",to_date(lit(eff_dt)))
                                  .withColumn("exp_dt",to_date(lit(exp_dt)))

    yelp_data_hist.show(4)
    // Save updated Yelp user data to HIST table
    yelp_data_hist
      .repartition(2)
      .write
      .option("header","true")
      .mode("overwrite")
      .parquet(histTabPath)
    println("written done ")

    println("First run completed successfully")
  }

}
