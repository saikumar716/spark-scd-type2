package com.sai.data_prep

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, DecimalType, DoubleType, IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.functions._

object yelp_user {
  def main(args: Array[String]): Unit = {
    println("SCD data preperation started")
    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("Dataframe Example")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._
    println("\nCreating dataframe from CSV file using 'SparkSession.read.format()',")
    val finSchema = new StructType()
      .add("user_id", StringType,true)
      .add("name", StringType,true)
      .add("review_count", IntegerType,true)
      .add("yelping_since", DateType,true)
      .add("friends", StringType,true)
      .add("useful", IntegerType,true)
      .add("funny", IntegerType,true)
      .add("cool", IntegerType,true)
      .add("fans", IntegerType,true)
      .add("elite", StringType,true)
      .add("average_stars", DoubleType,true)
      .add("compliment_hot", IntegerType,true)
      .add("compliment_more", IntegerType,true)
      .add("compliment_profile", IntegerType,true)
      .add("compliment_cute", IntegerType,true)
      .add("compliment_list", IntegerType,true)
      .add("compliment_note", IntegerType,true)
      .add("compliment_plain", IntegerType,true)
      .add("compliment_cool", IntegerType,true)
      .add("compliment_funny", IntegerType,true)
      .add("compliment_writer", IntegerType,true)
      .add("compliment_photos", IntegerType,true)
    val  Filepath =  "file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_user.csv"

    println("Read the original  yelp_user file to dataframe")
    val yelp_user_orig = sparkSession.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("ignoreLeadingWhiteSpace", true)
      .option("timestampFormat", "dd-MM-yyyy")
      .format("csv")
      .schema(finSchema)
      .load(Filepath)

    yelp_user_orig.printSchema()
    yelp_user_orig.show(3)

    println("Create file for month jul 20180731")
 val yelp_user_20180731 = yelp_user_orig
                         .withColumn("Date", lit("2018-07-31"))
    val  Filepath2 =  "file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_user_20180731.csv"
    yelp_user_20180731
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .save(Filepath2)
    println("written done ")

    println("Create file for month aug 20180831")
    val yelp_user_20180831 = yelp_user_orig
      .withColumn("Date", lit("2018-08-31"))
      .withColumn("average_stars",lit(4.5))
      .withColumn("fans",lit(10))
      .withColumn("elite",lit("2015,2016,2017"))
    val  Filepath3 =  "file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_user_20180831.csv"
    yelp_user_20180831
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .save(Filepath3)
    println("written done")

    println("Create file for month sep 20180930")
    val yelp_user_20180930 = yelp_user_orig
      .withColumn("Date", lit("2018-09-30"))
      .withColumn("average_stars",lit(3.5))
      .withColumn("fans",lit(20))
      .withColumn("elite",lit("2015,2016,2018"))
      .withColumn("useful",lit(3000))

    val  Filepath4 =  "file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_user_20180930.csv"
    yelp_user_20180930
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .save(Filepath4)
    println("written done")

    println("Create file for month nov 20181031")
    val yelp_user_20181031 = yelp_user_orig
      .withColumn("Date", lit("2018-10-31"))
      .withColumn("average_stars",lit(4))
      .withColumn("fans",lit(40))
      .withColumn("elite",lit("2015,2017,2018"))


    val  Filepath5 =  "file:///D://BigData_DSM//WorkSpace//spark-scd-type2//src//main//resources//yelp_data//yelp_user_20181031.csv"
    yelp_user_20181031
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .save(Filepath5)
  println("written done")
  }
}
