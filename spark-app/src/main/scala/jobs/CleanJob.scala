package jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.Schemas
import common.Paths

object CleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Clean Job - Silver")
      .getOrCreate()

    println(s"Loading Bronze Parquet data from: ${Paths.BronzePath}")
    val bronzeDF = spark.read.parquet(Paths.BronzePath)

    // Broadcast map for payment type enrichment
    val bPaymentMap = spark.sparkContext.broadcast(Schemas.paymentTypeMap)
    val toPaymentTypeUDF = udf((id: String) => {
      if (id == null) "Other"
      else bPaymentMap.value.getOrElse(id.trim, "Other")
    })

    // Clean dates, filter passenger/location counts and date ranges, add enriched columns
    val cleanedDF = bronzeDF
      .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
      .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
      .filter(col("passenger_count") > 0)
      .filter(col("PULocationID").isNotNull && col("DOLocationID").isNotNull)
      .filter(col("pickup_datetime").between("2021-01-01", "2023-12-31"))
      .withColumn("payment_type_desc", toPaymentTypeUDF(col("payment_type")))

    val totalRows = cleanedDF.count()
    println(s"Cleaned data row count: $totalRows")

    // Extract year/month for partitioning
    val silverDF = cleanedDF
      .withColumn("year", year(col("pickup_datetime")))
      .withColumn("month", month(col("pickup_datetime")))

    println(s"Writing partitioned Silver Parquet data to: ${Paths.SilverPath}")
    silverDF.write
      .mode("overwrite")
      .partitionBy("year", "month")
      .parquet(Paths.SilverPath)

    // Generate and write Reservoir Sample
    val targetSampleSize = sys.env.get("TARGET_SAMPLE_SIZE").map(_.toLong).getOrElse(100000L)
    println(s"Target sample size: $targetSampleSize")

    val sampleFraction = math.min(1.0, targetSampleSize.toDouble / math.max(1L, totalRows).toDouble)
    val sampleDF = cleanedDF.sample(withReplacement = false, fraction = sampleFraction, seed = 1234L)
    val actualSampleSize = sampleDF.count()
    println(s"Actual reservoir sample size: $actualSampleSize")

    println(s"Writing Silver Reservoir Sample to: ${Paths.SilverSamplePath}")
    sampleDF.write
      .mode("overwrite")
      .parquet(Paths.SilverSamplePath)

    println("CleanJob completed successfully.")
    spark.stop()
  }
}
