package jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import common.Schemas
import common.Paths

/**
 * CleanJob
 *
 * Cleans the raw Bronze data by parsing timestamps, filtering passenger and location
 * criteria, restricting dates to 2020-2023, and enriching with payment types.
 * Outputs the Silver partitioned Parquet layer (by year/month) and materializes
 * a true reservoir sample (Algorithm R equivalent) for approximate queries.
 */
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
      .filter(col("pickup_datetime").between("2020-01-01", "2023-12-31"))
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

    // Generate and write Reservoir Sample (exact-k uniform sample)
    // Uses orderBy(rand) + limit to guarantee exactly k rows, each with
    // equal selection probability — equivalent to Algorithm R output.
    val targetSampleSize = sys.env.get("TARGET_SAMPLE_SIZE").map(_.toLong).getOrElse(100000L)
    val clampedSampleSize = math.min(targetSampleSize, totalRows).toInt
    println(s"Target sample size: $targetSampleSize (clamped to $clampedSampleSize)")

    val sampleDF = silverDF
      .orderBy(rand(1234L))
      .limit(clampedSampleSize)
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
