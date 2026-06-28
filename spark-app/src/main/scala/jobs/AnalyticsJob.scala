package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.sketch.CountMinSketch
import common.Schemas
import common.Paths

object AnalyticsJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Analytics Job - Gold")
      .getOrCreate()

    import spark.implicits._

    println(s"Loading Silver Parquet data from: ${Paths.SilverPath}")
    val taxi = spark.read.parquet(Paths.SilverPath).cache()

    println(s"Loading Silver Reservoir Sample from: ${Paths.SilverSamplePath}")
    val sampleDF = spark.read.parquet(Paths.SilverSamplePath).cache()

    val totalRows = taxi.count()
    val sampleSize = sampleDF.count()
    println(s"Total rows in Silver: $totalRows")
    println(s"Reservoir Sample size: $sampleSize")

    val scalingFactor = if (sampleSize > 0) totalRows.toDouble / sampleSize.toDouble else 1.0

    // Load CMS parameters from env or defaults
    val eps = sys.env.get("CMS_EPSILON").map(_.toDouble).getOrElse(0.0001)
    val confidence = sys.env.get("CMS_CONFIDENCE").map(_.toDouble).getOrElse(0.95)
    val cmsSeed = sys.env.get("CMS_SEED").map(_.toInt).getOrElse(1)

    // Helper: Build CMS
    def buildCmsForKey(df: DataFrame, keyExpr: String): CountMinSketch = {
      df.select(expr(keyExpr).alias("_cms_key")).rdd.mapPartitions { iter =>
        val localCms = CountMinSketch.create(eps, confidence, cmsSeed)
        iter.foreach { row =>
          val v = if (row.isNullAt(0)) "__NULL__" else row.get(0).toString
          localCms.add(v)
        }
        Iterator(localCms)
      }.reduce((c1, c2) => { c1.mergeInPlace(c2); c1 })
    }

    // =================================================================================
    // QUERY 1: Top 20 Busiest Pickup Locations
    // =================================================================================
    println("\nRunning Query 1: Top 20 Busiest Pickup Locations...")
    val q1Exact = taxi.groupBy("PULocationID").agg(count("*").alias("exact_trip_count"))
    val q1Sample = sampleDF.groupBy("PULocationID").agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))

    val cmsPu = buildCmsForKey(taxi, "PULocationID")
    val topPuKeys = q1Exact.orderBy(desc("exact_trip_count")).limit(20).collect().map(r => if (r.isNullAt(0)) "__NULL__" else r.getString(0))
    val cmsPuEst = topPuKeys.map(k => (k, cmsPu.estimateCount(k)))
    val q1Cms = cmsPuEst.toSeq.toDF("PULocationID", "cms_estimated_count")

    val q1Comparison = q1Exact
      .join(q1Sample, Seq("PULocationID"), "left")
      .join(q1Cms, Seq("PULocationID"), "left")
      .orderBy(desc("exact_trip_count"))
      .limit(20)

    q1Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query1")
    q1Comparison.show(false)

    // =================================================================================
    // QUERY 2: Top 50 Weekday Morning Dropoffs (DOLocationID + hour)
    // =================================================================================
    println("\nRunning Query 2: Top 50 Weekday Morning Dropoffs...")
    val q2Exact = taxi
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime")))
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6))
      .groupBy("DOLocationID", "hour")
      .agg(count("*").alias("exact_dropoff_count"))

    val q2Sample = sampleDF
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime")))
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6))
      .groupBy("DOLocationID", "hour")
      .agg(round(count("*") * scalingFactor).alias("sampled_dropoff_count"))

    val cmsDo = buildCmsForKey(taxi, "concat_ws('_', DOLocationID, hour(dropoff_datetime))")
    val topDoHourKeys = q2Exact.orderBy(desc("exact_dropoff_count")).limit(50).collect().map(r => {
      val doId = if (r.isNullAt(0)) "__NULL__" else r.getString(0)
      val hr = if (r.isNullAt(1)) "__NULL__" else r.getInt(1).toString
      s"${doId}_$hr"
    })
    val cmsDoEst = topDoHourKeys.map(k => {
      val parts = k.split("_")
      val doId = parts(0)
      val hr = parts(1).toInt
      (doId, hr, cmsDo.estimateCount(k))
    })
    val q2Cms = cmsDoEst.toSeq.toDF("DOLocationID", "hour", "cms_estimated_count")

    val q2Comparison = q2Exact
      .join(q2Sample, Seq("DOLocationID", "hour"), "left")
      .join(q2Cms, Seq("DOLocationID", "hour"), "left")
      .orderBy(desc("exact_dropoff_count"))
      .limit(50)

    q2Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query2")
    q2Comparison.show(false)

    // =================================================================================
    // QUERY 3: Busiest Taxi Routes (Pickup-Dropoff Pair)
    // =================================================================================
    println("\nRunning Query 3: Busiest Taxi Routes...")
    val q3Exact = taxi.groupBy("PULocationID", "DOLocationID").agg(count("*").alias("exact_trip_count"))
    val q3Sample = sampleDF.groupBy("PULocationID", "DOLocationID").agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))

    val cmsRoutes = buildCmsForKey(taxi, "concat_ws('_', PULocationID, DOLocationID)")
    val topRouteKeys = q3Exact.orderBy(desc("exact_trip_count")).limit(20).collect().map(r => {
      val pu = if (r.isNullAt(0)) "__NULL__" else r.getString(0)
      val doLoc = if (r.isNullAt(1)) "__NULL__" else r.getString(1)
      s"${pu}_$doLoc"
    })
    val cmsRouteEst = topRouteKeys.map(k => {
      val parts = k.split("_")
      (parts(0), parts(1), cmsRoutes.estimateCount(k))
    })
    val q3Cms = cmsRouteEst.toSeq.toDF("PULocationID", "DOLocationID", "cms_estimated_count")

    val q3Comparison = q3Exact
      .join(q3Sample, Seq("PULocationID", "DOLocationID"), "left")
      .join(q3Cms, Seq("PULocationID", "DOLocationID"), "left")
      .orderBy(desc("exact_trip_count"))
      .limit(20)

    q3Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query3")
    q3Comparison.show(false)

    // =================================================================================
    // QUERY 4: Passenger Count Distribution
    // =================================================================================
    println("\nRunning Query 4: Passenger Count Distribution...")
    val q4Exact = taxi.filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
      .groupBy("passenger_count").agg(count("*").alias("exact_trip_count"))

    val q4Sample = sampleDF.filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
      .groupBy("passenger_count").agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))

    val cmsInputDF = taxi.withColumn("passenger_count_string", col("passenger_count").cast(StringType))
    val cmsPassenger = buildCmsForKey(cmsInputDF, "passenger_count_string")
    val passengerKeys = q4Exact.collect().map(_.getDouble(0).toString)
    val cmsPassengerEst = passengerKeys.map(k => (k.toDouble, cmsPassenger.estimateCount(k)))
    val q4Cms = cmsPassengerEst.toSeq.toDF("passenger_count", "cms_estimated_count")

    val q4Comparison = q4Exact
      .join(q4Sample, Seq("passenger_count"), "left")
      .join(q4Cms, Seq("passenger_count"), "left")
      .orderBy("passenger_count")

    q4Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query4")
    q4Comparison.show(false)

    // =================================================================================
    // QUERY 5: Payment Type Analysis
    // =================================================================================
    println("\nRunning Query 5: Payment Type Analysis...")
    val q5Exact = taxi.groupBy("payment_type_desc").agg(count("*").alias("exact_trip_count"))
    val q5Sample = sampleDF.groupBy("payment_type_desc").agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))

    val cmsPayment = buildCmsForKey(taxi, "coalesce(payment_type, '__NULL__')")
    val paymentKeys = Schemas.paymentTypeMap.keys.toSeq
    val cmsPaymentEst = paymentKeys.map(k => {
      val descVal = Schemas.paymentTypeMap.getOrElse(k, "Other")
      (descVal, cmsPayment.estimateCount(k))
    })
    val q5Cms = cmsPaymentEst.toSeq.toDF("payment_type_desc", "cms_estimated_count")

    val q5Comparison = q5Exact
      .join(q5Sample, Seq("payment_type_desc"), "left")
      .join(q5Cms, Seq("payment_type_desc"), "left")
      .orderBy(desc("exact_trip_count"))

    q5Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query5")
    q5Comparison.show(false)

    // =================================================================================
    // QUERY 6: Top 10 Most Frequent Tip Amounts
    // =================================================================================
    println("\nRunning Query 6: Top 10 Most Frequent Tip Amounts...")
    val q6Exact = taxi.filter(col("tip_amount") > 0).groupBy("tip_amount").agg(count("*").alias("exact_trip_count"))
    val q6Sample = sampleDF.filter(col("tip_amount") > 0).groupBy("tip_amount").agg(round(count("*") * scalingFactor).alias("sampled_trip_count"))

    val top10Tips = q6Exact.orderBy(desc("exact_trip_count")).limit(10).collect().map(_.getDouble(0).toString)
    val cmsTip = buildCmsForKey(taxi.filter(col("tip_amount") > 0), "cast(tip_amount as string)")
    val cmsTipEst = top10Tips.map(k => (k.toDouble, cmsTip.estimateCount(k)))
    val q6Cms = cmsTipEst.toSeq.toDF("tip_amount", "cms_estimated_count")

    val q6Comparison = q6Exact
      .join(q6Sample, Seq("tip_amount"), "left")
      .join(q6Cms, Seq("tip_amount"), "left")
      .orderBy(desc("exact_trip_count"))
      .limit(10)

    q6Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query6")
    q6Comparison.show(false)

    println("AnalyticsJob completed successfully.")
    spark.stop()
  }
}
