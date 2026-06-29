package jobs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.sketch.CountMinSketch
import common.Schemas
import common.Paths

import java.nio.file.{Files, Paths => JPaths, StandardOpenOption}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

/**
 * AnalyticsJob
 *
 * Runs 6 core analytical queries comparing Exact query execution against two
 * approximate methods: Reservoir Sampling (Algorithm R) and Count-Min Sketch (CMS).
 * Calculates absolute and percentage error margins and execution times for all methods
 * and outputs the performance metrics JSON artifact to `/data/gold/metrics.json`.
 */
object AnalyticsJob {

  // ---------------------------------------------------------------------------
  // Timing helper — returns (result, elapsedMillis)
  // Forces materialization via cache().count() so the timing reflects actual
  // Spark execution, not just lazy plan construction.
  // ---------------------------------------------------------------------------
  private def timeIt[T](block: => T): (T, Long) = {
    val t0 = System.nanoTime()
    val result = block
    val elapsed = (System.nanoTime() - t0) / 1000000L
    (result, elapsed)
  }

  /** Cache a DataFrame and force materialization; return the cached DF. */
  private def materialize(df: DataFrame): DataFrame = {
    val cached = df.cache()
    cached.count()
    cached
  }

  // ---------------------------------------------------------------------------
  // Per-query metrics case class
  // ---------------------------------------------------------------------------
  case class QueryMetrics(
    queryName:        String,
    exactTimeMs:      Long,
    sampleTimeMs:     Long,
    cmsTimeMs:        Long,
    sampleAbsError:   Double,
    samplePctError:   Double,
    cmsAbsError:      Double,
    cmsPctError:      Double
  )

  // ---------------------------------------------------------------------------
  // Compute aggregate absolute and percentage error between estimate and exact.
  // Uses the representative total count summed across the top-N rows in the
  // comparison DataFrame.  Returns (absoluteError, percentageError).
  // ---------------------------------------------------------------------------
  private def computeErrors(
    comparisonDF: DataFrame,
    exactCol:     String,
    estimateCol:  String
  ): (Double, Double) = {
    val rows = comparisonDF
      .select(col(exactCol).cast("double"), col(estimateCol).cast("double"))
      .na.fill(0.0)
      .collect()

    var totalExact    = 0.0
    var totalEstimate = 0.0
    rows.foreach { r =>
      totalExact    += r.getDouble(0)
      totalEstimate += r.getDouble(1)
    }

    val absError = math.abs(totalExact - totalEstimate)
    val pctError = if (totalExact != 0.0) (absError / totalExact) * 100.0 else 0.0
    (absError, pctError)
  }

  // ---------------------------------------------------------------------------
  // Simple JSON serializer (no external library needed)
  // ---------------------------------------------------------------------------
  private def metricsToJson(metrics: Seq[QueryMetrics]): String = {
    val sb = new StringBuilder
    sb.append("[\n")
    metrics.zipWithIndex.foreach { case (m, idx) =>
      sb.append("  {\n")
      sb.append(s"""    "query_name": "${m.queryName}",\n""")
      sb.append(s"""    "exact_time_ms": ${m.exactTimeMs},\n""")
      sb.append(s"""    "sample_time_ms": ${m.sampleTimeMs},\n""")
      sb.append(s"""    "cms_time_ms": ${m.cmsTimeMs},\n""")
      sb.append(s"""    "sample_absolute_error": ${m.sampleAbsError},\n""")
      sb.append(s"""    "sample_pct_error": ${BigDecimal(m.samplePctError).setScale(4, BigDecimal.RoundingMode.HALF_UP)},\n""")
      sb.append(s"""    "cms_absolute_error": ${m.cmsAbsError},\n""")
      sb.append(s"""    "cms_pct_error": ${BigDecimal(m.cmsPctError).setScale(4, BigDecimal.RoundingMode.HALF_UP)}\n""")
      sb.append("  }")
      if (idx < metrics.size - 1) sb.append(",")
      sb.append("\n")
    }
    sb.append("]\n")
    sb.toString()
  }

  // ---------------------------------------------------------------------------
  // Composite-key delimiter — pipe character, safe for numeric IDs.
  // Avoids underscore collisions (Bug 2 from audit).
  // ---------------------------------------------------------------------------
  private val KeyDelim = "|"

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

    // Metrics accumulator
    val allMetrics = ListBuffer[QueryMetrics]()

    // =================================================================================
    // QUERY 1: Top 20 Busiest Pickup Locations
    // =================================================================================
    println("\nRunning Query 1: Top 20 Busiest Pickup Locations...")

    val (q1Exact, q1ExactTime) = timeIt {
      materialize(taxi.groupBy("PULocationID").agg(count("*").alias("exact_trip_count")))
    }

    val (q1Sample, q1SampleTime) = timeIt {
      materialize(sampleDF.groupBy("PULocationID").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")))
    }

    val (q1Cms, q1CmsTime) = timeIt {
      val cmsPu = buildCmsForKey(taxi, "PULocationID")
      val topPuKeys = q1Exact.orderBy(desc("exact_trip_count")).limit(20).collect().map(r => if (r.isNullAt(0)) "__NULL__" else r.getString(0))
      val cmsPuEst = topPuKeys.map(k => (k, cmsPu.estimateCount(k)))
      cmsPuEst.toSeq.toDF("PULocationID", "cms_estimated_count")
    }

    val q1Comparison = q1Exact
      .join(q1Sample, Seq("PULocationID"), "left")
      .join(q1Cms, Seq("PULocationID"), "left")
      .orderBy(desc("exact_trip_count"))
      .limit(20)

    q1Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query1")
    q1Comparison.show(false)

    val (q1SampleAbsErr, q1SamplePctErr) = computeErrors(q1Comparison, "exact_trip_count", "sampled_trip_count")
    val (q1CmsAbsErr, q1CmsPctErr)       = computeErrors(q1Comparison, "exact_trip_count", "cms_estimated_count")
    allMetrics += QueryMetrics("Q1_Top20_Busiest_Pickup_Locations", q1ExactTime, q1SampleTime, q1CmsTime, q1SampleAbsErr, q1SamplePctErr, q1CmsAbsErr, q1CmsPctErr)

    // =================================================================================
    // QUERY 2: Top 50 Weekday Morning Dropoffs (DOLocationID + hour)
    // FIX: CMS now built on filtered weekday-morning data (Flaw 3)
    // FIX: Delimiter changed from '_' to '|' (Bug 2)
    // =================================================================================
    println("\nRunning Query 2: Top 50 Weekday Morning Dropoffs...")

    // Pre-filter for weekday mornings — shared by all three methods
    val weekdayMorningTaxi = taxi
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime")))
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6))

    val weekdayMorningSample = sampleDF
      .withColumn("hour", hour(col("dropoff_datetime")))
      .withColumn("day_of_week", dayofweek(col("dropoff_datetime")))
      .filter(col("hour").between(6, 11))
      .filter(col("day_of_week").between(2, 6))

    val (q2Exact, q2ExactTime) = timeIt {
      materialize(weekdayMorningTaxi
        .groupBy("DOLocationID", "hour")
        .agg(count("*").alias("exact_dropoff_count")))
    }

    val (q2Sample, q2SampleTime) = timeIt {
      materialize(weekdayMorningSample
        .groupBy("DOLocationID", "hour")
        .agg(round(count("*") * scalingFactor).alias("sampled_dropoff_count")))
    }

    val (q2Cms, q2CmsTime) = timeIt {
      // CMS built on FILTERED weekday-morning data to match exact/sample domain
      val cmsDo = buildCmsForKey(weekdayMorningTaxi,
        s"concat_ws('$KeyDelim', DOLocationID, hour(dropoff_datetime))")
      val topDoHourKeys = q2Exact.orderBy(desc("exact_dropoff_count")).limit(50).collect().map(r => {
        val doId = if (r.isNullAt(0)) "__NULL__" else r.getString(0)
        val hr = if (r.isNullAt(1)) "__NULL__" else r.getInt(1).toString
        s"$doId$KeyDelim$hr"
      })
      val cmsDoEst = topDoHourKeys.map(k => {
        val parts = k.split(s"\\$KeyDelim", -1)
        val doId = parts(0)
        val hr = parts(1).toInt
        (doId, hr, cmsDo.estimateCount(k))
      })
      cmsDoEst.toSeq.toDF("DOLocationID", "hour", "cms_estimated_count")
    }

    val q2Comparison = q2Exact
      .join(q2Sample, Seq("DOLocationID", "hour"), "left")
      .join(q2Cms, Seq("DOLocationID", "hour"), "left")
      .orderBy(desc("exact_dropoff_count"))
      .limit(50)

    q2Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query2")
    q2Comparison.show(false)

    val (q2SampleAbsErr, q2SamplePctErr) = computeErrors(q2Comparison, "exact_dropoff_count", "sampled_dropoff_count")
    val (q2CmsAbsErr, q2CmsPctErr)       = computeErrors(q2Comparison, "exact_dropoff_count", "cms_estimated_count")
    allMetrics += QueryMetrics("Q2_Top50_Weekday_Morning_Dropoffs", q2ExactTime, q2SampleTime, q2CmsTime, q2SampleAbsErr, q2SamplePctErr, q2CmsAbsErr, q2CmsPctErr)

    // =================================================================================
    // QUERY 3: Busiest Taxi Routes (Pickup-Dropoff Pair)
    // FIX: Delimiter changed from '_' to '|' (Bug 2)
    // =================================================================================
    println("\nRunning Query 3: Busiest Taxi Routes...")

    val (q3Exact, q3ExactTime) = timeIt {
      materialize(taxi.groupBy("PULocationID", "DOLocationID").agg(count("*").alias("exact_trip_count")))
    }

    val (q3Sample, q3SampleTime) = timeIt {
      materialize(sampleDF.groupBy("PULocationID", "DOLocationID").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")))
    }

    val (q3Cms, q3CmsTime) = timeIt {
      val cmsRoutes = buildCmsForKey(taxi,
        s"concat_ws('$KeyDelim', PULocationID, DOLocationID)")
      val topRouteKeys = q3Exact.orderBy(desc("exact_trip_count")).limit(20).collect().map(r => {
        val pu = if (r.isNullAt(0)) "__NULL__" else r.getString(0)
        val doLoc = if (r.isNullAt(1)) "__NULL__" else r.getString(1)
        s"$pu$KeyDelim$doLoc"
      })
      val cmsRouteEst = topRouteKeys.map(k => {
        val parts = k.split(s"\\$KeyDelim", -1)
        (parts(0), parts(1), cmsRoutes.estimateCount(k))
      })
      cmsRouteEst.toSeq.toDF("PULocationID", "DOLocationID", "cms_estimated_count")
    }

    val q3Comparison = q3Exact
      .join(q3Sample, Seq("PULocationID", "DOLocationID"), "left")
      .join(q3Cms, Seq("PULocationID", "DOLocationID"), "left")
      .orderBy(desc("exact_trip_count"))
      .limit(20)

    q3Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query3")
    q3Comparison.show(false)

    val (q3SampleAbsErr, q3SamplePctErr) = computeErrors(q3Comparison, "exact_trip_count", "sampled_trip_count")
    val (q3CmsAbsErr, q3CmsPctErr)       = computeErrors(q3Comparison, "exact_trip_count", "cms_estimated_count")
    allMetrics += QueryMetrics("Q3_Busiest_Taxi_Routes", q3ExactTime, q3SampleTime, q3CmsTime, q3SampleAbsErr, q3SamplePctErr, q3CmsAbsErr, q3CmsPctErr)

    // =================================================================================
    // QUERY 4: Passenger Count Distribution
    // FIX: Use Spark cast(StringType) for key lookup to match CMS insertion (Flaw 4)
    // =================================================================================
    println("\nRunning Query 4: Passenger Count Distribution...")

    val (q4Exact, q4ExactTime) = timeIt {
      materialize(taxi.filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
        .groupBy("passenger_count").agg(count("*").alias("exact_trip_count")))
    }

    val (q4Sample, q4SampleTime) = timeIt {
      materialize(sampleDF.filter(col("passenger_count").isNotNull && col("passenger_count").between(1, 8))
        .groupBy("passenger_count").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")))
    }

    val (q4Cms, q4CmsTime) = timeIt {
      val cmsInputDF = taxi.withColumn("passenger_count_string", col("passenger_count").cast(StringType))
      val cmsPassenger = buildCmsForKey(cmsInputDF, "passenger_count_string")
      // FIX: Use Spark's cast(StringType) for key lookup to guarantee same
      // string representation as CMS insertion (e.g., "1.0" not "1.0E0")
      val passengerKeys = q4Exact
        .select(col("passenger_count"), col("passenger_count").cast(StringType).alias("key_str"))
        .collect()
      val cmsPassengerEst = passengerKeys.map(r => {
        val pcount = r.getDouble(0)
        val keyStr = r.getString(1)
        (pcount, cmsPassenger.estimateCount(keyStr))
      })
      cmsPassengerEst.toSeq.toDF("passenger_count", "cms_estimated_count")
    }

    val q4Comparison = q4Exact
      .join(q4Sample, Seq("passenger_count"), "left")
      .join(q4Cms, Seq("passenger_count"), "left")
      .orderBy("passenger_count")

    q4Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query4")
    q4Comparison.show(false)

    val (q4SampleAbsErr, q4SamplePctErr) = computeErrors(q4Comparison, "exact_trip_count", "sampled_trip_count")
    val (q4CmsAbsErr, q4CmsPctErr)       = computeErrors(q4Comparison, "exact_trip_count", "cms_estimated_count")
    allMetrics += QueryMetrics("Q4_Passenger_Count_Distribution", q4ExactTime, q4SampleTime, q4CmsTime, q4SampleAbsErr, q4SamplePctErr, q4CmsAbsErr, q4CmsPctErr)

    // =================================================================================
    // QUERY 5: Payment Type Analysis
    // FIX: CMS keyed on payment_type_desc directly instead of raw code (Minor 7)
    // =================================================================================
    println("\nRunning Query 5: Payment Type Analysis...")

    val (q5Exact, q5ExactTime) = timeIt {
      materialize(taxi.groupBy("payment_type_desc").agg(count("*").alias("exact_trip_count")))
    }

    val (q5Sample, q5SampleTime) = timeIt {
      materialize(sampleDF.groupBy("payment_type_desc").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")))
    }

    val (q5Cms, q5CmsTime) = timeIt {
      // FIX: Build CMS directly on payment_type_desc so the key domain
      // matches exact/sample without relying on a manual map bridge.
      val cmsPayment = buildCmsForKey(taxi, "coalesce(payment_type_desc, '__NULL__')")
      val paymentDescKeys = q5Exact.collect().map(r =>
        if (r.isNullAt(0)) "__NULL__" else r.getString(0)
      )
      val cmsPaymentEst = paymentDescKeys.map(k => (k, cmsPayment.estimateCount(k)))
      cmsPaymentEst.toSeq.toDF("payment_type_desc", "cms_estimated_count")
    }

    val q5Comparison = q5Exact
      .join(q5Sample, Seq("payment_type_desc"), "left")
      .join(q5Cms, Seq("payment_type_desc"), "left")
      .orderBy(desc("exact_trip_count"))

    q5Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query5")
    q5Comparison.show(false)

    val (q5SampleAbsErr, q5SamplePctErr) = computeErrors(q5Comparison, "exact_trip_count", "sampled_trip_count")
    val (q5CmsAbsErr, q5CmsPctErr)       = computeErrors(q5Comparison, "exact_trip_count", "cms_estimated_count")
    allMetrics += QueryMetrics("Q5_Payment_Type_Analysis", q5ExactTime, q5SampleTime, q5CmsTime, q5SampleAbsErr, q5SamplePctErr, q5CmsAbsErr, q5CmsPctErr)

    // =================================================================================
    // QUERY 6: Top 10 Most Frequent Tip Amounts
    // FIX: Use Spark cast(StringType) for key lookup to match CMS insertion (Flaw 5)
    // =================================================================================
    println("\nRunning Query 6: Top 10 Most Frequent Tip Amounts...")

    val (q6Exact, q6ExactTime) = timeIt {
      materialize(taxi.filter(col("tip_amount") > 0).groupBy("tip_amount").agg(count("*").alias("exact_trip_count")))
    }

    val (q6Sample, q6SampleTime) = timeIt {
      materialize(sampleDF.filter(col("tip_amount") > 0).groupBy("tip_amount").agg(round(count("*") * scalingFactor).alias("sampled_trip_count")))
    }

    val (q6Cms, q6CmsTime) = timeIt {
      // FIX: Use Spark's cast(StringType) for both CMS insertion and lookup
      // to guarantee identical string representation of doubles.
      val q6Top = q6Exact.orderBy(desc("exact_trip_count")).limit(10)
      val top10Tips = q6Top
        .select(col("tip_amount"), col("tip_amount").cast(StringType).alias("key_str"))
        .collect()
      val cmsTip = buildCmsForKey(taxi.filter(col("tip_amount") > 0), "cast(tip_amount as string)")
      val cmsTipEst = top10Tips.map(r => {
        val tipVal = r.getDouble(0)
        val keyStr = r.getString(1)
        (tipVal, cmsTip.estimateCount(keyStr))
      })
      cmsTipEst.toSeq.toDF("tip_amount", "cms_estimated_count")
    }

    val q6Comparison = q6Exact
      .join(q6Sample, Seq("tip_amount"), "left")
      .join(q6Cms, Seq("tip_amount"), "left")
      .orderBy(desc("exact_trip_count"))
      .limit(10)

    q6Comparison.write.mode("overwrite").parquet(s"${Paths.GoldPath}/query6")
    q6Comparison.show(false)

    val (q6SampleAbsErr, q6SamplePctErr) = computeErrors(q6Comparison, "exact_trip_count", "sampled_trip_count")
    val (q6CmsAbsErr, q6CmsPctErr)       = computeErrors(q6Comparison, "exact_trip_count", "cms_estimated_count")
    allMetrics += QueryMetrics("Q6_Top10_Frequent_Tip_Amounts", q6ExactTime, q6SampleTime, q6CmsTime, q6SampleAbsErr, q6SamplePctErr, q6CmsAbsErr, q6CmsPctErr)

    // =================================================================================
    // Write metrics.json artifact
    // =================================================================================
    val metricsJson = metricsToJson(allMetrics.toSeq)
    val metricsPath = JPaths.get(s"${Paths.GoldPath}/metrics.json")

    // Ensure parent directory exists
    val parentDir = metricsPath.getParent
    if (parentDir != null && !Files.exists(parentDir)) {
      Files.createDirectories(parentDir)
    }

    Files.write(
      metricsPath,
      metricsJson.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )
    println(s"\nMetrics JSON written to: $metricsPath")
    println(metricsJson)

    println("AnalyticsJob completed successfully.")
    spark.stop()
  }
}
