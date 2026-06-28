package common

import org.apache.spark.sql.types._

object Schemas {
  val taxiSchema = StructType(Array(
    StructField("VendorID", StringType, nullable = true),
    StructField("tpep_pickup_datetime", StringType, nullable = true),
    StructField("tpep_dropoff_datetime", StringType, nullable = true),
    StructField("passenger_count", DoubleType, nullable = true),
    StructField("trip_distance", DoubleType, nullable = true),
    StructField("RatecodeID", StringType, nullable = true),
    StructField("store_and_fwd_flag", StringType, nullable = true),
    StructField("PULocationID", StringType, nullable = true),
    StructField("DOLocationID", StringType, nullable = true),
    StructField("payment_type", StringType, nullable = true),
    StructField("fare_amount", DoubleType, nullable = true),
    StructField("extra", DoubleType, nullable = true),
    StructField("mta_tax", DoubleType, nullable = true),
    StructField("tip_amount", DoubleType, nullable = true),
    StructField("tolls_amount", DoubleType, nullable = true),
    StructField("improvement_surcharge", DoubleType, nullable = true),
    StructField("total_amount", DoubleType, nullable = true),
    StructField("congestion_surcharge", DoubleType, nullable = true),
    StructField("airport_fee", DoubleType, nullable = true)
  ))

  val paymentTypeMap = Map(
    "0.0" -> "Flex Fare trip",
    "1.0" -> "Credit card",
    "2.0" -> "Cash",
    "3.0" -> "No charge",
    "4.0" -> "Dispute",
    "5.0" -> "Unknown",
    "6.0" -> "Voided trip"
  )
}
