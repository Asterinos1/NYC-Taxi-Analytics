package jobs

import org.apache.spark.sql.SparkSession
import common.Schemas
import common.Paths

object IngestJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Ingest Job - Bronze")
      .getOrCreate()

    println(s"Loading raw CSV data from: ${Paths.RawDataPath}")
    val df = spark.read
      .option("header", "true")
      .schema(Schemas.taxiSchema)
      .csv(Paths.RawDataPath)

    println(s"Writing raw data as Parquet to: ${Paths.BronzePath}")
    df.write
      .mode("overwrite")
      .parquet(Paths.BronzePath)

    println("IngestJob completed successfully.")
    spark.stop()
  }
}
