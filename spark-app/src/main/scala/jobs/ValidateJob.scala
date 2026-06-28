package jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.Paths

object ValidateJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Validate Job - Quality Gate")
      .getOrCreate()

    println(s"Loading Silver Parquet data for validation from: ${Paths.SilverPath}")
    val df = spark.read.parquet(Paths.SilverPath)

    // Assertion 1: Row count check
    val rowCount = df.count()
    println(s"Validation check: Row count = $rowCount")
    if (rowCount == 0) {
      throw new RuntimeException("Validation Failed: Silver dataset has 0 rows.")
    }

    // Assertion 2: Null checks on critical columns
    val nullPickupCount = df.filter(col("pickup_datetime").isNull).count()
    println(s"Validation check: Null pickup datetimes = $nullPickupCount")
    if (nullPickupCount > 0) {
      throw new RuntimeException(s"Validation Failed: Found $nullPickupCount null values in pickup_datetime.")
    }

    val nullPuLocCount = df.filter(col("PULocationID").isNull).count()
    println(s"Validation check: Null PULocationID = $nullPuLocCount")
    if (nullPuLocCount > 0) {
      throw new RuntimeException(s"Validation Failed: Found $nullPuLocCount null values in PULocationID.")
    }

    // Assertion 3: Business rules checks
    val invalidPassengerCount = df.filter(col("passenger_count") <= 0).count()
    println(s"Validation check: Passenger count <= 0 count = $invalidPassengerCount")
    if (invalidPassengerCount > 0) {
      throw new RuntimeException(s"Validation Failed: Found $invalidPassengerCount records with passenger_count <= 0.")
    }

    println("All quality gate validations passed successfully.")

    // Write a success sentinel file to confirm validation execution
    import java.io.PrintWriter
    import java.io.File
    val writer = new PrintWriter(new File("/data/validation_passed.txt"))
    writer.write(s"Validation passed at ${java.time.Instant.now().toString}\n")
    writer.write(s"Row count: $rowCount\n")
    writer.close()

    spark.stop()
  }
}
