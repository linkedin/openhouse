// ============================================================================
// OpenHouse Catalog Population Script
// ============================================================================
//
// PURPOSE:
// This script populates the OpenHouse catalog with 5 test tables across 3 databases:
//   - 2 unpartitioned tables with 3 columns each
//   - 3 partitioned tables with 3 data columns + 1 timestamp column (hourly partitioning)
//
// DATABASES POPULATED:
//   - openhouse.testdb (primary test database)
//   - openhouse.devdb (development database)
//   - openhouse.stagingdb (staging database)
//
// Each table goes through multiple operations to create a rich snapshot history:
//   - DROP TABLE IF EXISTS (ensures clean state)
//   - 4 insert commits (separate batches)
//   - 1 delete operation
//   - 1 additional insert commit after the delete
//   This creates at least 6 snapshots per table for testing time travel and metadata queries
//
// HOW TO RUN:
// 1. Start the docker environment:
//    cd infra/recipes/docker-compose/oh-hadoop-spark
//    docker-compose -f docker-compose.spectacle.yml up -d
//
// 2. Run this script from spark-master container:
//    docker exec -it local.spark-master spark-shell \
//      --conf spark.sql.catalog.openhouse=org.apache.iceberg.spark.SparkCatalog \
//      --conf spark.sql.catalog.openhouse.type=rest \
//      --conf spark.sql.catalog.openhouse.uri=http://local.openhouse-tables:8000 \
//      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
//      -i /opt/spark-apps/populate-catalog.scala
//
// TABLES CREATED (in each database):
// ┌─────────────────────┬──────────────────────────────────────┬─────────────────┐
// │ Table Name          │ Columns                              │ Partitioning    │
// ├─────────────────────┼──────────────────────────────────────┼─────────────────┤
// │ user_profiles       │ user_id, username, email             │ None            │
// │ product_catalog     │ product_id, product_name, price      │ None            │
// │ clickstream_events  │ event_id, user_id, page_url,         │ hours(event_    │
// │                     │ event_time                           │ time)           │
// │ sensor_readings     │ sensor_id, temperature, humidity,    │ hours(reading_  │
// │                     │ reading_time                         │ time)           │
// │ transaction_logs    │ transaction_id, amount, status,      │ hours(transac-  │
// │                     │ transaction_time                     │ tion_time)      │
// └─────────────────────┴──────────────────────────────────────┴─────────────────┘
//
// VERIFY RESULTS:
// After running, verify in spark-shell:
//   spark.sql("SHOW DATABASES").show()
//   spark.sql("SHOW TABLES IN openhouse.testdb").show()
//   spark.sql("SELECT * FROM openhouse.testdb.user_profiles LIMIT 5").show()
//   spark.sql("SELECT * FROM openhouse.testdb.user_profiles.snapshots").show()
//
// Or via OpenHouse API:
//   curl http://localhost:8000/v1/databases
//   curl http://localhost:8000/v1/databases/testdb/tables
//
// ============================================================================

import org.apache.spark.sql.SparkSession
import scala.util.Random
import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit

println("=" * 80)
println("OpenHouse Catalog Population Script")
println("=" * 80)

// ============================================================================
// Helper Functions
// ============================================================================

// Generates a random alphanumeric string of specified length
// Used for creating realistic test data like usernames, product names, etc.
def randomString(length: Int): String = {
  val chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  (1 to length).map(_ => chars(Random.nextInt(chars.length))).mkString
}

// Generates a random timestamp within the past N hours
// Used to create realistic time-series data for partitioned tables
// The hoursBack parameter determines the time range (e.g., 24 = last 24 hours)
def randomTimestamp(hoursBack: Int): Timestamp = {
  val now = Instant.now()
  val randomHoursAgo = Random.nextInt(hoursBack)
  new Timestamp(now.minus(randomHoursAgo, ChronoUnit.HOURS).toEpochMilli)
}

// ============================================================================
// Main Function: Populate Database
// ============================================================================
// This function creates and populates all 5 tables in the specified database
// It drops existing tables first to ensure a clean state
// ============================================================================

def populateDatabase(dbName: String): Unit = {
  println("\n" + "=" * 80)
  println(s"Processing Database: openhouse.${dbName}")
  println("=" * 80)

  // Create database if it doesn't exist
  // OpenHouse doesn't support "IF NOT EXISTS" check, so we try to create and ignore errors
  println(s"\nEnsuring database openhouse.${dbName} exists...")
  try {
    spark.sql(s"CREATE DATABASE openhouse.${dbName}")
    println(s"✓ Created database openhouse.${dbName}")
  } catch {
    case e: Exception if e.getMessage.contains("already exists") || e.getMessage.contains("AlreadyExistsException") =>
      println(s"✓ Database openhouse.${dbName} already exists")
    case e: Exception =>
      println(s"! Skipping database creation (OpenHouse may auto-create on first table): ${e.getMessage}")
  }

  // ============================================================================
  // PART 1: UNPARTITIONED TABLES
  // ============================================================================

  println("\n" + "-" * 80)
  println("PART 1: Creating 2 Unpartitioned Tables")
  println("-" * 80)

  // ==========================================================================
  // UNPARTITIONED TABLE 1: user_profiles
  // ==========================================================================
  // Schema: user_id (STRING), username (STRING), email (STRING)
  // ==========================================================================

  println(s"\n--- Table: openhouse.${dbName}.user_profiles ---")

  // Drop table if it exists to ensure clean state
  println("Dropping table if exists...")
  spark.sql(s"DROP TABLE IF EXISTS openhouse.${dbName}.user_profiles")

  println("Creating table...")
  spark.sql(s"""
    CREATE TABLE openhouse.${dbName}.user_profiles (
      user_id STRING,
      username STRING,
      email STRING
    ) USING iceberg
  """).show()

  // INSERT #1: Initial batch (25 users)
  println("Insert #1: Adding 25 users...")
  for (i <- 1 to 25) {
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.user_profiles
      VALUES ('user_${i}', 'username_${randomString(8)}', 'user${i}@example.com')
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.user_profiles").collect()(0)(0)}")

  // INSERT #2: Second batch (25 users)
  println("Insert #2: Adding 25 users...")
  for (i <- 26 to 50) {
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.user_profiles
      VALUES ('user_${i}', 'username_${randomString(8)}', 'user${i}@example.com')
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.user_profiles").collect()(0)(0)}")

  // INSERT #3: Third batch (25 users)
  println("Insert #3: Adding 25 users...")
  for (i <- 51 to 75) {
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.user_profiles
      VALUES ('user_${i}', 'username_${randomString(8)}', 'user${i}@example.com')
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.user_profiles").collect()(0)(0)}")

  // INSERT #4: Fourth batch (25 users)
  println("Insert #4: Adding 25 users...")
  for (i <- 76 to 100) {
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.user_profiles
      VALUES ('user_${i}', 'username_${randomString(8)}', 'user${i}@example.com')
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.user_profiles").collect()(0)(0)}")

  // DELETE #1: Remove some users (creates delete snapshot)
  println("Delete #1: Removing users 1-10...")
  spark.sql(s"DELETE FROM openhouse.${dbName}.user_profiles WHERE user_id IN ('user_1', 'user_2', 'user_3', 'user_4', 'user_5', 'user_6', 'user_7', 'user_8', 'user_9', 'user_10')").show()
  println(s"Count after delete: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.user_profiles").collect()(0)(0)}")

  // INSERT #5: Final batch (10 users)
  println("Insert #5: Adding 10 users...")
  for (i <- 101 to 110) {
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.user_profiles
      VALUES ('user_${i}', 'username_${randomString(8)}', 'user${i}@example.com')
    """)
  }
  println(s"Final count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.user_profiles").collect()(0)(0)}")

  // ==========================================================================
  // UNPARTITIONED TABLE 2: product_catalog
  // ==========================================================================
  // Schema: product_id (STRING), product_name (STRING), price (DOUBLE)
  // ==========================================================================

  println(s"\n--- Table: openhouse.${dbName}.product_catalog ---")

  println("Dropping table if exists...")
  spark.sql(s"DROP TABLE IF EXISTS openhouse.${dbName}.product_catalog")

  println("Creating table...")
  spark.sql(s"""
    CREATE TABLE openhouse.${dbName}.product_catalog (
      product_id STRING,
      product_name STRING,
      price DOUBLE
    ) USING iceberg
  """).show()

  // INSERT #1: Initial products (20 products)
  println("Insert #1: Adding 20 products...")
  for (i <- 1 to 20) {
    val price = 10.0 + Random.nextDouble() * 990.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.product_catalog
      VALUES ('prod_${i}', 'Product_${randomString(6)}', ${price})
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.product_catalog").collect()(0)(0)}")

  // INSERT #2: Second batch (20 products)
  println("Insert #2: Adding 20 products...")
  for (i <- 21 to 40) {
    val price = 10.0 + Random.nextDouble() * 990.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.product_catalog
      VALUES ('prod_${i}', 'Product_${randomString(6)}', ${price})
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.product_catalog").collect()(0)(0)}")

  // INSERT #3: Third batch (20 products)
  println("Insert #3: Adding 20 products...")
  for (i <- 41 to 60) {
    val price = 10.0 + Random.nextDouble() * 990.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.product_catalog
      VALUES ('prod_${i}', 'Product_${randomString(6)}', ${price})
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.product_catalog").collect()(0)(0)}")

  // INSERT #4: Fourth batch (20 products)
  println("Insert #4: Adding 20 products...")
  for (i <- 61 to 80) {
    val price = 10.0 + Random.nextDouble() * 990.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.product_catalog
      VALUES ('prod_${i}', 'Product_${randomString(6)}', ${price})
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.product_catalog").collect()(0)(0)}")

  // DELETE #1: Remove some products
  println("Delete #1: Removing products 1-5...")
  spark.sql(s"DELETE FROM openhouse.${dbName}.product_catalog WHERE product_id IN ('prod_1', 'prod_2', 'prod_3', 'prod_4', 'prod_5')").show()
  println(s"Count after delete: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.product_catalog").collect()(0)(0)}")

  // INSERT #5: Final batch (10 products)
  println("Insert #5: Adding 10 products...")
  for (i <- 81 to 90) {
    val price = 10.0 + Random.nextDouble() * 990.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.product_catalog
      VALUES ('prod_${i}', 'Product_${randomString(6)}', ${price})
    """)
  }
  println(s"Final count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.product_catalog").collect()(0)(0)}")

  // ============================================================================
  // PART 2: PARTITIONED TABLES (HOURLY PARTITIONING)
  // ============================================================================

  println("\n" + "-" * 80)
  println("PART 2: Creating 3 Partitioned Tables (Hourly Partitioning)")
  println("-" * 80)

  // ==========================================================================
  // PARTITIONED TABLE 1: clickstream_events
  // ==========================================================================
  // Schema: event_id, user_id, page_url, event_time
  // Partitioned by: hours(event_time)
  // ==========================================================================

  println(s"\n--- Table: openhouse.${dbName}.clickstream_events ---")

  println("Dropping table if exists...")
  spark.sql(s"DROP TABLE IF EXISTS openhouse.${dbName}.clickstream_events")

  println("Creating table...")
  spark.sql(s"""
    CREATE TABLE openhouse.${dbName}.clickstream_events (
      event_id STRING,
      user_id STRING,
      page_url STRING,
      event_time TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(event_time))
  """).show()

  // INSERT #1: Events from last 24 hours (30 events)
  println("Insert #1: Adding 30 events (last 24 hours)...")
  for (i <- 1 to 30) {
    val ts = randomTimestamp(24)
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.clickstream_events
      VALUES ('event_${i}', 'user_${Random.nextInt(100)}', 'https://example.com/${randomString(10)}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.clickstream_events").collect()(0)(0)}")

  // INSERT #2: Events from last 48 hours (30 events)
  println("Insert #2: Adding 30 events (last 48 hours)...")
  for (i <- 31 to 60) {
    val ts = randomTimestamp(48)
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.clickstream_events
      VALUES ('event_${i}', 'user_${Random.nextInt(100)}', 'https://example.com/${randomString(10)}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.clickstream_events").collect()(0)(0)}")

  // INSERT #3: Events from last 72 hours (30 events)
  println("Insert #3: Adding 30 events (last 72 hours)...")
  for (i <- 61 to 90) {
    val ts = randomTimestamp(72)
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.clickstream_events
      VALUES ('event_${i}', 'user_${Random.nextInt(100)}', 'https://example.com/${randomString(10)}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.clickstream_events").collect()(0)(0)}")

  // INSERT #4: Events from last 96 hours (30 events)
  println("Insert #4: Adding 30 events (last 96 hours)...")
  for (i <- 91 to 120) {
    val ts = randomTimestamp(96)
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.clickstream_events
      VALUES ('event_${i}', 'user_${Random.nextInt(100)}', 'https://example.com/${randomString(10)}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.clickstream_events").collect()(0)(0)}")

  // DELETE #1: Remove some events
  println("Delete #1: Removing events 1-15...")
  spark.sql(s"DELETE FROM openhouse.${dbName}.clickstream_events WHERE event_id IN ('event_1', 'event_2', 'event_3', 'event_4', 'event_5', 'event_6', 'event_7', 'event_8', 'event_9', 'event_10', 'event_11', 'event_12', 'event_13', 'event_14', 'event_15')").show()
  println(s"Count after delete: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.clickstream_events").collect()(0)(0)}")

  // INSERT #5: Final batch (15 events)
  println("Insert #5: Adding 15 events...")
  for (i <- 121 to 135) {
    val ts = randomTimestamp(24)
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.clickstream_events
      VALUES ('event_${i}', 'user_${Random.nextInt(100)}', 'https://example.com/${randomString(10)}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Final count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.clickstream_events").collect()(0)(0)}")

  // ==========================================================================
  // PARTITIONED TABLE 2: sensor_readings
  // ==========================================================================
  // Schema: sensor_id, temperature, humidity, reading_time
  // Partitioned by: hours(reading_time)
  // Temperature: 15-35°C, Humidity: 30-80%
  // ==========================================================================

  println(s"\n--- Table: openhouse.${dbName}.sensor_readings ---")

  println("Dropping table if exists...")
  spark.sql(s"DROP TABLE IF EXISTS openhouse.${dbName}.sensor_readings")

  println("Creating table...")
  spark.sql(s"""
    CREATE TABLE openhouse.${dbName}.sensor_readings (
      sensor_id STRING,
      temperature DOUBLE,
      humidity DOUBLE,
      reading_time TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(reading_time))
  """).show()

  // INSERT #1: Readings from last 48 hours (25 readings)
  println("Insert #1: Adding 25 sensor readings (last 48 hours)...")
  for (i <- 1 to 25) {
    val ts = randomTimestamp(48)
    val temp = 15.0 + Random.nextDouble() * 20.0
    val humidity = 30.0 + Random.nextDouble() * 50.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.sensor_readings
      VALUES ('sensor_${Random.nextInt(10)}', ${temp}, ${humidity}, TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.sensor_readings").collect()(0)(0)}")

  // INSERT #2: Readings from last 72 hours (25 readings)
  println("Insert #2: Adding 25 sensor readings (last 72 hours)...")
  for (i <- 26 to 50) {
    val ts = randomTimestamp(72)
    val temp = 15.0 + Random.nextDouble() * 20.0
    val humidity = 30.0 + Random.nextDouble() * 50.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.sensor_readings
      VALUES ('sensor_${Random.nextInt(10)}', ${temp}, ${humidity}, TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.sensor_readings").collect()(0)(0)}")

  // INSERT #3: Readings from last 96 hours (25 readings)
  println("Insert #3: Adding 25 sensor readings (last 96 hours)...")
  for (i <- 51 to 75) {
    val ts = randomTimestamp(96)
    val temp = 15.0 + Random.nextDouble() * 20.0
    val humidity = 30.0 + Random.nextDouble() * 50.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.sensor_readings
      VALUES ('sensor_${Random.nextInt(10)}', ${temp}, ${humidity}, TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.sensor_readings").collect()(0)(0)}")

  // INSERT #4: Readings from last 120 hours (25 readings)
  println("Insert #4: Adding 25 sensor readings (last 120 hours)...")
  for (i <- 76 to 100) {
    val ts = randomTimestamp(120)
    val temp = 15.0 + Random.nextDouble() * 20.0
    val humidity = 30.0 + Random.nextDouble() * 50.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.sensor_readings
      VALUES ('sensor_${Random.nextInt(10)}', ${temp}, ${humidity}, TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.sensor_readings").collect()(0)(0)}")

  // DELETE #1: Remove all readings from sensor_0
  println("Delete #1: Removing readings from sensor_0...")
  spark.sql(s"DELETE FROM openhouse.${dbName}.sensor_readings WHERE sensor_id = 'sensor_0'").show()
  println(s"Count after delete: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.sensor_readings").collect()(0)(0)}")

  // INSERT #5: Final batch (15 readings)
  println("Insert #5: Adding 15 sensor readings...")
  for (i <- 101 to 115) {
    val ts = randomTimestamp(24)
    val temp = 15.0 + Random.nextDouble() * 20.0
    val humidity = 30.0 + Random.nextDouble() * 50.0
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.sensor_readings
      VALUES ('sensor_${Random.nextInt(10)}', ${temp}, ${humidity}, TIMESTAMP('${ts}'))
    """)
  }
  println(s"Final count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.sensor_readings").collect()(0)(0)}")

  // ==========================================================================
  // PARTITIONED TABLE 3: transaction_logs
  // ==========================================================================
  // Schema: transaction_id, amount, status, transaction_time
  // Partitioned by: hours(transaction_time)
  // Status values: completed, pending, failed, cancelled
  // Amount: $10 - $10,000
  // ==========================================================================

  println(s"\n--- Table: openhouse.${dbName}.transaction_logs ---")

  println("Dropping table if exists...")
  spark.sql(s"DROP TABLE IF EXISTS openhouse.${dbName}.transaction_logs")

  println("Creating table...")
  spark.sql(s"""
    CREATE TABLE openhouse.${dbName}.transaction_logs (
      transaction_id STRING,
      amount DOUBLE,
      status STRING,
      transaction_time TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (hours(transaction_time))
  """).show()

  val statuses = Array("completed", "pending", "failed", "cancelled")

  // INSERT #1: Transactions from last 36 hours (30 transactions)
  println("Insert #1: Adding 30 transactions (last 36 hours)...")
  for (i <- 1 to 30) {
    val ts = randomTimestamp(36)
    val amount = 10.0 + Random.nextDouble() * 9990.0
    val status = statuses(Random.nextInt(statuses.length))
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.transaction_logs
      VALUES ('txn_${i}', ${amount}, '${status}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.transaction_logs").collect()(0)(0)}")

  // INSERT #2: Transactions from last 48 hours (30 transactions)
  println("Insert #2: Adding 30 transactions (last 48 hours)...")
  for (i <- 31 to 60) {
    val ts = randomTimestamp(48)
    val amount = 10.0 + Random.nextDouble() * 9990.0
    val status = statuses(Random.nextInt(statuses.length))
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.transaction_logs
      VALUES ('txn_${i}', ${amount}, '${status}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.transaction_logs").collect()(0)(0)}")

  // INSERT #3: Transactions from last 72 hours (30 transactions)
  println("Insert #3: Adding 30 transactions (last 72 hours)...")
  for (i <- 61 to 90) {
    val ts = randomTimestamp(72)
    val amount = 10.0 + Random.nextDouble() * 9990.0
    val status = statuses(Random.nextInt(statuses.length))
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.transaction_logs
      VALUES ('txn_${i}', ${amount}, '${status}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.transaction_logs").collect()(0)(0)}")

  // INSERT #4: Transactions from last 96 hours (30 transactions)
  println("Insert #4: Adding 30 transactions (last 96 hours)...")
  for (i <- 91 to 120) {
    val ts = randomTimestamp(96)
    val amount = 10.0 + Random.nextDouble() * 9990.0
    val status = statuses(Random.nextInt(statuses.length))
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.transaction_logs
      VALUES ('txn_${i}', ${amount}, '${status}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.transaction_logs").collect()(0)(0)}")

  // DELETE #1: Remove all failed transactions (conditional delete across partitions)
  println("Delete #1: Removing failed transactions...")
  spark.sql(s"DELETE FROM openhouse.${dbName}.transaction_logs WHERE status = 'failed'").show()
  println(s"Count after delete: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.transaction_logs").collect()(0)(0)}")

  // INSERT #5: Final batch (20 transactions)
  println("Insert #5: Adding 20 transactions...")
  for (i <- 121 to 140) {
    val ts = randomTimestamp(24)
    val amount = 10.0 + Random.nextDouble() * 9990.0
    val status = statuses(Random.nextInt(statuses.length))
    spark.sql(s"""
      INSERT INTO openhouse.${dbName}.transaction_logs
      VALUES ('txn_${i}', ${amount}, '${status}', TIMESTAMP('${ts}'))
    """)
  }
  println(s"Final count: ${spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.transaction_logs").collect()(0)(0)}")

  // ==========================================================================
  // Database Summary
  // ==========================================================================

  println("\n" + "-" * 80)
  println(s"Database openhouse.${dbName} - Summary")
  println("-" * 80)

  println(s"\nAll tables in openhouse.${dbName}:")
  spark.sql(s"SHOW TABLES IN openhouse.${dbName}").show(false)

  println(s"\nTable row counts in openhouse.${dbName}:")
  val tables = Array("user_profiles", "product_catalog", "clickstream_events", "sensor_readings", "transaction_logs")
  for (table <- tables) {
    val count = spark.sql(s"SELECT COUNT(*) FROM openhouse.${dbName}.${table}").collect()(0)(0)
    println(s"  - ${table}: ${count} rows")
  }
}

// ============================================================================
// Main Execution: Populate All Databases
// ============================================================================
// Creates and populates tables in 3 databases: testdb, devdb, stagingdb
// ============================================================================

println("\nStarting population of 3 databases...")
println("This will create 5 tables in each database (15 tables total)")

// List of databases to populate
val databases = Array("testdb", "devdb", "stagingdb")

// Populate each database
for (dbName <- databases) {
  try {
    populateDatabase(dbName)
    println(s"\n✓ Successfully populated openhouse.${dbName}")
  } catch {
    case e: Exception =>
      println(s"\n✗ Error populating openhouse.${dbName}: ${e.getMessage}")
      e.printStackTrace()
  }
}

// ============================================================================
// Final Summary
// ============================================================================

println("\n" + "=" * 80)
println("FINAL SUMMARY")
println("=" * 80)

println("\nAll databases in openhouse catalog:")
try {
  spark.sql("SHOW DATABASES").show(false)
} catch {
  case e: Exception =>
    println("Unable to list databases (OpenHouse limitation)")
    println(s"Populated databases: ${databases.mkString(", ")}")
}

println("\nTotal tables created:")
for (dbName <- databases) {
  try {
    val tableCount = spark.sql(s"SHOW TABLES IN openhouse.${dbName}").count()
    println(s"  - openhouse.${dbName}: ${tableCount} tables")
  } catch {
    case e: Exception =>
      println(s"  - openhouse.${dbName}: Unable to count tables")
  }
}

println("\n" + "=" * 80)
println("Population Script Complete!")
println("=" * 80)

println("\nNext steps:")
println("  - List databases: spark.sql(\"SHOW DATABASES\").show()")
println("  - List tables: spark.sql(\"SHOW TABLES IN openhouse.testdb\").show()")
println("  - Query tables: spark.sql(\"SELECT * FROM openhouse.testdb.user_profiles LIMIT 10\").show()")
println("  - View snapshots: spark.sql(\"SELECT * FROM openhouse.testdb.user_profiles.snapshots\").show()")
println("  - View partitions: spark.sql(\"SELECT * FROM openhouse.testdb.clickstream_events.partitions\").show()")
println("  - Time travel: spark.sql(\"SELECT * FROM openhouse.testdb.user_profiles VERSION AS OF <snapshot_id>\").show()")
println("=" * 80)
