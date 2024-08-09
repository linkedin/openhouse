spark.sql("CREATE TABLE openhouse.testdb.tbA (ts timestamp, col1 string, col2 string) PARTITIONED BY (days(ts))").show()
println("created first table: ")
spark.sql("DESCRIBE TABLE openhouse.testdb.tbA").show()
for (x <- 0 until 100) {
  spark.sql(s"INSERT INTO TABLE openhouse.testdb.tbA VALUES (current_timestamp(), 'testval1 $x', 'testval2 $x')")
}
println("inserted into first table")

spark.sql("CREATE TABLE openhouse.testdb.tbB (ts timestamp, col1 int) PARTITIONED BY (days(ts))").show()
println("created second table: ")
spark.sql("DESCRIBE TABLE openhouse.testdb.tbB").show()
for (x <- 0 until 100) {
  spark.sql(s"INSERT INTO TABLE openhouse.testdb.tbB VALUES (current_timestamp(), $x)")
}
println("inserted into second table")