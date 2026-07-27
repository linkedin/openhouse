package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import java.util.Collections

import scala.collection.JavaConverters._

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import com.linkedin.openhouse.spark.sql.execution.datasources.v2.VacuumTableExec._

class VacuumTableExecTest {

  private def props(pairs: (String, String)*): java.util.Map[String, String] =
    pairs.toMap.asJava

  @Test
  def ofdRetainDaysDefaultsToThree(): Unit = {
    assertEquals(3L, ofdRetainDays(Collections.emptyMap()))
    assertEquals(3L, ofdRetainDays(props(OFD_ONE_DAY_TTL_ENABLED_PROP -> "false")))
  }

  @Test
  def ofdRetainDaysOneDayWhenEnabled(): Unit = {
    assertEquals(1L, ofdRetainDays(props(OFD_ONE_DAY_TTL_ENABLED_PROP -> "true")))
    assertEquals(1L, ofdRetainDays(props(OFD_ONE_DAY_TTL_ENABLED_PROP -> "TRUE")))
  }

  @Test
  def parseHistoryRetentionDefaultsWhenAbsent(): Unit = {
    val expected = HistoryRetention(3, "DAY", 0)
    assertEquals(expected, parseHistoryRetention(null))
    assertEquals(expected, parseHistoryRetention(""))
    assertEquals(expected, parseHistoryRetention("""{"retention":{"count":5}}"""))
  }

  @Test
  def parseHistoryRetentionReadsHistoryBlock(): Unit = {
    assertEquals(
      HistoryRetention(30, "DAY", 10),
      parseHistoryRetention("""{"history":{"maxAge":30,"granularity":"DAY","versions":10}}"""))
    assertEquals(
      HistoryRetention(6, "HOUR", 0),
      parseHistoryRetention("""{"history":{"maxAge":6,"granularity":"HOUR"}}"""))
  }

  @Test
  def parseHistoryRetentionFallsBackOnMalformedJson(): Unit = {
    assertEquals(HistoryRetention(3, "DAY", 0), parseHistoryRetention("not json"))
  }

  @Test
  def granularityToChronoMapsAllUnits(): Unit = {
    assertEquals(java.time.temporal.ChronoUnit.HOURS, granularityToChrono("HOUR"))
    assertEquals(java.time.temporal.ChronoUnit.DAYS, granularityToChrono("day"))
    assertEquals(java.time.temporal.ChronoUnit.MONTHS, granularityToChrono("MONTH"))
    assertEquals(java.time.temporal.ChronoUnit.YEARS, granularityToChrono("YEAR"))
    assertEquals(java.time.temporal.ChronoUnit.DAYS, granularityToChrono("unknown"))
  }
}
