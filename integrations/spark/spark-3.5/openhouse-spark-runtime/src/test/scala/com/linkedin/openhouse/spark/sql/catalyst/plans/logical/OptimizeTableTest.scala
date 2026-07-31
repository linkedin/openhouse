package com.linkedin.openhouse.spark.sql.catalyst.plans.logical

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import com.linkedin.openhouse.spark.sql.catalyst.plans.logical.OptimizeTable._

class OptimizeTableTest {

  @Test
  def parseClusterConfigResolvesDefaultsAndTypedValues(): Unit = {
    val empty = parseClusterConfig(Map.empty)
    assertTrue(empty.keys.isEmpty)
    assertEquals(DEFAULT_SORT_MODE, empty.sortMode)
    assertEquals(DEFAULT_MIN_SNAPSHOT_AGE_MINUTES, empty.minAgeMinutes)
    assertEquals(DEFAULT_MAX_COMMITS, empty.maxCommits)
    assertTrue(empty.hwm.isEmpty)
    assertTrue(empty.state.isEmpty)

    val cfg = parseClusterConfig(Map(
      KEYS_PROP -> " ts , uid ",
      SORT_MODE_PROP -> "sort",
      MIN_SNAPSHOT_AGE_PROP -> "5",
      MAX_COMMITS_PROP -> "3",
      HWM_PROP -> "42",
      STATE_PROP -> """[{"config":"c1","keys":"ts","mode":"sort","upper":"20"}]"""))
    assertEquals(Seq("ts", "uid"), cfg.keys)
    assertEquals("sort", cfg.sortMode)
    assertEquals(5L, cfg.minAgeMinutes)
    assertEquals(3L, cfg.maxCommits)
    assertEquals(Some(42L), cfg.hwm)
    assertEquals(Seq(ClusterInterval("c1", "ts", "sort", None, "20")), cfg.state)
  }

  @Test
  def configIdStableAcrossWhitespaceChangesOnKeyOrMode(): Unit = {
    assertEquals(configId(Seq("ts", "uid"), "zorder"), configId(Seq(" ts ", " uid "), "ZORDER"))
    assertNotEquals(configId(Seq("ts", "uid"), "zorder"), configId(Seq("ts"), "zorder"))
    assertNotEquals(configId(Seq("ts"), "zorder"), configId(Seq("ts"), "sort"))
  }

  @Test
  def parseStateRoundTripsWithAndWithoutLower(): Unit = {
    val json = """[{"config":"c1","keys":"ts","mode":"sort","lower":"10","upper":"20"},""" +
      """{"config":"c2","keys":"ts,uid","mode":"zorder","upper":"2026-01-06 00:00:00"}]"""
    assertEquals(Seq(
      ClusterInterval("c1", "ts", "sort", Some("10"), "20"),
      ClusterInterval("c2", "ts,uid", "zorder", None, "2026-01-06 00:00:00")), parseState(json))
  }

  @Test
  def parseStateEmptyOrNullIsNoState(): Unit = {
    assertEquals(Seq.empty[ClusterInterval], parseState(""))
    assertEquals(Seq.empty[ClusterInterval], parseState(null))
  }

  @Test
  def parseStateMalformedFailsLoudly(): Unit = {
    val e = assertThrows(classOf[IllegalStateException], () => parseState("not json"))
    assertTrue(e.getMessage.contains(STATE_PROP))
    assertTrue(e.getMessage.contains("UNSET TBLPROPERTIES"))
  }

  @Test
  def advanceStateFirstRunCreatesInterval(): Unit = {
    assertEquals(
      Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "10")),
      advanceState(Seq.empty, "c1", Seq("ts"), "sort", Some("5"), "10", full = false))
  }

  @Test
  def advanceStateSameConfigExtendsUpperKeepsLower(): Unit = {
    val s0 = Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "10"))
    assertEquals(
      Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "20")),
      advanceState(s0, "c1", Seq("ts"), "sort", Some("10"), "20", full = false))
  }

  @Test
  def advanceStateFullCollapsesToUnbounded(): Unit = {
    val s0 = Seq(ClusterInterval("c1", "ts", "sort", Some("5"), "20"))
    assertEquals(
      Seq(ClusterInterval("c1", "ts", "sort", None, "30")),
      advanceState(s0, "c1", Seq("ts"), "sort", None, "30", full = true))
  }

  @Test
  def advanceStateConfigChangeAppendsAndRetains(): Unit = {
    val s0 = Seq(ClusterInterval("c1", "ts", "sort", None, "20"))
    val s1 = advanceState(s0, "c2", Seq("ts", "uid"), "zorder", Some("20"), "40", full = false)
    assertEquals(Seq(
      ClusterInterval("c1", "ts", "sort", None, "20"),
      ClusterInterval("c2", "ts,uid", "zorder", Some("20"), "40")), s1)
  }
}
