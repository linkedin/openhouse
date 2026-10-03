package com.linkedin.openhouse.tables.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Any positive size is a valid wire request, with no cap. An empty listing must succeed for the
 * largest sizes instead of reserving memory proportional to the requested size. The adapter runs in
 * a forked JVM capped at {@value #CHILD_HEAP} so a regression cannot exhaust the test host.
 */
public class ViewPaginationAdapterLargeSizeTest {

  private static final String CHILD_HEAP = "-Xmx64m";
  private static final long TIMEOUT_SECONDS = 60;

  @Test
  public void largestRequestedSizesOverAnEmptySourceSucceedWithoutSizeProportionalAllocation()
      throws Exception {
    List<String> sizes = Arrays.asList(String.valueOf(Integer.MAX_VALUE), "100000000");

    ChildResult child = runProbe(sizes);

    assertEquals(ViewPaginationLargeSizeProbe.OK, child.exitCode, child.output);
    for (String size : sizes) {
      assertTrue(child.output.contains("RESULT size=" + size + " rows=0 next=null"), child.output);
    }
  }

  private static ChildResult runProbe(List<String> sizes) throws Exception {
    String java =
        System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    List<String> command =
        new java.util.ArrayList<>(
            Arrays.asList(
                java,
                CHILD_HEAP,
                "-cp",
                System.getProperty("java.class.path"),
                ViewPaginationLargeSizeProbe.class.getName()));
    command.addAll(sizes);
    Path output = Files.createTempFile("view-pagination-probe", ".log");
    try {
      Process process =
          new ProcessBuilder(command)
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      if (!process.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        throw new AssertionError("Probe did not finish within " + TIMEOUT_SECONDS + "s");
      }
      return new ChildResult(
          process.exitValue(), new String(Files.readAllBytes(output), StandardCharsets.UTF_8));
    } finally {
      Files.deleteIfExists(output);
    }
  }

  private static final class ChildResult {
    private final int exitCode;
    private final String output;

    private ChildResult(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }
  }
}
