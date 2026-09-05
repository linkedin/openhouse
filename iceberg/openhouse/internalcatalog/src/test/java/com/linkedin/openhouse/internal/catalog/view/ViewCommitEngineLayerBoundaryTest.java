package com.linkedin.openhouse.internal.catalog.view;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The layer split, asserted structurally rather than only through behaviour. Identity generation,
 * storage selection, and root allocation belong to the service above this engine, exactly as they
 * belong above the internal table catalog, and a behavioural test alone would keep passing if
 * someone reintroduced a fallback that happened to agree with the fixtures.
 */
public class ViewCommitEngineLayerBoundaryTest {

  /** Names that only appear if the engine has taken a service responsibility back. */
  private static final List<String> FORBIDDEN_REFERENCES =
      Arrays.asList("selectStorage", "allocateTableLocation", "StorageSelector");

  private static final List<String> FORBIDDEN_COLLABORATORS =
      Arrays.asList(
          "com.linkedin.openhouse.cluster.storage.selector.StorageSelector",
          "com.linkedin.openhouse.cluster.storage.Storage");

  @Test
  void theEngineDeclaresNoStorageSelectionOrAllocationCollaborator() {
    List<String> offenders = new ArrayList<>();
    for (Field field : ViewCommitEngineImpl.class.getDeclaredFields()) {
      if (FORBIDDEN_COLLABORATORS.contains(field.getType().getName())) {
        offenders.add("field " + field.getName() + " of type " + field.getType().getName());
      }
    }
    for (Constructor<?> constructor : ViewCommitEngineImpl.class.getDeclaredConstructors()) {
      for (Class<?> parameter : constructor.getParameterTypes()) {
        if (FORBIDDEN_COLLABORATORS.contains(parameter.getName())) {
          offenders.add("constructor parameter of type " + parameter.getName());
        }
      }
    }
    Assertions.assertTrue(
        offenders.isEmpty(),
        "the commit engine must not depend on storage selection or allocation: " + offenders);
  }

  /**
   * The constant pool carries every method the class calls, so this catches a static or inlined
   * call that no injected collaborator would reveal.
   */
  @Test
  void theEngineNeverCallsStorageSelectionOrRootAllocation() {
    String constantPool = constantPoolOf(ViewCommitEngineImpl.class);
    for (String forbidden : FORBIDDEN_REFERENCES) {
      Assertions.assertFalse(
          constantPool.contains(forbidden),
          "the commit engine must not reference " + forbidden + "; allocation belongs above it");
    }
  }

  /**
   * Guards the guard: a probe that genuinely does allocate must be caught, or the scan above could
   * be silently looking at the wrong bytes.
   */
  @Test
  void theConstantPoolScanFindsAReferenceThatIsActuallyThere() {
    Assertions.assertTrue(
        constantPoolOf(AllocationProbe.class).contains("allocateTableLocation"),
        "the scan must see a call that is present, or it proves nothing about its absence");
    Assertions.assertFalse(
        constantPoolOf(AllocationProbe.class).contains("thisStringAppearsNowhere"),
        "the scan must not report a name that is absent");
  }

  private static String constantPoolOf(Class<?> type) {
    String resource = type.getName().replace('.', '/') + ".class";
    ClassLoader loader =
        type.getClassLoader() == null ? ClassLoader.getSystemClassLoader() : type.getClassLoader();
    try (InputStream stream = loader.getResourceAsStream(resource)) {
      Assertions.assertNotNull(stream, "could not read the class file for " + type.getName());
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int read;
      while ((read = stream.read(chunk)) != -1) {
        buffer.write(chunk, 0, read);
      }
      return new String(buffer.toByteArray(), StandardCharsets.ISO_8859_1);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("unused")
  private static final class AllocationProbe {
    String allocate(com.linkedin.openhouse.cluster.storage.Storage storage) {
      return storage.allocateTableLocation(
          "db", "v", "uuid", "creator", java.util.Collections.emptyMap());
    }
  }
}
