package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.InternalCatalogMetricsConstant;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The layer split asserted structurally: a behavioural test alone would keep passing if a fallback
 * were reintroduced that happened to agree with the fixtures.
 */
public class ViewCommitEngineLayerBoundaryTest {

  /** Names that appear only if the engine took a service responsibility back. */
  private static final List<String> FORBIDDEN_REFERENCES =
      Arrays.asList("selectStorage", "allocateTableLocation", "StorageSelector");

  private static final List<String> FORBIDDEN_COLLABORATORS =
      Arrays.asList(
          "com.linkedin.openhouse.cluster.storage.selector.StorageSelector",
          "com.linkedin.openhouse.cluster.storage.Storage");

  /** Query and lifecycle operations the engine no longer owns; HTS access belongs above it. */
  private static final List<String> RETIRED_OPERATIONS =
      Arrays.asList("loadView", "listViews", "dropView", "renameView");

  /** The contract is metadata publication only: exactly one operation, with this signature. */
  @Test
  void theEngineContractDeclaresOnlyTheCommitOperation() {
    List<Method> operations =
        Arrays.stream(ViewCommitEngine.class.getMethods())
            .filter(method -> !method.isSynthetic())
            .collect(Collectors.toList());
    Assertions.assertEquals(
        1, operations.size(), "the engine contract must expose only commit: " + operations);
    assertIsTheCommitSignature(operations.get(0));
  }

  @Test
  void theEngineImplementationExposesOnlyTheCommitOperation() {
    List<Method> publicMethods =
        Arrays.stream(ViewCommitEngineImpl.class.getDeclaredMethods())
            .filter(method -> !method.isSynthetic())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .collect(Collectors.toList());
    Assertions.assertEquals(
        1,
        publicMethods.size(),
        "the engine implementation must expose only commit: " + publicMethods);
    assertIsTheCommitSignature(publicMethods.get(0));
  }

  /** Any visibility: a retired operation kept as a private helper is still a second owner. */
  @Test
  void theRetiredOperationsAreAbsentFromTheContractAndTheImplementation() {
    for (Class<?> type : Arrays.asList(ViewCommitEngine.class, ViewCommitEngineImpl.class)) {
      List<String> present =
          Arrays.stream(type.getDeclaredMethods())
              .map(Method::getName)
              .filter(RETIRED_OPERATIONS::contains)
              .distinct()
              .collect(Collectors.toList());
      Assertions.assertTrue(
          present.isEmpty(), type.getSimpleName() + " must not declare " + present);
    }
  }

  /** The three retained view timers prove the field scan sees the class it inspects. */
  @Test
  void theRetiredViewLoadTimerConstantIsGoneAndTheThreeViewTimersRemain() throws Exception {
    List<String> fieldNames =
        Arrays.stream(InternalCatalogMetricsConstant.class.getDeclaredFields())
            .map(Field::getName)
            .collect(Collectors.toList());
    Assertions.assertFalse(
        fieldNames.contains("VIEW_LOAD_LATENCY"),
        "the engine no longer loads views, so it owns no load timer: " + fieldNames);
    Assertions.assertEquals(
        "view_commit_latency",
        InternalCatalogMetricsConstant.class.getField("VIEW_COMMIT_LATENCY").get(null));
    Assertions.assertEquals(
        "view_metadata_retrieval_latency",
        InternalCatalogMetricsConstant.class.getField("VIEW_METADATA_RETRIEVAL_LATENCY").get(null));
    Assertions.assertEquals(
        "view_metadata_update_latency",
        InternalCatalogMetricsConstant.class.getField("VIEW_METADATA_UPDATE_LATENCY").get(null));
    for (Field field : InternalCatalogMetricsConstant.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        field.setAccessible(true);
        Assertions.assertNotEquals(
            "view_load_latency",
            field.get(null),
            field.getName() + " must not reintroduce the retired load timer under another name");
      }
    }
  }

  private static void assertIsTheCommitSignature(Method method) {
    Assertions.assertEquals("commit", method.getName(), method.toString());
    Assertions.assertEquals(
        Arrays.asList(ViewCommitIntent.class),
        Arrays.asList(method.getParameterTypes()),
        method.toString());
    Assertions.assertEquals(ViewCommitResult.class, method.getReturnType(), method.toString());
  }

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

  /** The constant pool catches a static call that no injected collaborator would reveal. */
  @Test
  void theEngineNeverCallsStorageSelectionOrRootAllocation() {
    String constantPool = constantPoolOf(ViewCommitEngineImpl.class);
    for (String forbidden : FORBIDDEN_REFERENCES) {
      Assertions.assertFalse(
          constantPool.contains(forbidden),
          "the commit engine must not reference " + forbidden + "; allocation belongs above it");
    }
  }

  /** Guards the guard: otherwise the scan could be looking at the wrong bytes. */
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
