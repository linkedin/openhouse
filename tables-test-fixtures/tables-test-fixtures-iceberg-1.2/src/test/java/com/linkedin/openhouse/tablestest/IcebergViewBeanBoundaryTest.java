package com.linkedin.openhouse.tablestest;

import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
import com.linkedin.openhouse.internal.catalog.model.HouseTable;
import com.linkedin.openhouse.internal.catalog.view.ViewCommitEngine;
import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.SqlViewRepresentationIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Guards the Iceberg 1.2 / 1.5 boundary. Both fixtures compile and run this, so the expectation is
 * chosen at runtime from the classpath.
 *
 * <p>Under 1.2 a view type anywhere on a scanned bean signature fails during Spring introspection,
 * hence a reflective walk over every bean plus a direct audit of the contract and its DTOs.
 */
public class IcebergViewBeanBoundaryTest {

  private static final String VIEW_PACKAGE_PREFIX = "org.apache.iceberg.view.";
  private static final String VIEW_METADATA_CLASS = "org.apache.iceberg.view.ViewMetadata";
  private static final String VIEW_CODEC_CLASS =
      "com.linkedin.openhouse.internal.catalog.view.ViewMetadataCodec";
  private static final String VIEW_COMMIT_ENGINE_IMPL_CLASS =
      "com.linkedin.openhouse.internal.catalog.view.ViewCommitEngineImpl";

  private static final List<Class<?>> VERSION_NEUTRAL_TYPES =
      Arrays.asList(
          ViewCommitEngine.class,
          ViewCommitIntent.class,
          ViewCommitIntent.ViewCommitIntentBuilder.class,
          ViewCommitResult.class,
          ViewPointer.class,
          LoadedView.class,
          SqlViewRepresentationIntent.class,
          HouseTable.class,
          HouseTable.HouseTableBuilder.class);

  private static boolean icebergViewApiPresent() {
    try {
      Class.forName(VIEW_METADATA_CLASS);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private ConfigurableApplicationContext boot() {
    try {
      org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.register();
    } catch (Error e) {
      org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.disable();
    }
    SpringApplication application = new SpringApplication(SpringH2TestApplication.class);
    application.setDefaultProperties(Collections.singletonMap("server.port", "0"));
    return application.run();
  }

  @Test
  public void viewCommitBeansExistOnlyWhereTheIcebergViewApiDoes() {
    try (ConfigurableApplicationContext context = boot()) {
      String[] engineBeans = context.getBeanNamesForType(ViewCommitEngine.class);

      if (icebergViewApiPresent()) {
        Assertions.assertEquals(
            1,
            engineBeans.length,
            "expected exactly one view commit engine under Iceberg 1.5, found "
                + Arrays.toString(engineBeans));
        Assertions.assertEquals(
            1,
            context.getBeanNamesForType(loadOrFail(VIEW_CODEC_CLASS)).length,
            "expected exactly one view metadata codec under Iceberg 1.5");
        Assertions.assertNotNull(
            context.getBean(loadOrFail(VIEW_COMMIT_ENGINE_IMPL_CLASS)),
            "the 1.5 implementation must be the registered bean");
      } else {
        Assertions.assertEquals(
            0,
            engineBeans.length,
            "no view commit engine may exist under Iceberg 1.2, found "
                + Arrays.toString(engineBeans));
        for (String name : context.getBeanDefinitionNames()) {
          Assertions.assertFalse(
              name.toLowerCase().contains("viewmetadatacodec"),
              "no view metadata codec bean may exist under Iceberg 1.2: " + name);
        }
      }
    }
  }

  /** No bean is registered under 1.2, so the types are audited directly. */
  @Test
  public void theVersionNeutralContractAndItsValuesNameNoIcebergViewType() {
    List<String> offenders = new ArrayList<>();
    for (Class<?> type : VERSION_NEUTRAL_TYPES) {
      inspect(offenders, type.getSimpleName(), type);
    }
    Assertions.assertTrue(
        offenders.isEmpty(),
        "the view commit contract must stay loadable under Iceberg 1.2: " + offenders);
  }

  /**
   * Directly exercises the nullable create-flag / base-row builder contract. It runs under both
   * runtimes (the 1.5 fixture pulls in these 1.2 test sources), so it also proves the neutral types
   * build and read back under Iceberg 1.2 and that the retired operation and null-token APIs are
   * gone on both.
   */
  @Test
  public void theVersionNeutralIntentExposesTheCreateFlagAndBaseRowAndDropsTheOldApis() {
    HouseTable base =
        HouseTable.builder()
            .databaseId("db")
            .tableId("v")
            .tableLocation("/loc/00001-a.metadata.json")
            .storageType("local")
            .entityType("VIEW")
            .build();
    Assertions.assertEquals("/loc/00001-a.metadata.json", base.getTableLocation());

    ViewCommitIntent replace =
        ViewCommitIntent.builder()
            .databaseId("db")
            .viewId("v")
            .isCreate(false)
            .baseRow(base)
            .build();
    Assertions.assertEquals(Boolean.FALSE, replace.getIsCreate());
    Assertions.assertSame(base, replace.getBaseRow());

    // An unchanged toBuilder round-trip retains both new fields (the two-runtime retention
    // guarantee).
    ViewCommitIntent retained = replace.toBuilder().build();
    Assertions.assertEquals(
        Boolean.FALSE, retained.getIsCreate(), "toBuilder retains the create flag");
    Assertions.assertSame(base, retained.getBaseRow(), "toBuilder retains the captured row");

    // toBuilder round-trips the new fields and can flip the flag and clear the row.
    ViewCommitIntent create = replace.toBuilder().isCreate(true).baseRow(null).build();
    Assertions.assertEquals(Boolean.TRUE, create.getIsCreate());
    Assertions.assertNull(create.getBaseRow());

    // The boxed flag round-trips true, false, and null (an omitted flag is null, never a default).
    Assertions.assertEquals(
        Boolean.TRUE, ViewCommitIntent.builder().isCreate(true).build().getIsCreate());
    Assertions.assertEquals(
        Boolean.FALSE, ViewCommitIntent.builder().isCreate(false).build().getIsCreate());
    Assertions.assertNull(
        ViewCommitIntent.builder().isCreate(null).build().getIsCreate(),
        "a null create flag stays null");
    Assertions.assertNull(
        ViewCommitIntent.builder().build().getIsCreate(), "an omitted create flag is null");

    // The retired operation enum API is gone: no getOperation getter, no operation builder method.
    Assertions.assertTrue(
        Arrays.stream(ViewCommitIntent.class.getMethods())
            .noneMatch(method -> method.getName().equals("getOperation")),
        "the retired getOperation getter must not exist on the intent");
    Assertions.assertTrue(
        Arrays.stream(ViewCommitIntent.ViewCommitIntentBuilder.class.getMethods())
            .noneMatch(method -> method.getName().equals("operation")),
        "the retired operation builder method must not exist");

    // The retired null-token API is also gone: no getter on the value, no method on the builder.
    Assertions.assertTrue(
        Arrays.stream(ViewCommitIntent.class.getMethods())
            .noneMatch(method -> method.getName().equals("getBaseViewVersion")),
        "the retired baseViewVersion getter must not exist on the intent");
    Assertions.assertTrue(
        Arrays.stream(ViewCommitIntent.ViewCommitIntentBuilder.class.getMethods())
            .noneMatch(method -> method.getName().equals("baseViewVersion")),
        "the retired baseViewVersion builder method must not exist");
  }

  /** The conditional family is the only place an Iceberg view type may appear. */
  @Test
  public void onlyTheConditionalFamilyIsExemptFromTheVersionNeutralAudit() {
    Assertions.assertTrue(
        VERSION_NEUTRAL_TYPES.stream().noneMatch(IcebergViewBeanBoundaryTest::isViewScopedBean),
        "no audited type may be exempt from its own audit");
    if (icebergViewApiPresent()) {
      Assertions.assertTrue(
          isViewScopedBean(loadOrFail(VIEW_CODEC_CLASS)),
          "the codec legitimately names Iceberg view types and must stay exempt");
      Assertions.assertTrue(
          isViewScopedBean(loadOrFail(VIEW_COMMIT_ENGINE_IMPL_CLASS)),
          "the 1.5 implementation legitimately names Iceberg view types and must stay exempt");
    }
  }

  /**
   * Over {@link Type}, not erased {@link Class}: {@code List<ViewMetadata>} erases to {@code List}.
   */
  @Test
  public void noSharedBeanSignatureNamesAnIcebergViewType() {
    try (ConfigurableApplicationContext context = boot()) {
      List<String> offenders = new ArrayList<>();
      for (String name : context.getBeanDefinitionNames()) {
        Class<?> type = context.getType(name);
        if (type == null || isViewScopedBean(type)) {
          continue;
        }
        inspect(offenders, name, type);
      }
      Assertions.assertTrue(
          offenders.isEmpty(),
          "shared Spring beans must not name org.apache.iceberg.view types: " + offenders);
    }
  }

  private static void inspect(List<String> offenders, String beanName, Class<?> type) {
    inspect(offenders, beanName, type, VIEW_PACKAGE_PREFIX);
  }

  /** Every place a type can be named, including generic supertypes, bounds, and throws clauses. */
  private static void inspect(
      List<String> offenders, String beanName, Class<?> type, String prefix) {
    safely(
        offenders,
        beanName,
        type,
        prefix,
        () -> {
          for (Method method : type.getDeclaredMethods()) {
            recordMethod(offenders, beanName, type, method, prefix);
          }
        });
    safely(
        offenders,
        beanName,
        type,
        prefix,
        () -> {
          for (Method method : type.getMethods()) {
            recordMethod(offenders, beanName, type, method, prefix);
          }
        });
    safely(
        offenders,
        beanName,
        type,
        prefix,
        () -> {
          for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            for (Type parameter : constructor.getGenericParameterTypes()) {
              record(offenders, beanName, type, parameter, prefix, new HashSet<Type>());
            }
            for (Type thrown : constructor.getGenericExceptionTypes()) {
              record(offenders, beanName, type, thrown, prefix, new HashSet<Type>());
            }
            // A bound no parameter mentions is still part of the signature.
            for (TypeVariable<?> variable : constructor.getTypeParameters()) {
              for (Type bound : variable.getBounds()) {
                record(offenders, beanName, type, bound, prefix, new HashSet<Type>());
              }
            }
          }
        });
    safely(
        offenders,
        beanName,
        type,
        prefix,
        () -> {
          for (Field field : type.getDeclaredFields()) {
            record(offenders, beanName, type, field.getGenericType(), prefix, new HashSet<Type>());
          }
        });
    safely(
        offenders,
        beanName,
        type,
        prefix,
        () -> {
          for (TypeVariable<?> variable : type.getTypeParameters()) {
            for (Type bound : variable.getBounds()) {
              record(offenders, beanName, type, bound, prefix, new HashSet<Type>());
            }
          }
        });
    safely(
        offenders, beanName, type, prefix, () -> recordAncestry(offenders, beanName, type, prefix));
  }

  /** Read at every level: a grandchild's immediate supertype is raw, so one hop sees nothing. */
  private static void recordAncestry(
      List<String> offenders, String beanName, Class<?> beanType, String prefix) {
    Set<Class<?>> visited = new HashSet<>();
    Deque<Class<?>> pending = new ArrayDeque<>();
    pending.add(beanType);
    while (!pending.isEmpty()) {
      Class<?> current = pending.poll();
      if (current == null || Object.class.equals(current) || !visited.add(current)) {
        continue;
      }
      record(
          offenders,
          beanName,
          beanType,
          current.getGenericSuperclass(),
          prefix,
          new HashSet<Type>());
      for (Type implemented : current.getGenericInterfaces()) {
        record(offenders, beanName, beanType, implemented, prefix, new HashSet<Type>());
      }
      if (current.getSuperclass() != null) {
        pending.add(current.getSuperclass());
      }
      Collections.addAll(pending, current.getInterfaces());
    }
  }

  private static void recordMethod(
      List<String> offenders, String beanName, Class<?> type, Method method, String prefix) {
    record(offenders, beanName, type, method.getGenericReturnType(), prefix, new HashSet<Type>());
    for (Type parameter : method.getGenericParameterTypes()) {
      record(offenders, beanName, type, parameter, prefix, new HashSet<Type>());
    }
    for (Type thrown : method.getGenericExceptionTypes()) {
      record(offenders, beanName, type, thrown, prefix, new HashSet<Type>());
    }
    for (TypeVariable<?> variable : method.getTypeParameters()) {
      for (Type bound : variable.getBounds()) {
        record(offenders, beanName, type, bound, prefix, new HashSet<Type>());
      }
    }
  }

  /**
   * One unresolvable member must not blind the scan for its siblings, nor be silently tolerated.
   */
  private static void safely(
      List<String> offenders, String beanName, Class<?> type, String prefix, Runnable inspection) {
    try {
      inspection.run();
    } catch (TypeNotPresentException | NoClassDefFoundError e) {
      handleInspectionFailure(offenders, beanName, type, e, prefix);
    }
  }

  static void handleInspectionFailure(
      List<String> offenders, String beanName, Class<?> type, Throwable failure, String prefix) {
    String detail = String.valueOf(failure.getMessage());
    String slashed = prefix.replace('.', '/');
    if (detail.contains(prefix) || detail.contains(slashed)) {
      offenders.add(
          beanName
              + " ("
              + type.getName()
              + ") has a signature naming an absent type in "
              + prefix
              + ": "
              + detail);
      return;
    }

    Boolean referencesPrefix = classFileReferences(type, prefix);
    if (Boolean.TRUE.equals(referencesPrefix)) {
      offenders.add(
          beanName
              + " ("
              + type.getName()
              + ") references "
              + prefix
              + " in its class file, found by the class-file fallback after reflection failed on: "
              + detail);
      return;
    }
    if (referencesPrefix == null) {
      offenders.add(
          beanName
              + " ("
              + type.getName()
              + ") could not be inspected reflectively or read as a class file: "
              + detail);
      return;
    }
    if (!isKnownOptionalDependencyGap(type, detail)) {
      offenders.add(
          beanName
              + " ("
              + type.getName()
              + ") is not inspectable and is not an allowlisted optional-dependency bean: "
              + detail);
    }
  }

  /**
   * Reads signatures straight from the constant pool, resolving nothing.
   *
   * @return true when found, false when absent, null when the class file is unreadable
   */
  static Boolean classFileReferences(Class<?> type, String prefix) {
    String resource = type.getName().replace('.', '/') + ".class";
    ClassLoader loader =
        type.getClassLoader() == null ? ClassLoader.getSystemClassLoader() : type.getClassLoader();
    try (InputStream stream = loader.getResourceAsStream(resource)) {
      if (stream == null) {
        return null;
      }
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int read;
      while ((read = stream.read(chunk)) != -1) {
        buffer.write(chunk, 0, read);
      }
      String constantPool = new String(buffer.toByteArray(), StandardCharsets.ISO_8859_1);
      return constantPool.contains(prefix.replace('.', '/')) || constantPool.contains(prefix);
    } catch (IOException e) {
      return null;
    }
  }

  /** Keyed on both the exact bean and the exact missing dependency; broader masks leaks. */
  static boolean isKnownOptionalDependencyGap(Class<?> type, String detail) {
    String missing = detail.toLowerCase(java.util.Locale.ROOT);
    for (Map.Entry<String, String> allowed : OPTIONAL_DEPENDENCY_GAPS.entrySet()) {
      if (type.getName().equals(allowed.getKey()) && missing.contains(allowed.getValue())) {
        return true;
      }
    }
    return false;
  }

  private static final Map<String, String> OPTIONAL_DEPENDENCY_GAPS = optionalDependencyGaps();

  private static Map<String, String> optionalDependencyGaps() {
    Map<String, String> gaps = new HashMap<>();
    // Registered unconditionally by springdoc; Querydsl is not a dependency here.
    gaps.put(
        "org.springdoc.data.rest.customisers.QuerydslPredicateOperationCustomizer", "querydsl");
    return gaps;
  }

  /** Walks parameterized types, arrays, wildcards, and bounds to their components. */
  private static void record(
      List<String> offenders,
      String beanName,
      Class<?> beanType,
      Type candidate,
      String prefix,
      Set<Type> seen) {
    if (candidate == null || !seen.add(candidate)) {
      return;
    }
    if (candidate instanceof Class<?>) {
      Class<?> component = (Class<?>) candidate;
      while (component.isArray()) {
        component = component.getComponentType();
      }
      if (component.getName().startsWith(prefix)) {
        offenders.add(beanName + " (" + beanType.getName() + ") names " + component.getName());
      }
      return;
    }
    if (candidate instanceof ParameterizedType) {
      ParameterizedType parameterized = (ParameterizedType) candidate;
      record(offenders, beanName, beanType, parameterized.getRawType(), prefix, seen);
      // An inner class carries its enclosing type's arguments on the owner.
      record(offenders, beanName, beanType, parameterized.getOwnerType(), prefix, seen);
      for (Type argument : parameterized.getActualTypeArguments()) {
        record(offenders, beanName, beanType, argument, prefix, seen);
      }
      return;
    }
    if (candidate instanceof GenericArrayType) {
      record(
          offenders,
          beanName,
          beanType,
          ((GenericArrayType) candidate).getGenericComponentType(),
          prefix,
          seen);
      return;
    }
    if (candidate instanceof WildcardType) {
      WildcardType wildcard = (WildcardType) candidate;
      for (Type bound : wildcard.getUpperBounds()) {
        record(offenders, beanName, beanType, bound, prefix, seen);
      }
      for (Type bound : wildcard.getLowerBounds()) {
        record(offenders, beanName, beanType, bound, prefix, seen);
      }
      return;
    }
    if (candidate instanceof TypeVariable) {
      for (Type bound : ((TypeVariable<?>) candidate).getBounds()) {
        record(offenders, beanName, beanType, bound, prefix, seen);
      }
    }
  }

  @Test
  public void unqualifiedCatalogStillResolvesToTheTableCatalog() {
    try (ConfigurableApplicationContext context = boot()) {
      Assertions.assertTrue(
          context.getBean(Catalog.class) instanceof OpenHouseInternalCatalog,
          "an unqualified Catalog lookup must keep resolving to OpenHouseInternalCatalog");
    }
  }

  /** An erased walk would silently pass everything. */
  @Test
  public void theSignatureWalkerDescendsIntoGenericsArraysAndBounds() {
    String probedPrefix = "java.util.concurrent.";
    List<String> offenders = new ArrayList<>();
    for (Method method : NestedTypeProbe.class.getDeclaredMethods()) {
      record(
          offenders,
          "probe",
          NestedTypeProbe.class,
          method.getGenericReturnType(),
          probedPrefix,
          new HashSet<>());
      for (Type parameter : method.getGenericParameterTypes()) {
        record(
            offenders,
            "probe",
            NestedTypeProbe.class,
            parameter,
            probedPrefix,
            new HashSet<Type>());
      }
    }
    for (Field field : NestedTypeProbe.class.getDeclaredFields()) {
      record(
          offenders,
          "probe",
          NestedTypeProbe.class,
          field.getGenericType(),
          probedPrefix,
          new HashSet<>());
    }

    Assertions.assertTrue(
        offenders.stream().anyMatch(offender -> offender.contains("java.util.concurrent.Callable")),
        "a type nested inside a generic argument must be found: " + offenders);
    Assertions.assertTrue(
        offenders.stream().anyMatch(offender -> offender.contains("java.util.concurrent.Future")),
        "a type nested inside an array of generics must be found: " + offenders);
    Assertions.assertTrue(
        offenders.stream().anyMatch(offender -> offender.contains("java.util.concurrent.TimeUnit")),
        "a type nested inside a wildcard bound must be found: " + offenders);
    Assertions.assertTrue(
        offenders.stream()
            .anyMatch(offender -> offender.contains("java.util.concurrent.ExecutorService")),
        "a type nested inside a type-variable bound must be found: " + offenders);

    List<String> clean = new ArrayList<>();
    for (Field field : CleanProbe.class.getDeclaredFields()) {
      record(
          clean,
          "clean",
          CleanProbe.class,
          field.getGenericType(),
          probedPrefix,
          new HashSet<Type>());
    }
    Assertions.assertTrue(clean.isEmpty(), "a clean signature must not be flagged: " + clean);
  }

  /** Reflection fails on one member while another leaks; the fallback must report it. */
  @Test
  public void anUnrelatedResolutionFailureCannotHideALeakOnAnotherMember() {
    String probedPrefix = "java.util.concurrent.";
    NoClassDefFoundError unrelated = new NoClassDefFoundError("com/querydsl/core/types/Predicate");

    // NestedTypeProbe's class file references java.util.concurrent on several members.
    Assertions.assertEquals(
        Boolean.TRUE,
        classFileReferences(NestedTypeProbe.class, probedPrefix),
        "the fallback must see the reference without resolving anything");

    List<String> offenders = new ArrayList<>();
    handleInspectionFailure(offenders, "leaky", NestedTypeProbe.class, unrelated, probedPrefix);
    Assertions.assertEquals(
        1,
        offenders.size(),
        "an unrelated linkage failure must not suppress a leak on another member: " + offenders);
    Assertions.assertTrue(offenders.get(0).contains("class-file fallback"), offenders.get(0));

    // "Could not check" is not "clean", so it is still reported.
    List<String> unverifiable = new ArrayList<>();
    handleInspectionFailure(unverifiable, "unknownBean", CleanProbe.class, unrelated, probedPrefix);
    Assertions.assertEquals(1, unverifiable.size(), String.valueOf(unverifiable));
    Assertions.assertTrue(unverifiable.get(0).contains("not an allowlisted"), unverifiable.get(0));

    // A failure naming the package directly needs no fallback.
    List<String> direct = new ArrayList<>();
    handleInspectionFailure(
        direct,
        "direct",
        CleanProbe.class,
        new NoClassDefFoundError("java/util/concurrent/Callable"),
        probedPrefix);
    Assertions.assertEquals(1, direct.size(), String.valueOf(direct));
  }

  /** Keyed on both the exact bean type and the exact missing dependency. */
  @Test
  public void theOptionalDependencyAllowlistIsNarrow() {
    Assertions.assertFalse(
        isKnownOptionalDependencyGap(CleanProbe.class, "com/querydsl/core/types/Predicate"),
        "an arbitrary bean may not ride the allowlist");
  }

  /** The two shapes a member-only walk cannot see, driven through the real entry point. */
  @Test
  public void inspectFindsAnInheritedConcreteBindingAndAnUnusedBound() {
    String probedPrefix = "java.util.concurrent.";

    List<String> inherited = new ArrayList<>();
    inspect(inherited, "inheritedBinding", InheritedBindingProbe.class, probedPrefix);
    Assertions.assertTrue(
        inherited.stream().anyMatch(offender -> offender.contains("java.util.concurrent.Callable")),
        "a parent's type variable bound to a concrete forbidden argument must be found; its"
            + " inherited method erases to Object, so only the generic supertype names it: "
            + inherited);

    List<String> unusedBound = new ArrayList<>();
    inspect(unusedBound, "unusedBound", UnusedBoundProbe.class, probedPrefix);
    Assertions.assertTrue(
        unusedBound.stream()
            .anyMatch(offender -> offender.contains("java.util.concurrent.ExecutorService")),
        "a method type bound that no parameter mentions is still declared signature: "
            + unusedBound);
    Assertions.assertTrue(
        unusedBound.stream()
            .anyMatch(offender -> offender.contains("java.util.concurrent.TimeoutException")),
        "a generic throws clause is part of the signature too: " + unusedBound);

    List<String> twoLevel = new ArrayList<>();
    inspect(twoLevel, "twoLevelBinding", TwoLevelBindingProbe.class, probedPrefix);
    Assertions.assertTrue(
        twoLevel.stream().anyMatch(offender -> offender.contains("java.util.concurrent.Callable")),
        "a binding two levels up must be found; this class's immediate supertype is a raw Class,"
            + " so a single-hop ancestor check sees nothing: "
            + twoLevel);

    List<String> throughInterfaces = new ArrayList<>();
    inspect(
        throughInterfaces,
        "inheritedInterfaceBinding",
        InheritedInterfaceBindingProbe.class,
        probedPrefix);
    Assertions.assertTrue(
        throughInterfaces.stream()
            .anyMatch(offender -> offender.contains("java.util.concurrent.Future")),
        "the same hole exists through an interface chain and must be closed too: "
            + throughInterfaces);

    List<String> clean = new ArrayList<>();
    inspect(clean, "clean", CleanProbe.class, probedPrefix);
    Assertions.assertTrue(clean.isEmpty(), "a clean type must not be flagged by inspect: " + clean);
  }

  @SuppressWarnings("unused")
  private static class GenericParent<T> {
    public T get() {
      return null;
    }
  }

  /** Names Callable only through its generic supertype. */
  @SuppressWarnings("unused")
  private static class InheritedBindingProbe
      extends GenericParent<java.util.concurrent.Callable<String>> {}

  /** Two levels up: the immediate supertype is raw, so one hop sees nothing. */
  @SuppressWarnings("unused")
  private static final class TwoLevelBindingProbe extends InheritedBindingProbe {}

  @SuppressWarnings("unused")
  private interface GenericFace<T> {
    T get();
  }

  @SuppressWarnings("unused")
  private interface MiddleFace extends GenericFace<java.util.concurrent.Future<String>> {}

  /** Abstract on purpose: a declared {@code get} would name Future directly and pass trivially. */
  @SuppressWarnings("unused")
  private abstract static class InheritedInterfaceBindingProbe implements MiddleFace {}

  @SuppressWarnings("unused")
  private static final class UnusedBoundProbe {
    <T extends java.util.concurrent.ExecutorService> void boundMentionedNowhereElse() {}

    void declaredThrows() throws java.util.concurrent.TimeoutException {}
  }

  @SuppressWarnings("unused")
  private static final class NestedTypeProbe {
    private List<java.util.concurrent.Callable<String>> nestedInGenericArgument;
    private List<? extends java.util.concurrent.TimeUnit> nestedInWildcardBound;

    private java.util.concurrent.Future<String>[] nestedInArrayOfGenerics() {
      return null;
    }

    private <T extends java.util.concurrent.ExecutorService> void nestedInTypeVariableBound(T t) {}
  }

  @SuppressWarnings("unused")
  private static final class CleanProbe {
    private List<String> plainGeneric;
    private String[] plainArray;
  }

  /** The only types allowed to name Iceberg view types. */
  private static boolean isViewScopedBean(Class<?> type) {
    return type.getName().startsWith("com.linkedin.openhouse.internal.catalog.view.")
        && (type.getName().endsWith("ViewMetadataCodec")
            || type.getName().endsWith("ViewCommitEngineImpl"));
  }

  private static Class<?> loadOrFail(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new AssertionError("expected " + className + " on the Iceberg 1.5 classpath", e);
    }
  }
}
