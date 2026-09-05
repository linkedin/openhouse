package com.linkedin.openhouse.tablestest;

import com.linkedin.openhouse.internal.catalog.OpenHouseInternalCatalog;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
 * Guards the Iceberg 1.2 / 1.5 boundary. Lives in the 1.2 fixture's test sources, which the 1.5
 * fixture also compiles and runs, so the expectation is chosen at runtime from the classpath.
 *
 * <p>The fixture component-scans {@code com.linkedin.openhouse.internal.catalog}, and under 1.2 a
 * view type anywhere on a shared bean signature fails during Spring introspection rather than at
 * the call site. Hence a reflective check over every bean, not just the view ones, plus a direct
 * audit of the commit contract and its DTOs, which must stay loadable even where no bean exists.
 */
public class IcebergViewBeanBoundaryTest {

  private static final String VIEW_PACKAGE_PREFIX = "org.apache.iceberg.view.";
  private static final String VIEW_METADATA_CLASS = "org.apache.iceberg.view.ViewMetadata";
  private static final String VIEW_CODEC_CLASS =
      "com.linkedin.openhouse.internal.catalog.view.ViewMetadataCodec";
  private static final String VIEW_COMMIT_ENGINE_IMPL_CLASS =
      "com.linkedin.openhouse.internal.catalog.view.ViewCommitEngineImpl";

  /** The contract and every value it exchanges must load under either Iceberg. */
  private static final List<Class<?>> VERSION_NEUTRAL_TYPES =
      Arrays.asList(
          ViewCommitEngine.class,
          ViewCommitIntent.class,
          ViewCommitResult.class,
          ViewPointer.class,
          LoadedView.class,
          SqlViewRepresentationIntent.class);

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

  /** Under 1.2 the conditional configuration is skipped; under 1.5 there is one of each. */
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

  /**
   * A bean-registry walk cannot see the contract under Iceberg 1.2, because no bean is registered
   * there; this audits the types directly, so a view type on the interface or on any DTO is caught
   * on the version where it would actually break.
   */
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

  /** The conditional family is the only place an Iceberg view type may legitimately appear. */
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
   * The walk is over {@link Type}, not erased {@link Class}, because {@code List<ViewMetadata>}
   * erases to {@code List}; it includes inherited methods; and a resolution failure counts as an
   * offender, since that is the Spring-introspection failure this test exists to prevent.
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
    safely(
        offenders,
        beanName,
        type,
        () -> {
          for (Method method : type.getDeclaredMethods()) {
            record(offenders, beanName, type, method.getGenericReturnType());
            for (Type parameter : method.getGenericParameterTypes()) {
              record(offenders, beanName, type, parameter);
            }
          }
        });
    // Inherited public API is just as visible to Spring as declared API.
    safely(
        offenders,
        beanName,
        type,
        () -> {
          for (Method method : type.getMethods()) {
            record(offenders, beanName, type, method.getGenericReturnType());
            for (Type parameter : method.getGenericParameterTypes()) {
              record(offenders, beanName, type, parameter);
            }
          }
        });
    safely(
        offenders,
        beanName,
        type,
        () -> {
          for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            for (Type parameter : constructor.getGenericParameterTypes()) {
              record(offenders, beanName, type, parameter);
            }
          }
        });
    safely(
        offenders,
        beanName,
        type,
        () -> {
          for (Field field : type.getDeclaredFields()) {
            record(offenders, beanName, type, field.getGenericType());
          }
        });
    safely(
        offenders,
        beanName,
        type,
        () -> {
          for (TypeVariable<?> variable : type.getTypeParameters()) {
            for (Type bound : variable.getBounds()) {
              record(offenders, beanName, type, bound);
            }
          }
        });
  }

  /**
   * Reflection resolves a whole member category at once, so one unresolvable member would otherwise
   * blind the scan for its siblings. A failure is never ignored: an Iceberg view type is the leak;
   * anything else falls back to a class-file scan, and only the narrow allowlist is tolerated,
   * because "could not check" is not "clean".
   */
  private static void safely(
      List<String> offenders, String beanName, Class<?> type, Runnable inspection) {
    try {
      inspection.run();
    } catch (TypeNotPresentException | NoClassDefFoundError e) {
      handleInspectionFailure(offenders, beanName, type, e, VIEW_PACKAGE_PREFIX);
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
   * Descriptors and generic signatures live in the constant pool as plain text, so this sees every
   * declared signature without asking the class loader to resolve anything.
   *
   * @return true when found, false when definitely absent, null when the class file is unreadable
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

  /**
   * Keyed on both the exact bean and the exact missing dependency; anything broader masks leaks.
   */
  static boolean isKnownOptionalDependencyGap(Class<?> type, String detail) {
    String missing = detail.toLowerCase(java.util.Locale.ROOT);
    for (Map.Entry<String, String> allowed : OPTIONAL_DEPENDENCY_GAPS.entrySet()) {
      if (type.getName().equals(allowed.getKey()) && missing.contains(allowed.getValue())) {
        return true;
      }
    }
    return false;
  }

  /** Bean class name to the lowercase fragment of the dependency it may be missing. */
  private static final Map<String, String> OPTIONAL_DEPENDENCY_GAPS = optionalDependencyGaps();

  private static Map<String, String> optionalDependencyGaps() {
    Map<String, String> gaps = new HashMap<>();
    // Registered unconditionally by springdoc, but Querydsl is not a dependency of this fixture.
    gaps.put("org.springdoc.data.rest.customisers.QuerydslPredicateOperationCustomizer", "querydsl");
    return gaps;
  }

  private static void record(
      List<String> offenders, String beanName, Class<?> beanType, Type candidate) {
    record(offenders, beanName, beanType, candidate, VIEW_PACKAGE_PREFIX, new HashSet<Type>());
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

  /** The table catalog stays the one unqualified Iceberg catalog on both versions. */
  @Test
  public void unqualifiedCatalogStillResolvesToTheTableCatalog() {
    try (ConfigurableApplicationContext context = boot()) {
      Assertions.assertTrue(
          context.getBean(Catalog.class) instanceof OpenHouseInternalCatalog,
          "an unqualified Catalog lookup must keep resolving to OpenHouseInternalCatalog");
    }
  }

  /** Proves the walker descends at all: an erased walk would silently pass everything. */
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
            offenders, "probe", NestedTypeProbe.class, parameter, probedPrefix, new HashSet<Type>());
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
          clean, "clean", CleanProbe.class, field.getGenericType(), probedPrefix, new HashSet<Type>());
    }
    Assertions.assertTrue(clean.isEmpty(), "a clean signature must not be flagged: " + clean);
  }

  /** Reflection fails on one member while another leaks; the fallback must still report it. */
  @Test
  public void anUnrelatedResolutionFailureCannotHideALeakOnAnotherMember() {
    String probedPrefix = "java.util.concurrent.";
    NoClassDefFoundError unrelated = new NoClassDefFoundError("com/querydsl/core/types/Predicate");

    // NestedTypeProbe's class file references java.util.concurrent types on several members.
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

    // A bean that genuinely does not reference the package, and is not allowlisted, is still
    // reported: "could not check" is not "clean".
    List<String> unverifiable = new ArrayList<>();
    handleInspectionFailure(unverifiable, "unknownBean", CleanProbe.class, unrelated, probedPrefix);
    Assertions.assertEquals(1, unverifiable.size(), String.valueOf(unverifiable));
    Assertions.assertTrue(unverifiable.get(0).contains("not an allowlisted"), unverifiable.get(0));

    // The failure that names the searched package directly is reported without needing the
    // fallback.
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
