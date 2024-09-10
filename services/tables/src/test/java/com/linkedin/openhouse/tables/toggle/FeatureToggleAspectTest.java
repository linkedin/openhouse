package com.linkedin.openhouse.tables.toggle;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.cluster.configs.TblPropsToggleRegistry;
import com.linkedin.openhouse.internal.catalog.toggle.IcebergFeatureGate;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.repository.impl.TblPropsEnabler;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * This is a very much mock-ish test, leaving this in the same package as {@link
 * FeatureToggleAspect} mainly for cases like {@link #testObtainIdFromAnnotationArgs()}
 */
class FeatureToggleAspectTest {
  @Mock private TableFeatureToggle tableFeatureToggle;

  @Mock private TblPropsToggleRegistry tblPropsToggleRegistry;

  @Mock private JoinPoint joinPoint;
  @Mock private ProceedingJoinPoint proceedingJoinPoint;

  @Mock private MethodSignature methodSignature;

  @Mock private IcebergFeatureGate featureGate;

  @Mock private TblPropsEnabler tblPropsEnabler;

  @InjectMocks private FeatureToggleAspect featureToggleAspect;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testCheckTblPropEnabled_HappyPath() throws Throwable {
    TableDto tableDto = TableDto.builder().tableId("testTable").databaseId("testDb").build();
    Object[] args = new Object[] {"testKey", tableDto};

    when(proceedingJoinPoint.getSignature()).thenReturn(methodSignature);
    when(methodSignature.getReturnType()).thenReturn(boolean.class);
    when(proceedingJoinPoint.getArgs()).thenReturn(args);
    when(proceedingJoinPoint.proceed()).thenReturn(false);
    when(tableFeatureToggle.isFeatureActivated("testDb", "testTable", "TEST_FEATURE"))
        .thenReturn(true);
    when(tblPropsToggleRegistry.obtainFeatureByKey("testKey"))
        .thenReturn(Optional.of("TEST_FEATURE"));

    boolean result = featureToggleAspect.checkTblPropEnabled(proceedingJoinPoint, tblPropsEnabler);

    assertTrue(result);
    verify(proceedingJoinPoint, times(1)).proceed();
  }

  @Test
  void testCheckTblPropEnabled_FeatureNotEnabled() throws Throwable {
    TableDto tableDto = TableDto.builder().tableId("testTable").databaseId("testDb").build();
    Object[] args = new Object[] {"testKey", tableDto};

    when(proceedingJoinPoint.getSignature()).thenReturn(methodSignature);
    when(methodSignature.getReturnType()).thenReturn(boolean.class);
    when(proceedingJoinPoint.getArgs()).thenReturn(args);
    when(proceedingJoinPoint.proceed()).thenReturn(false);
    when(tableFeatureToggle.isFeatureActivated("testDb", "testTable", "TEST_FEATURE"))
        .thenReturn(false);
    when(tblPropsToggleRegistry.obtainFeatureByKey("testKey"))
        .thenReturn(Optional.of("TEST_FEATURE"));

    boolean result = featureToggleAspect.checkTblPropEnabled(proceedingJoinPoint, tblPropsEnabler);

    assertFalse(result);
    verify(proceedingJoinPoint, times(1)).proceed();
  }

  @Test
  void testCheckTblPropEnabled_FeatureOverturningDecision() throws Throwable {
    TableDto tableDto = TableDto.builder().tableId("testTable").databaseId("testDb").build();
    Object[] args = new Object[] {"testKey", tableDto};

    when(proceedingJoinPoint.getSignature()).thenReturn(methodSignature);
    when(methodSignature.getReturnType()).thenReturn(boolean.class);
    when(proceedingJoinPoint.getArgs()).thenReturn(args);
    when(proceedingJoinPoint.proceed())
        .thenReturn(false); /* original decision without feature toggle is false */
    when(tableFeatureToggle.isFeatureActivated("testDb", "testTable", "TEST_FEATURE"))
        .thenReturn(true);
    when(tblPropsToggleRegistry.obtainFeatureByKey("testKey"))
        .thenReturn(Optional.of("TEST_FEATURE"));

    boolean result = featureToggleAspect.checkTblPropEnabled(proceedingJoinPoint, tblPropsEnabler);

    assertTrue(result); /* evidence of overturning the original decision from feature toggle*/
    verify(proceedingJoinPoint, times(1)).proceed();
  }

  @Test
  void testCheckTblPropEnabled_WrongSignature() {
    when(proceedingJoinPoint.getSignature()).thenReturn(methodSignature);
    when(methodSignature.getReturnType()).thenReturn(String.class);

    assertThrows(
        RuntimeException.class,
        () -> featureToggleAspect.checkTblPropEnabled(proceedingJoinPoint, tblPropsEnabler));
  }

  @Test
  void testCheckTblPropEnabled_WrongArgType() {
    Object[] args = new Object[] {"testKey", "notTableDto"};

    when(proceedingJoinPoint.getSignature()).thenReturn(methodSignature);
    when(methodSignature.getReturnType()).thenReturn(boolean.class);
    when(proceedingJoinPoint.getArgs()).thenReturn(args);

    assertThrows(
        RuntimeException.class,
        () -> featureToggleAspect.checkTblPropEnabled(proceedingJoinPoint, tblPropsEnabler));
  }

  // Following test cases are for checkIcebergFeatureFlag

  @Test
  void testCheckIcebergFeatureFlag_FeatureActivated() {
    TableIdentifier tableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {tableIdentifier, "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);
    when(featureGate.value()).thenReturn("TEST_FEATURE");
    when(tableFeatureToggle.isFeatureActivated("namespace", "tableName", "TEST_FEATURE"))
        .thenReturn(true);

    assertThrows(
        ResourceGatedByToggledOnFeatureException.class,
        () -> featureToggleAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testCheckIcebergFeatureFlag_FeatureNotActivated() {
    TableIdentifier tableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {tableIdentifier, "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);
    when(featureGate.value()).thenReturn("TEST_FEATURE");
    when(tableFeatureToggle.isFeatureActivated("namespace", "tableName", "TEST_FEATURE"))
        .thenReturn(false);

    assertDoesNotThrow(() -> featureToggleAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testCheckIcebergFeatureFlag_NoTableIdentifier() {
    Object[] args = new Object[] {"notTableIdentifier", "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);

    assertThrows(
        RuntimeException.class,
        () -> featureToggleAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testCheckIcebergFeatureFlag_TableIdentifierNotFirst() {
    TableIdentifier tableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {"firstArg", tableIdentifier, "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);
    when(featureGate.value()).thenReturn("TEST_FEATURE");
    when(tableFeatureToggle.isFeatureActivated("namespace", "tableName", "TEST_FEATURE"))
        .thenReturn(false);

    assertDoesNotThrow(() -> featureToggleAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testObtainIdFromAnnotationArgs() {
    TableIdentifier expectedTableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {expectedTableIdentifier, "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);

    TableIdentifier result = featureToggleAspect.obtainIdFromAnnotationArgs(joinPoint);

    assertEquals(expectedTableIdentifier, result);
  }

  @Test
  void testObtainIdFromAnnotationNotFirstArgs() {
    TableIdentifier expectedTableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {"otherArg", expectedTableIdentifier};
    when(joinPoint.getArgs()).thenReturn(args);

    TableIdentifier result = featureToggleAspect.obtainIdFromAnnotationArgs(joinPoint);

    assertEquals(expectedTableIdentifier, result);
  }

  @Test
  void testObtainIdFromAnnotationArgs_NoTableIdentifier() {
    Object[] args = new Object[] {"notTableIdentifier", "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);

    assertThrows(
        RuntimeException.class, () -> featureToggleAspect.obtainIdFromAnnotationArgs(joinPoint));
  }
}
