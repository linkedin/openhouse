package com.linkedin.openhouse.tables.toggle;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.internal.catalog.toggle.IcebergFeatureGate;
import org.apache.iceberg.catalog.TableIdentifier;
import org.aspectj.lang.JoinPoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * This is a very much mock-ish test, leaving this in the same package as {@link FeatureGateAspect}
 * mainly for cases like {@link #testObtainIdFromAnnotationArgs()}
 */
class FeatureGateAspectTest {
  @Mock private TableFeatureToggle tableFeatureToggle;

  @Mock private JoinPoint joinPoint;

  @Mock private IcebergFeatureGate featureGate;

  @InjectMocks private FeatureGateAspect featureGateAspect;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

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
        () -> featureGateAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testCheckIcebergFeatureFlag_FeatureNotActivated() {
    TableIdentifier tableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {tableIdentifier, "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);
    when(featureGate.value()).thenReturn("TEST_FEATURE");
    when(tableFeatureToggle.isFeatureActivated("namespace", "tableName", "TEST_FEATURE"))
        .thenReturn(false);

    assertDoesNotThrow(() -> featureGateAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testCheckIcebergFeatureFlag_NoTableIdentifier() {
    Object[] args = new Object[] {"notTableIdentifier", "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);

    assertThrows(
        RuntimeException.class,
        () -> featureGateAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testCheckIcebergFeatureFlag_TableIdentifierNotFirst() {
    TableIdentifier tableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {"firstArg", tableIdentifier, "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);
    when(featureGate.value()).thenReturn("TEST_FEATURE");
    when(tableFeatureToggle.isFeatureActivated("namespace", "tableName", "TEST_FEATURE"))
        .thenReturn(false);

    assertDoesNotThrow(() -> featureGateAspect.checkIcebergFeatureFlag(joinPoint, featureGate));
  }

  @Test
  void testObtainIdFromAnnotationArgs() {
    TableIdentifier expectedTableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {expectedTableIdentifier, "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);

    TableIdentifier result = featureGateAspect.obtainIdFromAnnotationArgs(joinPoint);

    assertEquals(expectedTableIdentifier, result);
  }

  @Test
  void testObtainIdFromAnnotationNotFirstArgs() {
    TableIdentifier expectedTableIdentifier = TableIdentifier.of("namespace", "tableName");
    Object[] args = new Object[] {"otherArg", expectedTableIdentifier};
    when(joinPoint.getArgs()).thenReturn(args);

    TableIdentifier result = featureGateAspect.obtainIdFromAnnotationArgs(joinPoint);

    assertEquals(expectedTableIdentifier, result);
  }

  @Test
  void testObtainIdFromAnnotationArgs_NoTableIdentifier() {
    Object[] args = new Object[] {"notTableIdentifier", "otherArg"};
    when(joinPoint.getArgs()).thenReturn(args);

    assertThrows(
        RuntimeException.class, () -> featureGateAspect.obtainIdFromAnnotationArgs(joinPoint));
  }
}
