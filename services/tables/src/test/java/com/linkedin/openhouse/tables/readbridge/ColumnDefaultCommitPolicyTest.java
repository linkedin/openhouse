package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.common.exception.CommitRejectedException;
import com.linkedin.openhouse.common.exception.TableConfigUnavailableException;
import com.linkedin.openhouse.internal.catalog.PendingCommit;
import com.linkedin.openhouse.tables.config.CatalogRequestContext;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ColumnDefaultCommitPolicyTest {

  private static final String ENABLED = ReadBridgeConfigResolver.COLUMN_DEFAULTS_CAPABILITY_PROP;
  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "country", Types.StringType.get()));

  private static final CatalogRequestContext NO_BYPASS = new CatalogRequestContext() {};

  /** Every opted-in table has a default on field 2. */
  private static final ColumnDefaultsSource FIELD_2 =
      tableDto -> Collections.singletonMap(2, TextNode.valueOf("US"));

  /** No table has a default, so only the opt-in lock can reject. */
  private static final ColumnDefaultsSource NO_DEFAULTS = tableDto -> Collections.emptyMap();

  private static final ColumnDefaultsSource NEVER_ASKED =
      tableDto -> {
        throw new IllegalStateException("looked up defaults for a commit the rules skip");
      };

  private static final CatalogRequestContext BYPASS =
      new CatalogRequestContext() {
        @Override
        public boolean columnDefaultPolicyBypass() {
          return true;
        }
      };

  @ParameterizedTest
  @MethodSource("changesToCommittedTrue")
  void optInCannotBeChangedOrRemovedOnceTrue(String committed, String proposed) {
    CommitRejectedException error =
        Assertions.assertThrows(
            CommitRejectedException.class,
            () -> policy(NO_BYPASS).check(commit(table(committed), optIn(proposed))));
    Assertions.assertTrue(error.getMessage().contains(ENABLED));
    Assertions.assertTrue(error.getMessage().contains("immutable once set to true"));
  }

  static Stream<Arguments> changesToCommittedTrue() {
    return Stream.of(
        Arguments.of("true", "false"),
        Arguments.of("true", null),
        Arguments.of("true", "TRUE"),
        Arguments.of("TRUE", "false"),
        Arguments.of(" True ", "false"));
  }

  @ParameterizedTest
  @MethodSource("changesToCommittedFalseOrInvalid")
  void optInFalseOrInvalidCanBeChangedOrRemoved(String committed, String proposed) {
    Assertions.assertDoesNotThrow(
        () -> policy(NO_BYPASS).check(commit(table(committed), optIn(proposed))));
  }

  static Stream<Arguments> changesToCommittedFalseOrInvalid() {
    return Stream.of(
        Arguments.of("false", "true"),
        Arguments.of("false", null),
        Arguments.of("false", "FALSE"),
        Arguments.of("invalid", "true"),
        Arguments.of("invalid", null));
  }

  @ParameterizedTest
  @MethodSource("firstAssignmentsAndUnchangedValues")
  void optInAllowsFirstAssignmentAndUnchangedValues(TableMetadata base, String proposed) {
    Assertions.assertDoesNotThrow(() -> policy(NO_BYPASS).check(commit(base, optIn(proposed))));
  }

  static Stream<Arguments> firstAssignmentsAndUnchangedValues() {
    Named<TableMetadata> create = Named.of("create", null);
    Named<TableMetadata> withoutOptIn = Named.of("existing table", table(null));
    return Stream.of(
        Arguments.of(create, "true"),
        Arguments.of(create, "false"),
        Arguments.of(withoutOptIn, "true"),
        Arguments.of(withoutOptIn, "false"),
        Arguments.of(withoutOptIn, null),
        Arguments.of(Named.of("committed true", table("true")), "true"),
        Arguments.of(Named.of("committed false", table("false")), "false"));
  }

  /** The bypass lifts the opt-in lock and skips the rules, default lookups included. */
  @Test
  void bypassSkipsTheLockAndTheRules() {
    Assertions.assertDoesNotThrow(
        () -> policy(BYPASS, NEVER_ASKED).check(commit(table("true"), optIn(null))));
  }

  /**
   * Type 2 has no capabilities to read outside a request, so a snapshot commit there is not
   * default-aware.
   */
  @Test
  void snapshotCommitOutsideARequestBreaksType2() {
    CommitRejectedException error =
        Assertions.assertThrows(
            CommitRejectedException.class,
            () -> policy(NO_BYPASS, FIELD_2).check(commit(table("true"), optIn("true"))));
    Assertions.assertTrue(
        error
            .getMessage()
            .contains(
                "which this client does not declare (X-Client-Name: (none), User-Agent: (none),"
                    + " X-OpenHouse-Client-Capabilities: (none))"),
        error.getMessage());
  }

  /** Type 2 accepts exactly the clients that declare the {@code column-defaults} capability. */
  @Test
  void snapshotCommitFromDeclaringClientPassesType2() {
    CatalogRequestContext declaring =
        new CatalogRequestContext() {
          @Override
          public Set<String> clientCapabilities() {
            return Collections.singleton(ClientCapabilityGate.COLUMN_DEFAULTS);
          }
        };

    Assertions.assertDoesNotThrow(
        () -> policy(declaring, FIELD_2).check(commit(table("true"), optIn("true"))));
  }

  /** A default the table already declares but cannot apply stays the server's error. */
  @Test
  void storedUnusableDefaultIsNotAClientError() {
    ColumnDefaultsSource unusable =
        tableDto -> {
          throw new ColumnDefaultException("column country: default is not a string");
        };

    Assertions.assertThrows(
        TableConfigUnavailableException.class,
        () -> policy(NO_BYPASS, unusable).check(commit(table("true"), optIn("true"))));
  }

  private static ColumnDefaultCommitPolicy policy(CatalogRequestContext requestContext) {
    return policy(requestContext, NO_DEFAULTS);
  }

  private static ColumnDefaultCommitPolicy policy(
      CatalogRequestContext requestContext, ColumnDefaultsSource source) {
    return new ColumnDefaultCommitPolicy(
        requestContext,
        new ClientCapabilityGate(requestContext),
        new ReadBridgeStripProtection(new ReadBridgeConfigResolver(source)));
  }

  /** The table with {@code optIn} committed as its opt-in, or without one when null. */
  private static TableMetadata table(String optIn) {
    return TableMetadata.newTableMetadata(
        SCHEMA, PartitionSpec.unpartitioned(), "file:/data/openhouse/db/tbl", optIn(optIn));
  }

  /** Properties carrying {@code value} as the opt-in, or none when null. */
  private static Map<String, String> optIn(String value) {
    return value == null ? Collections.emptyMap() : Collections.singletonMap(ENABLED, value);
  }

  /**
   * A snapshot commit to {@code base}, or creating the table when null, that leaves {@code
   * properties}.
   */
  private static PendingCommit commit(TableMetadata base, Map<String, String> properties) {
    return new PendingCommit(
        TABLE,
        Optional.ofNullable(base),
        TableMetadata.newTableMetadata(
            SCHEMA, PartitionSpec.unpartitioned(), "file:/data/openhouse/db/tbl", properties),
        ImmutableMap.copyOf(properties),
        true);
  }
}
