package com.linkedin.openhouse.internal.catalog;

import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AbstractCommitStatsPublishOperationTest {

  private static final TableIdentifier TABLE = TableIdentifier.of("db", "tbl");

  /** Concrete subclass that records what it was asked to publish. */
  private static final class RecordingOperation extends AbstractCommitStatsPublishOperation {
    private CommitStats published;
    private int publishCount;

    @Override
    protected void publish(CommitStats stats) {
      this.published = stats;
      this.publishCount++;
    }
  }

  private static TableMetadata metadataWith(Map<String, String> properties) {
    TableMetadata md = mock(TableMetadata.class);
    when(md.properties()).thenReturn(properties);
    when(md.location()).thenReturn("s3://bucket/db/tbl");
    when(md.currentSnapshot()).thenReturn(null); // properties-only commit
    return md;
  }

  @Test
  void testPublishesWhenTablePropertyEnabledAndUuidPresent() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(getCanonicalFieldName("tableUUID"), "uuid-123");
    props.put(AbstractCommitStatsPublishOperation.COMMIT_STATS_COLLECTION_ENABLED_PROP, "true");

    RecordingOperation op = new RecordingOperation();
    op.execute(new PostCommitContext(TABLE, metadataWith(props)));

    Assertions.assertEquals(1, op.publishCount);
    Assertions.assertNotNull(op.published);
    Assertions.assertEquals("uuid-123", op.published.getTableUuid());
    Assertions.assertEquals("db", op.published.getDatabaseName());
    Assertions.assertEquals("tbl", op.published.getTableName());
  }

  @Test
  void testDoesNotPublishWhenTablePropertyAbsent() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(getCanonicalFieldName("tableUUID"), "uuid-123");
    // no opt-in property

    RecordingOperation op = new RecordingOperation();
    op.execute(new PostCommitContext(TABLE, metadataWith(props)));

    Assertions.assertEquals(0, op.publishCount);
  }

  @Test
  void testDoesNotPublishWhenTablePropertyFalse() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(getCanonicalFieldName("tableUUID"), "uuid-123");
    props.put(AbstractCommitStatsPublishOperation.COMMIT_STATS_COLLECTION_ENABLED_PROP, "false");

    RecordingOperation op = new RecordingOperation();
    op.execute(new PostCommitContext(TABLE, metadataWith(props)));

    Assertions.assertEquals(0, op.publishCount);
  }

  @Test
  void testDoesNotPublishWhenNoTableUuidEvenIfEnabled() throws Exception {
    Map<String, String> props = new HashMap<>();
    // enabled, but no stable UUID to key on -> factory returns empty
    props.put(AbstractCommitStatsPublishOperation.COMMIT_STATS_COLLECTION_ENABLED_PROP, "true");

    RecordingOperation op = new RecordingOperation();
    op.execute(new PostCommitContext(TABLE, metadataWith(props)));

    Assertions.assertEquals(0, op.publishCount);
  }

  @Test
  void testGetNameIsStable() {
    Assertions.assertEquals("commit-stats-publish", new RecordingOperation().getName());
  }
}
