package com.linkedin.openhouse.internal.catalog.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.internal.catalog.model.MetadataUpdateResult;
import com.linkedin.openhouse.internal.catalog.model.SnapshotRefChange;
import com.linkedin.openhouse.internal.catalog.model.SnapshotRefChange.RefState;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.BadRequestException;

/** Applies an authoritative, ordered table update list without publishing intermediate metadata. */
public final class MetadataUpdateCommit {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private MetadataUpdateCommit() {}

  /**
   * Applies every update to one builder and returns the completed metadata and its ref transitions.
   * A null base denotes creation; a null update list is not an explicit transaction.
   */
  public static MetadataUpdateResult apply(TableMetadata base, List<Map<String, Object>> updates) {
    return apply(base, updates, UnaryOperator.identity());
  }

  /** Transforms added schemas before applying the ordered updates to a single native builder. */
  public static MetadataUpdateResult apply(
      TableMetadata base,
      List<Map<String, Object>> updates,
      UnaryOperator<Schema> schemaTransform) {
    if (updates == null) {
      throw new BadRequestException("An explicit metadata update list is required");
    }

    // Bind maps directly to JSON nodes so integral IDs are not coerced through floating point.
    List<MetadataUpdate> parsed = new ArrayList<>(updates.size());
    for (int index = 0; index < updates.size(); index++) {
      try {
        JsonNode node = MAPPER.valueToTree(updates.get(index));
        MetadataUpdate update = MetadataUpdateParser.fromJson(node);
        if (update instanceof MetadataUpdate.AddViewVersion
            || update instanceof MetadataUpdate.SetCurrentViewVersion) {
          throw new UnsupportedOperationException("View updates cannot be applied to a table");
        }
        if (update instanceof MetadataUpdate.AddSchema) {
          MetadataUpdate.AddSchema addSchema = (MetadataUpdate.AddSchema) update;
          Schema transformed =
              Objects.requireNonNull(
                  schemaTransform.apply(addSchema.schema()),
                  "Schema transform must not return null");
          if (transformed != addSchema.schema()) {
            update = new MetadataUpdate.AddSchema(transformed, addSchema.lastColumnId());
          }
        }
        parsed.add(update);
      } catch (RuntimeException e) {
        throw new BadRequestException(
            e, "Invalid metadata update at index %s: %s", index, e.getMessage());
      }
    }

    TableMetadata.Builder builder =
        base == null ? TableMetadata.buildFromEmpty() : TableMetadata.buildFrom(base);
    if (base == null) {
      for (int index = 0; index < parsed.size(); index++) {
        MetadataUpdate update = parsed.get(index);
        if (update instanceof MetadataUpdate.UpgradeFormatVersion) {
          int version = ((MetadataUpdate.UpgradeFormatVersion) update).formatVersion();
          if (version == 1) {
            CreationFormatVersion.set(builder, index);
          } else {
            try {
              builder.upgradeFormatVersion(version);
            } catch (IllegalArgumentException e) {
              throw new BadRequestException(
                  e, "Invalid metadata update at index %s: %s", index, e.getMessage());
            }
          }
          break;
        }
      }
    }
    // The builder does not expose refs. Mirror only successful native ref mutations, never build
    // intermediate metadata: doing so would change snapshot history and last-added ID semantics.
    // Sorted keys make the multiple implicit removals caused by one remove-snapshots deterministic.
    Map<String, RefState> refs = new TreeMap<>();
    if (base != null) {
      base.refs().forEach((name, ref) -> refs.put(name, state(ref)));
    }
    List<SnapshotRefChange> refChanges = new ArrayList<>();
    for (int index = 0; index < parsed.size(); index++) {
      MetadataUpdate update = parsed.get(index);
      try {
        update.applyTo(builder);
        recordRefChanges(update, index, refs, refChanges);
      } catch (RuntimeException e) {
        throw new BadRequestException(
            e, "Invalid metadata update at index %s: %s", index, e.getMessage());
      }
    }

    try {
      TableMetadata metadata = builder.build();
      if (metadata == null) {
        throw new IllegalArgumentException("Creation requires table metadata updates");
      }
      return new MetadataUpdateResult(metadata, refChanges);
    } catch (RuntimeException e) {
      throw new BadRequestException(e, "Invalid metadata update transaction: %s", e.getMessage());
    }
  }

  /**
   * Iceberg 1.5.2.17 defaults empty builders to v2, and its public upgrade API cannot initialize
   * v1. Use the same private setter as native newTableMetadata only for explicit v1 creation,
   * before applying updates. Other versions and existing tables use the public upgrade API. An
   * incompatible dependency must fail rather than silently upgrade a requested v1.
   */
  private static final class CreationFormatVersion {
    private static final Method SET_INITIAL_FORMAT_VERSION = initialFormatVersionSetter();

    private static Method initialFormatVersionSetter() {
      try {
        Method method =
            TableMetadata.Builder.class.getDeclaredMethod("setInitialFormatVersion", int.class);
        method.setAccessible(true);
        return method;
      } catch (ReflectiveOperationException | RuntimeException e) {
        throw new IllegalStateException(
            "Iceberg creation requires accessible Builder.setInitialFormatVersion(int)", e);
      }
    }

    private static void set(TableMetadata.Builder builder, int index) {
      try {
        SET_INITIAL_FORMAT_VERSION.invoke(builder, 1);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IllegalArgumentException) {
          throw new BadRequestException(
              cause, "Invalid metadata update at index %s: %s", index, cause.getMessage());
        }
        throw new IllegalStateException("Iceberg creation format initialization failed", cause);
      } catch (IllegalAccessException | IllegalArgumentException e) {
        throw new IllegalStateException(
            "Cannot invoke Iceberg Builder.setInitialFormatVersion(int)", e);
      }
    }
  }

  private static void recordRefChanges(
      MetadataUpdate update,
      int index,
      Map<String, RefState> refs,
      List<SnapshotRefChange> changes) {
    if (update instanceof MetadataUpdate.SetSnapshotRef) {
      MetadataUpdate.SetSnapshotRef set = (MetadataUpdate.SetSnapshotRef) update;
      RefState after =
          RefState.builder()
              .snapshotId(set.snapshotId())
              .type(set.type().toLowerCase(Locale.ROOT))
              .minSnapshotsToKeep(set.minSnapshotsToKeep())
              .maxSnapshotAgeMs(set.maxSnapshotAgeMs())
              .maxRefAgeMs(set.maxRefAgeMs())
              .build();
      RefState before = refs.put(set.name(), after);
      recordChange(changes, index, "set-snapshot-ref", set.name(), before, after);
    } else if (update instanceof MetadataUpdate.RemoveSnapshotRef) {
      String name = ((MetadataUpdate.RemoveSnapshotRef) update).name();
      recordChange(changes, index, "remove-snapshot-ref", name, refs.remove(name), null);
    } else if (update instanceof MetadataUpdate.RemoveSnapshot) {
      long removedId = ((MetadataUpdate.RemoveSnapshot) update).snapshotId();
      Iterator<Map.Entry<String, RefState>> iterator = refs.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, RefState> entry = iterator.next();
        if (entry.getValue().getSnapshotId() == removedId) {
          recordChange(changes, index, "remove-snapshots", entry.getKey(), entry.getValue(), null);
          iterator.remove();
        }
      }
    }
  }

  private static void recordChange(
      List<SnapshotRefChange> changes,
      int index,
      String action,
      String name,
      RefState before,
      RefState after) {
    if (!Objects.equals(before, after)) {
      changes.add(
          SnapshotRefChange.builder()
              .updateIndex(index)
              .action(action)
              .refName(name)
              .before(before)
              .after(after)
              .build());
    }
  }

  private static RefState state(SnapshotRef ref) {
    return RefState.builder()
        .snapshotId(ref.snapshotId())
        .type(ref.isBranch() ? "branch" : "tag")
        .minSnapshotsToKeep(ref.minSnapshotsToKeep())
        .maxSnapshotAgeMs(ref.maxSnapshotAgeMs())
        .maxRefAgeMs(ref.maxRefAgeMs())
        .build();
  }
}
