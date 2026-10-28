package com.linkedin.openhouse.tables.model;

import com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils;
import java.util.Collections;
import java.util.Set;

/**
 * The reserved, server-managed view properties: keys inside the {@code openhouse.} namespace that a
 * client may set once at create and may never alter afterwards.
 *
 * <p>This is the view-side counterpart of the table reserved-key space ({@link
 * HouseTableSerdeUtils} together with {@code PreservedKeyChecker}), kept deliberately separate: the
 * table machinery is bound to the {@code HouseTable} entity and {@code TableDto} and is the wrong
 * home for view keys. Only the bare {@code openhouse.} namespace string is shared with tables; the
 * reflection and predicate machinery around it in {@link HouseTableSerdeUtils} is table-specific
 * and is not reused.
 *
 * <p>It is the single source of truth for every reserved view key. Today it is consumed by {@code
 * OpenHouseViewsApiValidator}, which value-checks these keys and allow-lists the ones a client may
 * write at create; it is intended to also back the persistence layer's freeze-on-replace and
 * strip-on-read once views are persisted (tracked on {@code ViewsService#putView}), so every layer
 * agrees on what "reserved" means for a view. New {@code openhouse.view.*} properties are added
 * here, not inline in a validator or service.
 */
public final class ReservedViewProperties {

  private ReservedViewProperties() {}

  /**
   * The dialect the view was authored in. The client sets it once at create as a view property; the
   * server treats it as immutable thereafter. It is the view counterpart of a client-supplied, then
   * frozen, reserved table key such as {@code openhouse.tableUUID}.
   */
  public static final String SOURCE_DIALECT =
      HouseTableSerdeUtils.OPENHOUSE_NAMESPACE + "view.sourceDialect";

  /** The reserved keys a client is permitted to set, and only at create time. */
  private static final Set<String> WRITABLE_AT_CREATE = Collections.singleton(SOURCE_DIALECT);

  /**
   * Whether a client may set this reserved key when creating a view. Every other key in the {@code
   * openhouse.} namespace is server-owned and is rejected on the create surface.
   */
  public static boolean isWritableAtCreate(String key) {
    return WRITABLE_AT_CREATE.contains(key);
  }
}
