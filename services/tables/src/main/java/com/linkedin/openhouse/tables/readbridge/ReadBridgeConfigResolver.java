package com.linkedin.openhouse.tables.readbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.toggle.TableFeatureToggle;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Open-source encoder for the {@code read-bridge} feature: for a table the ramp has activated, it
 * asks the pluggable {@link ColumnDefaultsSource} for that table's column initial-defaults and
 * stamps each as a namespaced entry in the per-table {@code config} — {@code
 * openhouse.read-bridge.column-default.<fieldId> = <single-value-json>}. The client decoder ({@code
 * ReadBridge} in {@code openhouse-java-runtime}) reads these entries and overlays the defaults at
 * metadata-load time.
 *
 * <p>No envelope/POJO: the flat config map (Iceberg REST {@code LoadTableResponse.config}
 * convention) carries the structure directly.
 *
 * <h3>Who decides what</h3>
 *
 * This class owns the <em>policy</em> — the feature id, the ramp, and the wire keys — and a
 * deployment supplies only the <em>data</em>, via {@link ColumnDefaultsSource}. Keeping the ramp
 * here means every deployment inherits it, the self-service property {@code
 * read-bridge.column-default.enabled} is documented alongside the {@code openhouse.read-bridge.*}
 * keys it controls, and a deployment's source is never asked to derive defaults for a table that is
 * not bridged.
 *
 * <h3>What capabilities share, and what they don't</h3>
 *
 * Capabilities bridged through this class share the <em>infrastructure</em> and nothing else: the
 * per-table {@code config} channel, the {@code openhouse.read-bridge.*} namespace, and the client's
 * decode/apply path. Rollout is never shared. Each capability has its own source, feature id, and
 * self-service table property, so it can be ramped, paused or killed without touching any other.
 * There is deliberately no single switch, toggle id or property meaning "all of read-bridge".
 *
 * <p>{@link #resolve(TableDto)} therefore contains no cross-capability gate at all — it only merges
 * what each capability decided for itself. A shared short-circuit there would couple rollouts that
 * are meant to be independent: a deployment supplying a deletion-vector source but no
 * column-default source must still get deletion vectors.
 *
 * <h3>Combining rollouts later</h3>
 *
 * Independence is the default, not the ceiling. A superset ramp — say {@code v3-read-bridge},
 * activating every capability at once for tables that want the whole V3 read surface — is a natural
 * later addition, and nothing here blocks it: it would be one more feature id consulted alongside
 * the capability's own. The mechanism-wide ids ({@code read-bridge}, {@code v3-read-bridge}) are
 * left unused today so one of them can take that role without colliding with a capability. It will
 * need an explicit precedence rule; the sane one is that the more specific wins, so a table setting
 * {@code read-bridge.column-default.enabled=false} stays opted out of that capability even while
 * opted into the superset.
 *
 * <h3>Gating, cheapest check first (per capability)</h3>
 *
 * <ol>
 *   <li>No source supplied for the capability ({@link ColumnDefaultsSource#NONE}) — structurally
 *       inert, and notably makes no toggle lookup, so open-source and dev deployments add nothing
 *       to the table-load path.
 *   <li>{@link TableFeatureToggle#isFeatureActivatedWithOverride} — the per-table ramp: an explicit
 *       {@code read-bridge.column-default.enabled} table property opts a table in or out without a
 *       HouseTables call; when absent, the server-managed toggle decides via an exact {@code
 *       (databaseId, tableId, featureId)} lookup ({@link
 *       com.linkedin.openhouse.tables.toggle.BaseTableFeatureToggle}). There is no glob / {@code *}
 *       matcher today — fleet ramp means writing {@code ACTIVE} rows (or setting the table
 *       property) per table. That HTS row (or the property) is the kill switch; there is
 *       deliberately no cluster property duplicating it, which would only add a second place to
 *       look and a slower one, since it would need a redeploy to change.
 * </ol>
 *
 * <h3>The toggle is on the read path, so it fails open</h3>
 *
 * Consulting the ramp here can put a blocking HouseTables call on table load when the self-service
 * property is absent — a path toggles are not otherwise on (elsewhere they gate writes and
 * table-property changes). A HouseTables blip must therefore not fail table reads, so a lookup
 * failure is logged and treated as "not bridged".
 *
 * <p>That is safe for exactly the same reason old clients may ignore unknown keys: not bridging
 * leaves the reader at today's behavior. The two are the same property of a capability, used twice.
 * A capability where ignoring is unsafe — deletion vectors, where skipping means returning deleted
 * rows — must NOT reuse this fail-open block; for those, a lookup failure has to fail the read,
 * because serving data that is silently wrong is worse than serving an error.
 *
 * <p>The override-honoring form is the correct one here and should stay that way: read-bridge is a
 * rollout, not an authorization gate. Features that decide whether a user may write a preserved
 * property must keep using the server-only {@code isFeatureActivated}, because the table property
 * this form honors is writable by the very user being gated.
 *
 * <h3>Adding a capability</h3>
 *
 * Add a source interface, a {@code <capability>Config} method that owns its own source check, kill
 * switch, ramp and keys, and one merge line in {@link #resolve(TableDto)}. Nothing in the existing
 * capability changes. Deliberately not generalised into a capability registry yet: with a single
 * implementation that interface would be a guess.
 *
 * <p><b>Ignoring is not always safe.</b> The client ignores config keys it does not recognise, so a
 * capability may only be bridged this way if ignoring it leaves the client at today's behavior.
 * That holds for column defaults — an old client reads {@code NULL}, exactly as it does now. It
 * would NOT hold for something like deletion vectors, where ignoring the key means returning
 * deleted rows: a silent correctness violation rather than a missed improvement. A capability of
 * that kind cannot rely on the ignore rule and must not be stamped for a client too old to honor
 * it, which means gating on the client version advertised in the {@code User-Agent} header.
 *
 * <p><b>Mirror:</b> {@link #COLUMN_DEFAULT_PREFIX} is the shared contract with the client decoder;
 * keep it in sync.
 */
@Slf4j
public class ReadBridgeConfigResolver {

  /**
   * Feature id for the column-default capability's ramp. Also names its self-service table property
   * ({@code read-bridge.column-default.enabled}) and its config keys, below.
   *
   * <p>Per capability, NOT one id for all of read-bridge. Capabilities bridged through this
   * namespace differ wildly in risk and readiness — deletion vectors must be rampable separately
   * from column defaults, not dragged along by them. The id is also baked into a user-facing table
   * property, so splitting it later means migrating properties customers have already set; it costs
   * nothing to get right while nothing is ramped. The bare {@code read-bridge} id is left free for
   * a future superset ramp.
   */
  public static final String COLUMN_DEFAULT_FEATURE_ID = "read-bridge.column-default";

  /**
   * Config key prefix for a per-column read-time default; suffixed with the Iceberg field-id.
   * Derived from the feature id so the ramp, the property and the wire keys cannot drift apart.
   */
  public static final String COLUMN_DEFAULT_PREFIX = "openhouse." + COLUMN_DEFAULT_FEATURE_ID + ".";

  private final ColumnDefaultsSource columnDefaultsSource;

  private final TableFeatureToggle featureToggle;

  public ReadBridgeConfigResolver(
      ColumnDefaultsSource columnDefaultsSource, TableFeatureToggle featureToggle) {
    this.columnDefaultsSource = columnDefaultsSource;
    this.featureToggle = featureToggle;
  }

  /**
   * Resolves the per-table client {@code config} for {@code tableDto}, empty when nothing is
   * bridged. Purely a merge of independently-gated capabilities; see the class javadoc for why
   * there is no shared gate here.
   *
   * <p>Takes the DTO alone: it already carries the database and table ids, and passing them
   * separately invites the call sites to disagree about where they came from.
   */
  public Map<String, String> resolve(TableDto tableDto) {
    Map<String, String> config = new HashMap<>();
    config.putAll(columnDefaultConfig(tableDto));
    return config;
  }

  /** The column-default capability: its own source, ramp and keys. */
  private Map<String, String> columnDefaultConfig(TableDto tableDto) {
    if (columnDefaultsSource == ColumnDefaultsSource.NONE) {
      // Nothing can be bridged for this capability: skip the toggle lookup, which is a remote
      // HouseTables call on the table-load path.
      return Collections.emptyMap();
    }
    if (!isColumnDefaultRamped(tableDto)) {
      return Collections.emptyMap(); // not ramped for this table -> stamp nothing
    }
    Map<Integer, JsonNode> columnDefaults = columnDefaultsSource.defaults(tableDto);
    if (columnDefaults == null || columnDefaults.isEmpty()) {
      return Collections.emptyMap(); // nothing to bridge -> stamp nothing
    }
    Map<String, String> config = new HashMap<>();
    // JsonNode.toString() is the single-value JSON (e.g. "US" -> "\"US\"", 0 -> "0").
    columnDefaults.forEach(
        (fieldId, value) -> config.put(COLUMN_DEFAULT_PREFIX + fieldId, value.toString()));
    return config;
  }

  /**
   * Whether the column-default capability is ramped for this table, failing open on a toggle-lookup
   * failure so a HouseTables blip degrades bridging rather than failing the read. Safe only because
   * not bridging is today's behavior; see the class javadoc.
   */
  private boolean isColumnDefaultRamped(TableDto tableDto) {
    try {
      return featureToggle.isFeatureActivatedWithOverride(tableDto, COLUMN_DEFAULT_FEATURE_ID);
    } catch (RuntimeException e) {
      log.warn(
          "read-bridge: toggle lookup failed for {}.{}; treating {} as not ramped",
          tableDto.getDatabaseId(),
          tableDto.getTableId(),
          COLUMN_DEFAULT_FEATURE_ID,
          e);
      return false;
    }
  }
}
