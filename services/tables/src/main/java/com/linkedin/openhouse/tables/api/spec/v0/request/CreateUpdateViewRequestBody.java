package com.linkedin.openhouse.tables.api.spec.v0.request;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.ViewRepresentation;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Request body for POST and PUT on /v2/databases/{databaseId}/views. Nullable fields are omitted
 * from the serialized payload rather than emitted as JSON null, so an omitted {@code
 * baseViewVersion} on create stays absent on the wire.
 */
@Builder(toBuilder = true)
@EqualsAndHashCode
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateUpdateViewRequestBody {

  @Schema(
      description = "Unique Resource identifier for a view within a Database",
      example = "my_view")
  @NotEmpty(message = "viewId cannot be empty")
  @Size(max = 128)
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String viewId;

  @Schema(
      description = "Unique Resource identifier for the Database containing the View",
      example = "my_database")
  @NotEmpty(message = "databaseId cannot be empty")
  @Size(max = 128)
  @Pattern(regexp = ALPHA_NUM_UNDERSCORE_REGEX, message = ALPHA_NUM_UNDERSCORE_ERROR_MSG)
  private String databaseId;

  @Schema(
      description = "Unique Resource identifier for the Cluster containing the Database",
      example = "my_cluster")
  @NotEmpty(message = "clusterId cannot be empty")
  @Pattern(
      regexp = ALPHA_NUM_UNDERSCORE_REGEX_HYPHEN_ALLOW,
      message = ALPHA_NUM_UNDERSCORE_ERROR_MSG_HYPHEN_ALLOW)
  private String clusterId;

  @Schema(
      description = "Schema of the view. OpenHouse views use Iceberg schema specification",
      example =
          "{\"type\": \"struct\", "
              + "\"fields\": [{\"id\": 1,\"required\": true,\"name\": \"id\",\"type\": \"string\"}, "
              + "{\"id\": 2,\"required\": true,\"name\": \"name\",\"type\": \"string\"}]}")
  @NotEmpty(message = "schema cannot be empty")
  private String schema;

  @Schema(
      description = "Engine-specific representations of the view definition",
      example = "[{\"type\": \"sql\", \"sql\": \"SELECT 1\", \"dialect\": \"spark\"}]")
  @NotEmpty(message = "representations cannot be empty")
  @Valid
  private List<ViewRepresentation> representations;

  @Schema(description = "Dialect of the representation the view was authored in", example = "spark")
  @NotEmpty(message = "sourceDialect cannot be empty")
  private String sourceDialect;

  @Schema(
      nullable = true,
      description = "Catalog used to resolve unqualified identifiers in the view SQL",
      example = "openhouse")
  private String defaultCatalog;

  @Schema(
      nullable = true,
      description = "Namespace used to resolve unqualified identifiers in the view SQL",
      example = "[\"my_database\"]")
  private List<String> defaultNamespace;

  @Schema(nullable = true, description = "View properties", example = "{\"key\": \"value\"}")
  private Map<String, String> viewProperties;

  /**
   * Route-sensitive: absent or {@code INITIAL_VERSION} on create, and the current metadata pointer
   * on replace. Intentionally carries no bean constraint because the rule differs per HTTP verb and
   * is owned by the verb-aware view validator.
   */
  @Schema(
      nullable = true,
      description = "The version of the view that the current update is based upon")
  private String baseViewVersion;

  /**
   * Uses default Gson null handling rather than {@code serializeNulls()} so this stays consistent
   * with the class-level {@link JsonInclude.Include#NON_NULL}: an omitted nullable field is absent
   * from the payload, not present as JSON null.
   */
  public String toJson() {
    return new Gson().toJson(this);
  }
}
