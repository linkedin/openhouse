package com.linkedin.openhouse.tables.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.tables.model.DatabaseDto;
import com.linkedin.openhouse.tables.model.TableDto;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

/**
 * OpenHouse Authorization Handler leverages OpaHandler to interface with OPA for authorization
 * grants, revokes, and checks.
 */
@Slf4j
public class OpaHandler {
  private static final String CHECK_ACCESS_ENDPOINT = "/v1/data/openhouse/authorization";

  private static final String USER_ROLES_ENDPOINT = "/v1/data/user_roles";

  private WebClient webClient;

  /** Instantiates OpaHandler */
  public OpaHandler(String baseURI) {
    this.webClient = createWebClient(baseURI);
  }

  private WebClient createWebClient(String baseURI) {
    HttpClient httpClient = HttpClient.newConnection();
    ClientHttpConnector connector = new ReactorClientHttpConnector(httpClient);
    return WebClient.builder().baseUrl(baseURI).clientConnector(connector).build();
  }

  /**
   * Check if the principal has privilege on the table.
   *
   * @param principal
   * @param tableDto
   * @param privilege
   * @return true, if the principal has the privilege, false otherwise
   */
  public boolean checkAccessDecision(String principal, TableDto tableDto, Privileges privilege) {
    String requestBody =
        buildCheckAccessDecisionRequestBody(
            principal, privilege.toString(), tableDto.getDatabaseId(), tableDto.getTableId());

    return makeCheckAccessDecisionRequest(requestBody);
  }

  /**
   * Check if the principal has privilege on the database.
   *
   * @param principal
   * @param databaseDto
   * @param privilege
   * @return true, if the principal has the privilege, false otherwise
   */
  public boolean checkAccessDecision(
      String principal, DatabaseDto databaseDto, Privileges privilege) {
    String requestBody =
        buildCheckAccessDecisionRequestBody(
            principal, privilege.toString(), databaseDto.getDatabaseId());

    return makeCheckAccessDecisionRequest(requestBody);
  }

  /**
   * Grant a role to a principal on the table.
   *
   * @param role
   * @param principal
   * @param tableDto
   */
  public void grantRole(String role, String principal, TableDto tableDto) {
    String userRoleEndpoint =
        String.format(
            USER_ROLES_ENDPOINT + "/%s/%s/%s",
            tableDto.getDatabaseId(),
            tableDto.getTableId(),
            principal);
    String requestBody = buildGrantRevokeRequestBody(tableDto, principal, role, true);

    makeGrantRevokeRoleRequest(requestBody, userRoleEndpoint);
  }

  /**
   * Grant a role to a principal on the database.
   *
   * @param role
   * @param principal
   * @param databaseDto
   */
  public void grantRole(String role, String principal, DatabaseDto databaseDto) {
    String userRoleEndpoint =
        String.format(USER_ROLES_ENDPOINT + "/%s/%s", databaseDto.getDatabaseId(), principal);
    String requestBody = buildGrantRevokeRequestBody(databaseDto, principal, role, true);

    makeGrantRevokeRoleRequest(requestBody, userRoleEndpoint);
  }

  /**
   * Revoke a role from a principal on the table.
   *
   * @param role
   * @param principal
   * @param tableDto
   */
  public void revokeRole(String role, String principal, TableDto tableDto) {
    String userRoleEndpoint =
        String.format(
            USER_ROLES_ENDPOINT + "/%s/%s/%s",
            tableDto.getDatabaseId(),
            tableDto.getTableId(),
            principal);
    String requestBody = buildGrantRevokeRequestBody(tableDto, principal, role, false);
    makeGrantRevokeRoleRequest(requestBody, userRoleEndpoint);
  }

  /**
   * Revoke a role from a principal on the database.
   *
   * @param role
   * @param principal
   * @param databaseDto
   */
  public void revokeRole(String role, String principal, DatabaseDto databaseDto) {
    String userRoleEndpoint =
        String.format(USER_ROLES_ENDPOINT + "/%s/%s", databaseDto.getDatabaseId(), principal);
    String requestBody = buildGrantRevokeRequestBody(databaseDto, principal, role, false);
    makeGrantRevokeRoleRequest(requestBody, userRoleEndpoint);
  }

  private String buildGrantRevokeRequestBody(
      TableDto tableDto, String principal, String role, boolean isGrant) {
    List<String> currentRoles =
        getRolesForPrincipalOnResource(tableDto.getDatabaseId(), tableDto.getTableId(), principal);

    if (isGrant) {
      if (!currentRoles.contains(role)) {
        currentRoles.add(role);
      }
    } else {
      if (currentRoles.contains(role)) {
        currentRoles.remove(role);
      }
    }

    JsonObject body = new JsonObject();
    body.add("roles", new Gson().toJsonTree(currentRoles));
    return body.toString();
  }

  private String buildGrantRevokeRequestBody(
      DatabaseDto databaseDto, String principal, String role, boolean isGrant) {
    List<String> currentRoles =
        getRolesForPrincipalOnResource(databaseDto.getDatabaseId(), principal);

    if (isGrant) {
      if (!currentRoles.contains(role)) {
        currentRoles.add(role);
      }
    } else {
      if (currentRoles.contains(role)) {
        currentRoles.remove(role);
      }
    }

    JsonObject body = new JsonObject();
    body.add("roles", new Gson().toJsonTree(currentRoles));
    return body.toString();
  }

  public List<String> getRolesForPrincipalOnResource(
      String databaseId, String tableId, String principal) {
    String endpoint =
        String.format(USER_ROLES_ENDPOINT + "/%s/%s/%s/roles", databaseId, tableId, principal);

    JsonNode userRoles =
        webClient.get().uri(endpoint).retrieve().bodyToMono(JsonNode.class).block();

    if (userRoles == null || userRoles.isEmpty()) {
      return new ArrayList<>();
    }

    List<String> currentRoles = new ArrayList<>();
    userRoles.get("result").forEach(n -> currentRoles.add(n.asText()));
    return currentRoles;
  }

  public Map<String, List<String>> getAllRolesOnResource(String databaseId, String tableId) {
    String endpoint = String.format(USER_ROLES_ENDPOINT + "/%s/%s", databaseId, tableId);

    JsonNode userRoles =
        webClient.get().uri(endpoint).retrieve().bodyToMono(JsonNode.class).block();

    if (userRoles == null || userRoles.isEmpty()) {
      return new HashMap<>();
    }

    Map<String, List<String>> currentRoles = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> resultEntries = userRoles.fields();
    JsonNode resultNode = resultEntries.next().getValue();

    resultNode
        .fields()
        .forEachRemaining(
            entry -> {
              log.info("entry: {}", entry);
              String principal = entry.getKey();
              JsonNode userRolesNode = entry.getValue();
              log.info("entry getValue: {}", entry.getValue());
              JsonNode rolesNode = userRolesNode.path("roles");
              if (rolesNode != null && rolesNode.isArray()) {
                List<String> roles = new ArrayList<>();
                rolesNode.forEach(r -> roles.add(r.asText()));
                currentRoles.put(principal, roles);
              }
            });
    return currentRoles;
  }

  public Map<String, List<String>> getAllRolesOnResource(String databaseId) {
    String endpoint = String.format(USER_ROLES_ENDPOINT + "/%s", databaseId);

    JsonNode userRoles =
        webClient.get().uri(endpoint).retrieve().bodyToMono(JsonNode.class).block();

    if (userRoles == null || userRoles.isEmpty()) {
      return new HashMap<>();
    }

    Map<String, List<String>> currentRoles = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> resultEntries = userRoles.fields();
    JsonNode resultNode = resultEntries.next().getValue();
    resultNode
        .fields()
        .forEachRemaining(
            entry -> {
              log.info("entry: {}", entry);
              String principal = entry.getKey();

              JsonNode userRolesNode = entry.getValue();
              log.info("entry getValue: {}", entry.getValue());
              JsonNode rolesNode = userRolesNode.path("roles");
              if (rolesNode != null && rolesNode.isArray()) {
                List<String> roles = new ArrayList<>();
                rolesNode.forEach(r -> roles.add(r.asText()));
                currentRoles.put(principal, roles);
              }
            });
    return currentRoles;
  }

  private List<String> getRolesForPrincipalOnResource(String databaseId, String principal) {
    String endpoint = String.format(USER_ROLES_ENDPOINT + "/%s/%s/roles", databaseId, principal);

    JsonNode userRoles =
        webClient.get().uri(endpoint).retrieve().bodyToMono(JsonNode.class).block();

    if (userRoles == null || userRoles.isEmpty()) {
      return new ArrayList<>();
    }

    List<String> currentRoles = new ArrayList<>();
    userRoles.get("result").forEach(n -> currentRoles.add(n.asText()));
    return currentRoles;
  }

  private void makeGrantRevokeRoleRequest(String requestBody, String endpoint) {
    webClient
        .put()
        .uri(endpoint)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(requestBody)
        .retrieve()
        .bodyToMono(String.class)
        .onErrorResume(WebClientResponseException.class, e -> Mono.empty())
        .block();
  }

  private boolean makeCheckAccessDecisionRequest(String requestBody) {
    JsonNode jsonNode =
        webClient
            .post()
            .uri(CHECK_ACCESS_ENDPOINT)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(requestBody)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(node -> node.path("result").path("allow"))
            .onErrorResume(WebClientResponseException.class, e -> Mono.empty())
            .block();

    if (jsonNode != null) {
      return jsonNode.asBoolean();
    }
    return false;
  }

  private String buildCheckAccessDecisionRequestBody(
      String principal, String privilege, String dbId, String tbId) {
    Map<String, String> userPrivilege = new HashMap<>();
    userPrivilege.put("user", principal);
    userPrivilege.put("privilege_to_check", privilege);
    userPrivilege.put("db_id", dbId);
    userPrivilege.put("tbl_id", tbId);

    JsonObject body = new JsonObject();
    body.add("input", new Gson().toJsonTree(userPrivilege));
    return body.toString();
  }

  private String buildCheckAccessDecisionRequestBody(
      String principal, String privilege, String dbId) {
    Map<String, String> userPrivilege = new HashMap<>();
    userPrivilege.put("user", principal);
    userPrivilege.put("privilege_to_check", privilege);
    userPrivilege.put("db_id", dbId);

    JsonObject body = new JsonObject();
    body.add("input", new Gson().toJsonTree(userPrivilege));
    return body.toString();
  }

  private List<String> extractRoles(JsonNode userNode) {
    JsonNode rolesNode = userNode.path("roles");
    if (rolesNode != null && rolesNode.isArray()) {
      log.info("rolesNode not empty: {}", rolesNode);
      List<String> roles = new ArrayList<>();
      for (JsonNode roleNode : rolesNode) {
        roles.add(roleNode.asText());
      }
      return roles;
    } else {
      // Handle the case where "roles" is missing or not an array
      return Collections.emptyList();
    }
  }
}
