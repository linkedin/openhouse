package com.linkedin.openhouse.optimizer.api.mapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.optimizer.api.model.HistoryStatus;
import com.linkedin.openhouse.optimizer.api.model.JobResult;
import com.linkedin.openhouse.optimizer.api.model.OperationStatus;
import com.linkedin.openhouse.optimizer.api.model.OperationType;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsDto;
import com.linkedin.openhouse.optimizer.api.model.TableOperationsHistoryDto;
import com.linkedin.openhouse.optimizer.api.model.TableStatsDto;
import com.linkedin.openhouse.optimizer.api.model.TableStatsHistoryDto;
import com.linkedin.openhouse.optimizer.entity.TableOperationsHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableOperationsRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsHistoryRow;
import com.linkedin.openhouse.optimizer.entity.TableStatsRow;
import org.mapstruct.Mapper;

/**
 * MapStruct mapper for converting between optimizer JPA entities and their corresponding DTOs.
 *
 * <p>Spring-instantiated at compile time. Inject via {@code @Autowired} or constructor injection.
 *
 * <p>Type-conversion helpers bridge the entity's raw String/JSON shape (the entities keep enum and
 * structured-result columns as Strings to stay decoupled from wire-API identity) and the wire DTO's
 * typed enums and nested objects.
 */
@Mapper(componentModel = "spring")
public interface OptimizerMapper {

  ObjectMapper JSON = new ObjectMapper();

  /** Map a {@link TableOperationsRow} to its DTO. */
  TableOperationsDto toDto(TableOperationsRow row);

  /** Map a {@link TableOperationsHistoryRow} to its DTO. */
  TableOperationsHistoryDto toDto(TableOperationsHistoryRow row);

  /** Map a {@link TableStatsRow} to its DTO. */
  TableStatsDto toDto(TableStatsRow row);

  /** Map a {@link TableStatsHistoryRow} to its DTO. */
  TableStatsHistoryDto toDto(TableStatsHistoryRow row);

  // --- entity String ↔ wire enum/object helpers ---

  default OperationType toOperationType(String value) {
    return value == null ? null : OperationType.valueOf(value);
  }

  default String fromOperationType(OperationType value) {
    return value == null ? null : value.name();
  }

  default OperationStatus toOperationStatus(String value) {
    return value == null ? null : OperationStatus.valueOf(value);
  }

  default String fromOperationStatus(OperationStatus value) {
    return value == null ? null : value.name();
  }

  default HistoryStatus toHistoryStatus(String value) {
    return value == null ? null : HistoryStatus.valueOf(value);
  }

  default String fromHistoryStatus(HistoryStatus value) {
    return value == null ? null : value.name();
  }

  default JobResult toJobResult(String json) {
    if (json == null) {
      return null;
    }
    try {
      return JSON.readValue(json, JobResult.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to parse JobResult JSON from DB", e);
    }
  }

  default String fromJobResult(JobResult value) {
    if (value == null) {
      return null;
    }
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize JobResult to JSON", e);
    }
  }
}
