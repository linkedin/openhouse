package com.linkedin.openhouse.optimizer.api.mapper;

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
 */
@Mapper(componentModel = "spring")
public interface OptimizerMapper {

  /** Map a {@link TableOperationsRow} to its DTO. */
  TableOperationsDto toDto(TableOperationsRow row);

  /** Map a {@link TableOperationsHistoryRow} to its DTO. */
  TableOperationsHistoryDto toDto(TableOperationsHistoryRow row);

  /** Map a {@link TableStatsRow} to its DTO. */
  TableStatsDto toDto(TableStatsRow row);

  /** Map a {@link TableStatsHistoryRow} to its DTO. */
  TableStatsHistoryDto toDto(TableStatsHistoryRow row);
}
