package com.linkedin.openhouse.housetables.e2e.usertable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.model.TestHouseTableModelConstants;
import com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableHtsJdbcRepository;
import com.linkedin.openhouse.housetables.services.UserTablesService;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.Advisor;
import org.springframework.aop.aspectj.AbstractAspectJAdvice;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.support.PersistenceExceptionTranslationInterceptor;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Load-bearing, not incidental. {@link
 * com.linkedin.openhouse.housetables.repository.impl.jdbc.UserTableRepositoryTranslationAspect}
 * only works because it lands in a proxy outside the one Spring Data builds, and nothing at any
 * call site would reveal it if that stopped being true: every corrupt read would quietly degrade to
 * a generic 500 with no diagnostic, and the suite would otherwise still pass.
 */
@SpringBootTest(classes = SpringH2HtsApplication.class)
public class RepositoryTranslationAspectTest {

  private static final String ASPECT_DB = "aspect_db";

  private static final String ASPECT_BEAN = "userTableRepositoryTranslationAspect";

  @Autowired UserTableHtsJdbcRepository htsJdbcRepository;

  @Autowired UserTablesService userTablesService;

  @Autowired DataSource dataSource;

  @AfterEach
  public void tearDown() {
    new JdbcTemplate(dataSource)
        .update("DELETE FROM user_table_row WHERE database_id = ?", ASPECT_DB);
  }

  /**
   * The interception point itself. Asserted twice over: structurally, that {@code
   * PersistenceExceptionTranslationInterceptor} sits in a strictly deeper proxy than our advice;
   * and behaviourally, that a real corrupt hydration therefore arrives already translated and comes
   * back out unwrapped. Were the advice inside that interceptor its {@code DataAccessException}
   * catch would never fire, and this read would surface as a bare {@code JpaSystemException}.
   */
  @Test
  public void testAspectRunsOutsidePersistenceExceptionTranslationSoItSeesTheTranslatedFailure() {
    List<List<Object>> layers = adviceLayers(htsJdbcRepository);

    int aspectLayer = -1;
    int translationLayer = -1;
    for (int layer = 0; layer < layers.size(); layer++) {
      for (Object advice : layers.get(layer)) {
        if (advice instanceof AbstractAspectJAdvice
            && ASPECT_BEAN.equals(((AbstractAspectJAdvice) advice).getAspectName())) {
          aspectLayer = layer;
        }
        if (advice instanceof PersistenceExceptionTranslationInterceptor) {
          translationLayer = layer;
        }
      }
    }

    assertThat(aspectLayer)
        .as("the translation advice must advise the repository proxy")
        .isNotNegative();
    assertThat(translationLayer)
        .as("Spring Data must still install its translator")
        .isNotNegative();
    assertThat(aspectLayer)
        .as("the advice must wrap persistence exception translation, not sit inside it")
        .isLessThan(translationLayer);

    insertCorruptRow("interception_point");

    assertThatThrownBy(
            () ->
                htsJdbcRepository.findByDatabaseIdIgnoreCaseAndTableIdIgnoreCase(
                    ASPECT_DB, "interception_point"))
        .isInstanceOf(CorruptEntityTypeException.class)
        .hasMessageContaining("Column user_table_row.entity_type holds unrecognized value")
        .hasMessageContaining("UNKNOWN");
  }

  /**
   * The property the aspect buys. {@code UserTablesServiceImpl} wraps nothing, so this read reaches
   * the diagnostic only because coverage is structural rather than remembered; deleting the aspect
   * turns it into a generic 500.
   */
  @Test
  public void testCorruptRowStillCarriesTheDiagnosticWithNoCallSiteWrapAnywhere() {
    insertCorruptRow("structural_coverage");

    assertThatThrownBy(() -> userTablesService.getNeutralEntity(ASPECT_DB, "structural_coverage"))
        .isInstanceOf(CorruptEntityTypeException.class)
        .hasMessageContaining("Column user_table_row.entity_type holds unrecognized value");

    // A second, unrelated unwrapped call site: the write path's occupancy read. Not a query
    // family, because the table predicate excludes an unrecognized spelling before hydration, so
    // only the type-neutral reads can meet a corrupt row at all under H2.
    assertThatThrownBy(
            () ->
                userTablesService.putUserTable(
                    UserTable.builder()
                        .databaseId(ASPECT_DB)
                        .tableId("structural_coverage")
                        .tableVersion("/openhouse/aspect_db/structural_coverage/v0_metadata.json")
                        .metadataLocation(
                            "/openhouse/aspect_db/structural_coverage/v1_metadata.json")
                        .build()))
        .isInstanceOf(CorruptEntityTypeException.class);
  }

  private static List<List<Object>> adviceLayers(Object proxy) {
    List<List<Object>> layers = new ArrayList<>();
    Object cursor = proxy;
    while (cursor instanceof Advised && layers.size() < 8) {
      Advised advised = (Advised) cursor;
      List<Object> advices = new ArrayList<>();
      for (Advisor advisor : advised.getAdvisors()) {
        advices.add(advisor.getAdvice());
      }
      layers.add(advices);
      try {
        cursor = advised.getTargetSource().getTarget();
      } catch (Exception e) {
        break;
      }
    }
    return layers;
  }

  private void insertCorruptRow(String tableId) {
    new JdbcTemplate(dataSource)
        .update(
            "INSERT INTO user_table_row "
                + "(database_id, table_id, version, metadata_location, storage_type, creation_time, entity_type) "
                + "VALUES (?, ?, ?, ?, ?, ?, 'UNKNOWN')",
            ASPECT_DB,
            tableId,
            0L,
            String.format("/openhouse/%s/%s/v0_metadata.json", ASPECT_DB, tableId),
            TestHouseTableModelConstants.TEST_DEFAULT_STORAGE_TYPE,
            TestHouseTableModelConstants.TEST_CREATION_TIME);
  }
}
