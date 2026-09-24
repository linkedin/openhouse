package com.linkedin.openhouse.housetables.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

class ExplainingDataSourceTest {
  private final DataSource delegate = mock(DataSource.class);
  private final Connection raw = mock(Connection.class);
  private final PreparedStatement actual = mock(PreparedStatement.class);
  private final PreparedStatement explanation = mock(PreparedStatement.class);
  private final ResultSet result = mock(ResultSet.class);
  private Connection connection;

  @BeforeEach
  void setup() throws Exception {
    when(delegate.getConnection()).thenReturn(raw);
    connection = new ExplainingDataSource(delegate).getConnection();
    when(explanation.executeQuery()).thenReturn(result);
    when(result.next()).thenReturn(true);
    when(result.getString(1))
        .thenReturn(
            "{\"query_block\":{\"table\":"
                + "{\"table_name\":\"t\",\"access_type\":\"ref\",\"key\":\"PRIMARY\","
                + "\"rows_examined_per_scan\":1}}}");
    ExplainingDataSource.begin();
  }

  @AfterEach
  void cleanup() {
    ExplainingDataSource.end();
  }

  @Test
  void explainsActualMutationWithAllBoundTypesBeforeItExecutes() throws Exception {
    String sql = "delete from t where id=? and (? is null or version=?)";
    prepare(sql);
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setString(1, "MixedCase");
    statement.setNull(2, Types.BIGINT);
    statement.setLong(3, 12L);
    statement.executeUpdate();
    InOrder order = inOrder(explanation, actual);
    order.verify(explanation).setString(1, "MixedCase");
    order.verify(explanation).setNull(2, Types.BIGINT);
    order.verify(explanation).setLong(3, 12L);
    order.verify(explanation).executeQuery();
    order.verify(actual).executeUpdate();
  }

  @Test
  void resetsBindingsAndCapturesEveryExecutionNotJustFirstQuery() throws Exception {
    String sql = "select value from t where id=?";
    prepare(sql);
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setInt(1, 1);
    statement.executeQuery();
    statement.clearParameters();
    statement.setString(1, "new");
    statement.executeQuery();
    List<ExplainingDataSource.Plan> plans = ExplainingDataSource.end();
    ExplainingDataSource.begin();
    assertThat(plans).hasSize(2);
    assertThat(plans.get(1).bindings).containsExactly("1:setString=[1, new]");
    verify(explanation).setInt(1, 1);
    verify(explanation).setString(1, "new");
  }

  @Test
  void explainsFoldedCountPredicateInsteadOfTrustingAnEmptyPlan() throws Exception {
    String sql = "select count(*) from t where id=?";
    prepare(sql);
    when(result.getString(1))
        .thenReturn("{\"query_block\":{\"message\":\"Select tables optimized away\"}}");
    PreparedStatement probe = mock(PreparedStatement.class);
    ResultSet probeResult = mock(ResultSet.class);
    when(raw.prepareStatement("EXPLAIN FORMAT=JSON select 1 from t where id=?")).thenReturn(probe);
    when(probe.executeQuery()).thenReturn(probeResult);
    when(probeResult.next()).thenReturn(true);
    when(probeResult.getString(1))
        .thenReturn(
            "{\"query_block\":{\"table\":"
                + "{\"table_name\":\"t\",\"access_type\":\"const\",\"key\":\"PRIMARY\","
                + "\"rows_examined_per_scan\":1}}}");
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setString(1, "key");
    statement.executeQuery();
    verify(probe).setString(1, "key");
    List<ExplainingDataSource.Plan> plans = ExplainingDataSource.end();
    ExplainingDataSource.begin();
    assertThat(plans).hasSize(1);
    assertThat(plans.get(0).sql).isEqualTo(sql);
    assertThat(MysqlIndexPlan.violations(plans.get(0).explain, 10)).isEmpty();
  }

  @Test
  void doesNotHideSqlErrorsOrUnsupportedCapturePaths() throws Exception {
    String sql = "select value from t where id=?";
    prepare(sql);
    when(explanation.executeQuery()).thenThrow(new SQLException("bad plan"));
    PreparedStatement statement = connection.prepareStatement(sql);
    assertThatThrownBy(statement::executeQuery)
        .isInstanceOf(SQLException.class)
        .hasMessage("bad plan");
    assertThatThrownBy(statement::addBatch).isInstanceOf(SQLException.class);
    assertThatThrownBy(connection::createStatement).isInstanceOf(SQLException.class);
    assertThatThrownBy(() -> connection.unwrap(Connection.class)).isInstanceOf(SQLException.class);
  }

  private void prepare(String sql) throws Exception {
    when(raw.prepareStatement(sql)).thenReturn(actual);
    when(raw.prepareStatement("EXPLAIN FORMAT=JSON " + sql)).thenReturn(explanation);
  }
}
