package bio.terra.service.snapshotbuilder.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.exception.ProcessResultSetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AggregateSynapseQueryResultsUtilsTest {
  @Test
  void rollupCountsReturnsListOfInt() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getInt(1)).thenReturn(5);

    assertThat(
        "rollupCountsMapper converts table result to list of ints",
        AggregateSynapseQueryResultsUtils.toCount(rs),
        equalTo(5));
  }

  @Test
  void rollupCountsHandlesSQLException() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getInt(1)).thenThrow(new SQLException());

    assertThrows(
        ProcessResultSetException.class,
        () -> AggregateSynapseQueryResultsUtils.toCount(rs),
        "Error processing result set into SnapshotBuilderConcept model");
  }

  @Test
  void toDomainIdReturnsString() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("domain_id")).thenReturn("domain_id");

    assertThat(
        "domainId converts table result to a string",
        AggregateSynapseQueryResultsUtils.toDomainId(rs),
        equalTo("domain_id"));
  }

  @Test
  void toDomainIdHandlesSQLException() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("domain_id")).thenThrow(new SQLException());

    assertThrows(
        ProcessResultSetException.class,
        () -> AggregateSynapseQueryResultsUtils.toDomainId(rs),
        "Error processing result set into String domain ID");
  }
}
