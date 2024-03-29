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
  void domainIdReturnsListOfString() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString(1)).thenReturn("domain");

    assertThat(
        "domainId converts table result to list of string",
        AggregateSynapseQueryResultsUtils.domainId(rs),
        equalTo("domain"));
  }

  @Test
  void domainIdHandlesSQLException() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString(1)).thenThrow(new SQLException());

    assertThrows(
        ProcessResultSetException.class,
        () -> AggregateSynapseQueryResultsUtils.domainId(rs),
        "Error processing result set into String domain ID");
  }
}
