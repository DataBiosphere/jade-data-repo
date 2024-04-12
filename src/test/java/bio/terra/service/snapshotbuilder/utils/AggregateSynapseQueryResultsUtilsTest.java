package bio.terra.service.snapshotbuilder.utils;

import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.CONCEPT_ID;
import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.CONCEPT_NAME;
import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.COUNT;
import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.DOMAIN_ID;
import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.HAS_CHILDREN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.filedata.exception.ProcessResultSetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
    when(rs.getString(DOMAIN_ID)).thenReturn(DOMAIN_ID);

    assertThat(
        "domainId converts table result to a string",
        AggregateSynapseQueryResultsUtils.toDomainId(rs),
        equalTo(DOMAIN_ID));
  }

  @Test
  void toDomainIdHandlesSQLException() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString(DOMAIN_ID)).thenThrow(new SQLException());

    assertThrows(
        ProcessResultSetException.class,
        () -> AggregateSynapseQueryResultsUtils.toDomainId(rs),
        "Error processing result set into String domain ID");
  }

  @Test
  void toConcept() throws Exception {
    var expected =
        new SnapshotBuilderConcept().name(CONCEPT_NAME).id(1).hasChildren(true).count(100);

    ResultSet rs = mock(ResultSet.class);
    when(rs.getLong(COUNT)).thenReturn((long) expected.getCount());
    when(rs.getString(CONCEPT_NAME)).thenReturn(expected.getName());
    when(rs.getLong(CONCEPT_ID)).thenReturn((long) expected.getId());
    when(rs.getBoolean(HAS_CHILDREN)).thenReturn(expected.isHasChildren());

    assertThat(
        "toConcept converts table result to SnapshotBuilderConcept",
        AggregateSynapseQueryResultsUtils.toConcept(rs),
        equalTo(expected));
  }
}
