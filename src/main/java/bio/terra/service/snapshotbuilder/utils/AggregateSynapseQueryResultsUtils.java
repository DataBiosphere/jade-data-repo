package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.filedata.exception.ProcessResultSetException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AggregateSynapseQueryResultsUtils {
  // TODO - pull real values for hasChildren and count
  public static SnapshotBuilderConcept aggregateConceptResult(ResultSet rs) {
    try {
      return new SnapshotBuilderConcept()
          .name(rs.getString("concept_name"))
          .id((int) rs.getLong("concept_id"))
          .hasChildren(true)
          .count(1);
    } catch (SQLException e) {
      throw new ProcessResultSetException(
          "Error processing result set into SnapshotBuilderConcept model", e);
    }
  }

  public static int rollupCountsMapper(ResultSet rs) {
    try {
      return rs.getInt(0);
    } catch (SQLException e) {
      throw new ProcessResultSetException(
          "Error processing result set into SnapshotBuilderConcept model", e);
    }
  }
}
