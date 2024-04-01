package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.filedata.exception.ProcessResultSetException;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AggregateSynapseQueryResultsUtils {
  // TODO - pull real values for hasChildren and count
  public static SnapshotBuilderConcept toConcept(ResultSet rs) {
    int count;
    try {
      count = SnapshotBuilderService.fuzzyLowCount((int) rs.getLong("count"));
    } catch (SQLException | IllegalArgumentException e) {
      count = 1;
    }

    try {
      return new SnapshotBuilderConcept()
          .name(rs.getString("concept_name"))
          .id((int) rs.getLong("concept_id"))
          .hasChildren(true)
          .count(count);
    } catch (SQLException e) {
      throw new ProcessResultSetException(
          "Error processing result set into SnapshotBuilderConcept model", e);
    }
  }

  public static int toCount(ResultSet rs) {
    try {
      // Java ResultSet is 1 indexed
      // https://docs.oracle.com/en/java/javase/17/docs/api/java.sql/java/sql/ResultSet.html#getInt(int)
      return rs.getInt(1);
    } catch (SQLException e) {
      throw new ProcessResultSetException(
          "Error processing result set into SnapshotBuilderConcept model", e);
    }
  }

  public static String toDomainId(ResultSet rs) {
    try {
      // Azure ResultSet is 1 indexed
      // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
      return rs.getString(1);
    } catch (SQLException e) {
      throw new ProcessResultSetException("Error processing result set into String domain ID", e);
    }
  }
}
