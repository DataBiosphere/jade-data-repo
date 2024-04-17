package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.filedata.exception.ProcessResultSetException;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AggregateSynapseQueryResultsUtils {

  public interface CountGetter<T> {
    T get(String fieldName) throws SQLException;
  }

  static <T> T getField(CountGetter<T> getter, String fieldName) {
    try {
      return getter.get(fieldName);
    } catch (SQLException e) {
      throw new ProcessResultSetException("Error processing result set", e);
    }
  }

  public static SnapshotBuilderConcept toConcept(ResultSet rs) {
    return new SnapshotBuilderConcept()
        .name(getField(rs::getString, "concept_name"))
        .id(getField(rs::getLong, "concept_id").intValue())
        .hasChildren(getField(rs::getBoolean, HierarchyQueryBuilder.HAS_CHILDREN))
        .code(getField(rs::getLong, HierarchyQueryBuilder.CONCEPT_CODE))
        .count(SnapshotBuilderService.fuzzyLowCount(getField(rs::getLong, "count").intValue()));
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
    return getField(rs::getString, "domain_id");
  }
}
