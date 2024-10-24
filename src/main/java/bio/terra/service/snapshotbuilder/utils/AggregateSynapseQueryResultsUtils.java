package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.filedata.exception.ProcessResultSetException;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

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

  public static Map.Entry<Integer, String> toConceptIdNamePair(ResultSet rs) {
    return Map.entry(
        getField(rs::getLong, Concept.CONCEPT_ID).intValue(),
        getField(rs::getString, Concept.CONCEPT_NAME));
  }

  public static SnapshotBuilderConcept toConcept(ResultSet rs) {
    return new SnapshotBuilderConcept()
        .name(getField(rs::getString, Concept.CONCEPT_NAME))
        .id(getField(rs::getLong, Concept.CONCEPT_ID).intValue())
        .hasChildren(getField(rs::getLong, QueryBuilderFactory.HAS_CHILDREN) > 0)
        .code(getField(rs::getString, Concept.CONCEPT_CODE))
        .count(
            SnapshotBuilderService.fuzzyLowCount(
                getField(rs::getLong, QueryBuilderFactory.COUNT).intValue()));
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
    return getField(rs::getString, Concept.DOMAIN_ID);
  }
}
