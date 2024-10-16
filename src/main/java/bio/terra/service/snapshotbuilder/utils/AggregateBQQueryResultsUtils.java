package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.query.table.Concept;
import com.google.cloud.bigquery.FieldValueList;
import java.util.Map;

public class AggregateBQQueryResultsUtils {
  public static Map.Entry<Integer, String> toConceptIdNamePair(FieldValueList row) {
    return Map.entry(
        (int) (row.get(Concept.CONCEPT_ID).getLongValue()),
        row.get(Concept.CONCEPT_NAME).getStringValue());
  }

  public static SnapshotBuilderConcept toConcept(FieldValueList row) {
    return new SnapshotBuilderConcept()
        .id((int) (row.get(Concept.CONCEPT_ID).getLongValue()))
        .name(row.get(Concept.CONCEPT_NAME).getStringValue())
        .code(row.get(Concept.CONCEPT_CODE).getStringValue())
        .hasChildren(row.get(QueryBuilderFactory.HAS_CHILDREN).getLongValue() > 0)
        .count(
            SnapshotBuilderService.fuzzyLowCount(
                (int) row.get(QueryBuilderFactory.COUNT).getLongValue()));
  }

  public static int toCount(FieldValueList row) {
    return (int) row.get(0).getLongValue();
  }

  public static String toDomainId(FieldValueList row) {
    return row.get(Concept.DOMAIN_ID).getStringValue();
  }
}
