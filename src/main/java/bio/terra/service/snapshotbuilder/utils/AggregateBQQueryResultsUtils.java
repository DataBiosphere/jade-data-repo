package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import com.google.cloud.bigquery.FieldValueList;

public class AggregateBQQueryResultsUtils {
  public static SnapshotBuilderConcept toConcept(FieldValueList row) {
    return new SnapshotBuilderConcept()
        .id((int) (row.get(HierarchyQueryBuilder.CONCEPT_ID).getLongValue()))
        .name(row.get(HierarchyQueryBuilder.CONCEPT_NAME).getStringValue())
        .code(row.get(HierarchyQueryBuilder.CONCEPT_CODE).getLongValue())
        .hasChildren(row.get(HierarchyQueryBuilder.HAS_CHILDREN).getBooleanValue())
        .count(
            SnapshotBuilderService.fuzzyLowCount(
                (int) row.get(HierarchyQueryBuilder.COUNT).getLongValue()));
  }

  public static int toCount(FieldValueList row) {
    return (int) row.get(0).getLongValue();
  }

  public static String toDomainId(FieldValueList row) {
    return row.get("domain_id").getStringValue();
  }
}
