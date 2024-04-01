package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import com.google.cloud.bigquery.FieldValueList;

public class AggregateBQQueryResultsUtils {
  // TODO - pull real values for hasChildren and count
  public static SnapshotBuilderConcept toConcept(FieldValueList row) {
    int count;
    try {
      count =
          SnapshotBuilderService.fuzzyLowCount(
              (int) row.get("count").getLongValue()); // If exists, use its value
    } catch (IllegalArgumentException e) {
      count = 1;
    }
    return new SnapshotBuilderConcept()
        .id((int) (row.get("concept_id").getLongValue()))
        .name(row.get("concept_name").getStringValue())
        .hasChildren(true)
        .count(count);
  }

  public static int toCount(FieldValueList row) {
    return (int) row.get(0).getLongValue();
  }

  public static String toDomainId(FieldValueList row) {
    return row.get(0).getStringValue();
  }
}
