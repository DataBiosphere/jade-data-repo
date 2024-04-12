package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import com.google.cloud.bigquery.FieldValueList;

import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.CONCEPT_ID;
import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.CONCEPT_NAME;
import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.HAS_CHILDREN;
import static bio.terra.service.snapshotbuilder.utils.QueryBuilderConstants.COUNT;


public class AggregateBQQueryResultsUtils {
  public static SnapshotBuilderConcept toConcept(FieldValueList row) {
    int count;
    try {
      count =
          SnapshotBuilderService.fuzzyLowCount(
              (int)
                  row.get(COUNT).getLongValue()); // If exists, use its value
    } catch (IllegalArgumentException e) {
      count = 1;
    }
    return new SnapshotBuilderConcept()
        .id((int) (row.get(CONCEPT_ID).getLongValue()))
        .name(row.get(CONCEPT_NAME).getStringValue())
        .hasChildren(row.get(HAS_CHILDREN).getBooleanValue())
        .count(count);
  }

  public static int toCount(FieldValueList row) {
    return (int) row.get(0).getLongValue();
  }

  public static String toDomainId(FieldValueList row) {
    return row.get("domain_id").getStringValue();
  }
}
