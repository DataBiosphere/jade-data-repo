package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderConcept;
import com.google.cloud.bigquery.TableResult;
import java.util.List;
import java.util.stream.StreamSupport;

public class AggregateBQQueryResultsUtils {
  // TODO - pull real values for hasChildren and count
  public static List<SnapshotBuilderConcept> aggregateConceptResults(TableResult result) {
    return StreamSupport.stream(result.iterateAll().spliterator(), false)
        .map(
            row ->
                new SnapshotBuilderConcept()
                    .id((int) (row.get("concept_id").getLongValue()))
                    .name(row.get("concept_name").getStringValue())
                    .hasChildren(true)
                    .count(1))
        .toList();
  }
}
