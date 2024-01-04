package bio.terra.service.snapshot.flight.export;

import bio.terra.common.Column;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.snapshot.exception.SnapshotExportException;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnapshotExportValidatePrimaryKeysStep implements Step {
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public SnapshotExportValidatePrimaryKeysStep(SnapshotService snapshotService, UUID snapshotId) {
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    Dataset dataset = snapshot.getFirstSnapshotSource().getDataset();
    List<SnapshotTable> tables = snapshot.getTables();
    List<String> tablesWithDuplicates =
        tables.stream()
            .map(
                table -> {
                  String tableName = table.getName();
                  DatasetTable datasetTable =
                      dataset
                          .getTableByName(tableName)
                          .orElseThrow(
                              () ->
                                  new CorruptMetadataException(
                                      String.format(
                                          "Could not find '%s' table in the snapshot's dataset",
                                          tableName)));
                  List<Column> primaryKeyColumns = datasetTable.getPrimaryKey();
                  if (primaryKeyColumns.isEmpty()) {
                    return null;
                  }

                  boolean hasDuplicates;
                  try {
                    hasDuplicates =
                        (BigQueryPdao.duplicatePrimaryKeys(snapshot, primaryKeyColumns, tableName)
                                .getTotalRows()
                            > 0);
                  } catch (InterruptedException e) {
                    throw new GoogleResourceException(
                        String.format(
                            "Failed to query BigQuery for duplicate rows in snapshot table '%s'",
                            tableName));
                  }
                  if (hasDuplicates) {
                    return table.getName();
                  } else {
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (tablesWithDuplicates.isEmpty()) {
      return StepResult.getStepResultSuccess();
    } else {
      String message = "Validating snapshot primary keys for export failed.";
      int numTables = tablesWithDuplicates.size();
      String tableNames = String.join(", ", tablesWithDuplicates);
      List<String> details =
          List.of(
              String.format(
                  "%d table(s) had rows with duplicate primary keys [%s]. ", numTables, tableNames),
              "To export, please create a new snapshot, ensuring there are no rows with duplicate primary keys.");
      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new SnapshotExportException(message, details));
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
