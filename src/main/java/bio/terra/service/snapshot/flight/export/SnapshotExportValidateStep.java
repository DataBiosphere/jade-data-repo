package bio.terra.service.snapshot.flight.export;

import bio.terra.common.Column;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnapshotExportValidateStep implements Step {
  private final BigQueryPdao bigQueryPdao;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public SnapshotExportValidateStep(
      BigQueryPdao bigQueryPdao, SnapshotService snapshotService, UUID snapshotId) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    Dataset dataset = snapshot.getFirstSnapshotSource().getDataset();
    List<SnapshotTable> tables = snapshot.getTables();
    List<String> tablesWithoutPrimaryKeys = new ArrayList<>();
    List<String> tablesWithDuplicates =
        tables.stream()
            .map(
                table -> {
                  String tableName = table.getName();
                  Optional<DatasetTable> datasetTable = dataset.getTableByName(tableName);
                  if (datasetTable.isEmpty()) {
                    throw new CorruptMetadataException(
                        String.format(
                            "Could not find '%s' table in the snapshot's dataset", tableName));
                  }
                  DatasetTable realDatasetTable = datasetTable.get();
                  List<Column> primaryKeyColumns = realDatasetTable.getPrimaryKey();
                  if (primaryKeyColumns.isEmpty()) {
                    tablesWithoutPrimaryKeys.add(table.getName());
                    return null;
                  }

                  boolean hasDuplicates;
                  try {
                    hasDuplicates =
                        bigQueryPdao.hasDuplicatePrimaryKeys(
                            snapshot, primaryKeyColumns, tableName);
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

    if (tablesWithDuplicates.isEmpty() && tablesWithoutPrimaryKeys.isEmpty()) {
      return StepResult.getStepResultSuccess();
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append("Validating snapshot for export failed. ");
      if (!tablesWithDuplicates.isEmpty()) {
        int numTables = tablesWithDuplicates.size();
        String tableNames = String.join(", ", tablesWithDuplicates);
        sb.append(
            String.format(
                "%d table(s) had rows with duplicate primary keys [%s]. ", numTables, tableNames));
        sb.append(
            "To export, please create a new snapshot, ensuring there are rows with duplicate primary keys. ");
      }
      if (!tablesWithoutPrimaryKeys.isEmpty()) {
        int numTables = tablesWithoutPrimaryKeys.size();
        String tableNames = String.join(", ", tablesWithoutPrimaryKeys);
        sb.append(String.format("%d table(s) had no primary keys [%s]. ", numTables, tableNames));
        sb.append("To export, please ensure every table being exported has a primary key. ");
      }

      return new StepResult(
          StepStatus.STEP_RESULT_FAILURE_FATAL, new UnsupportedOperationException(sb.toString()));
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
