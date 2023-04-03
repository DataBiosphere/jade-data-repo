package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public interface CreateSnapshotParquetFilesAzureStep extends Step {

  AzureSynapsePdao azureSynapsePdao();

  SnapshotService snapshotService();

  @Override
  default StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    List<SnapshotTable> tables = snapshotService().retrieveTables(snapshotId);

    azureSynapsePdao()
        .dropTables(
            tables.stream()
                .map(table -> IngestUtils.formatSnapshotTableName(snapshotId, table.getName()))
                .collect(Collectors.toList()));
    return StepResult.getStepResultSuccess();
  }
}
