package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;

public class CreateSnapshotParquetFilesAzureStep implements Step {

  protected final AzureSynapsePdao azureSynapsePdao;
  protected final SnapshotService snapshotService;
  protected final UUID snapshotId;

  public CreateSnapshotParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao, SnapshotService snapshotService, UUID snapshotId) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    throw new NotImplementedException(
        "doStep should be implemented by Snapshot Type Specific Steps");
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);

    azureSynapsePdao.dropTables(
        tables.stream()
            .map(table -> IngestUtils.formatSnapshotTableName(snapshotId, table.getName()))
            .collect(Collectors.toList()));
    return StepResult.getStepResultSuccess();
  }
}
