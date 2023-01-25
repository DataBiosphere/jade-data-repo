package bio.terra.service.snapshot.flight.create;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class CreateSnapshotCleanSynapseAzureStep implements Step {

  private AzureSynapsePdao azureSynapsePdao;
  private SnapshotService snapshotService;

  public CreateSnapshotCleanSynapseAzureStep(
      AzureSynapsePdao azureSynapsePdao, SnapshotService snapshotService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    azureSynapsePdao.dropTables(
        snapshotService.retrieveTables(snapshotId).stream()
            .filter(table -> table.getRowCount() > 0)
            .map(table -> IngestUtils.formatSnapshotTableName(snapshotId, table.getName()))
            .collect(Collectors.toList()));
    azureSynapsePdao.dropTables(
        List.of(IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE)));
    azureSynapsePdao.dropDataSources(
        Arrays.asList(
            IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
            IngestUtils.getTargetDataSourceName(context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        Arrays.asList(
            IngestUtils.getSourceDatasetScopedCredentialName(context.getFlightId()),
            IngestUtils.getTargetScopedCredentialName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
