package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.sql.SQLException;
import java.util.Map;

public class CreateSnapshotByAssetParquetFilesAzureStep
    implements Step, CreateSnapshotParquetFilesAzureInterface {
  private final SnapshotRequestModel snapshotReq;
  private final SnapshotService snapshotService;
  private final AzureSynapsePdao azureSynapsePdao;

  public CreateSnapshotByAssetParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    this.snapshotReq = snapshotReq;
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    return createSnapshotParquetFiles(context, azureSynapsePdao, snapshotService);
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    undoCreateSnapshotParquetFiles(context, snapshotService, azureSynapsePdao);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public Map<String, Long> createSnapshotPrimaryDataParquetFiles(FlightContext context)
      throws InterruptedException, SQLException {
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    SnapshotRequestAssetModel assetModel = contentsModel.getAssetSpec();

    Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());
    SnapshotSource source = snapshot.getFirstSnapshotSource();

    AssetSpecification assetSpec = source.getAssetSpecification();

    return azureSynapsePdao.createSnapshotParquetFilesByAsset(
        assetSpec,
        snapshot.getId(),
        IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
        IngestUtils.getTargetDataSourceName(context.getFlightId()),
        assetModel,
        snapshotReq.isGlobalFileIds());
  }
}
