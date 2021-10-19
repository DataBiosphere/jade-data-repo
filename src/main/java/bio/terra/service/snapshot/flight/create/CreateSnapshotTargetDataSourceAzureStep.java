package bio.terra.service.snapshot.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.storage.blob.BlobUrlParts;
import java.sql.SQLException;

// TODO - this is the exact same step as used for ingest - find way to share code
public class CreateSnapshotTargetDataSourceAzureStep implements Step {
  private AzureSynapsePdao azureSynapsePdao;
  private AzureBlobStorePdao azureBlobStorePdao;

  public CreateSnapshotTargetDataSourceAzureStep(
      AzureSynapsePdao azureSynapsePdao, AzureBlobStorePdao azureBlobStorePdao) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource snapshotAzureStorageAccountResource =
        workingMap.get(
            CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);

    String snapshotParquetTargetLocation =
        IngestUtils.getParquetTargetLocationURL(snapshotAzureStorageAccountResource);
    BlobUrlParts snapshotSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            snapshotParquetTargetLocation,
            billingProfile,
            snapshotAzureStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA);
    try {
      azureSynapsePdao.createExternalDataSource(
          snapshotSignUrlBlob,
          IngestUtils.getTargetScopedCredentialName(context.getFlightId()),
          IngestUtils.getTargetDataSourceName(context.getFlightId()));
    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // TODO - drop data sources, etc.
    // TODO - UNCOMMENT BEFORE MERGE
    //    azureSynapsePdao.dropTables(
    //        Arrays.asList(IngestUtils.getSynapseTableName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }
}
