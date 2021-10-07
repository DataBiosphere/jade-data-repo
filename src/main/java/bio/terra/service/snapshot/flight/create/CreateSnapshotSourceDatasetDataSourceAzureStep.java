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
import java.util.Arrays;

public class CreateSnapshotSourceDatasetDataSourceAzureStep implements Step {
  private AzureSynapsePdao azureSynapsePdao;
  private AzureBlobStorePdao azureBlobStorePdao;

  public CreateSnapshotSourceDatasetDataSourceAzureStep(
      AzureSynapsePdao azureSynapsePdao, AzureBlobStorePdao azureBlobStorePdao) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource datasetAzureStorageAccountResource =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);
    AzureStorageAccountResource snapshotAzureStorageAccountResource =
        workingMap.get(
            CommonMapKeys.SNAPSHOT_STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);

    String parquetDatasetSourceLocation =
        IngestUtils.getParquetTargetLocationURL(datasetAzureStorageAccountResource);
    BlobUrlParts snapshotSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDatasetSourceLocation,
            billingProfile,
            snapshotAzureStorageAccountResource,
            AzureStorageAccountResource.ContainerType.METADATA);
    try {
      azureSynapsePdao.createExternalDataSource(
          snapshotSignUrlBlob,
          IngestUtils.getIngestRequestScopedCredentialName(context.getFlightId()),
          IngestUtils.getIngestRequestDataSourceName(context.getFlightId()));
    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropDataSources(
        Arrays.asList(IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        Arrays.asList(IngestUtils.getIngestRequestScopedCredentialName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }
}
