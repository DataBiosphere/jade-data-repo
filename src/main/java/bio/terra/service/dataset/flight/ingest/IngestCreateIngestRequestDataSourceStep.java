package bio.terra.service.dataset.flight.ingest;

import static bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.*;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.CommonMapKeys;
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
import java.util.List;

public class IngestCreateIngestRequestDataSourceStep implements Step {
  private final AzureSynapsePdao azureSynapsePdao;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestCreateIngestRequestDataSourceStep(
      AzureSynapsePdao azureSynapsePdao,
      AzureBlobStorePdao azureBlobStorePdao,
      AuthenticatedUserRequest userRequest) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    final BlobUrlParts signedBlobUrlParts;
    if (IngestUtils.isCombinedFileIngest(context)) {
      String path = workingMap.get(IngestMapKeys.INGEST_CONTROL_FILE_PATH, String.class);
      AzureStorageAccountResource storageAccount =
          workingMap.get(
              CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
      signedBlobUrlParts =
          azureBlobStorePdao.getOrSignUrlForTargetFactory(
              path, billingProfileModel, storageAccount, ContainerType.SCRATCH, userRequest);
    } else {
      signedBlobUrlParts =
          azureBlobStorePdao.getOrSignUrlForSourceFactory(
              ingestRequestModel.getPath(), billingProfileModel.getTenantId(), userRequest);
    }

    try {
      azureSynapsePdao.createExternalDataSource(
          signedBlobUrlParts,
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
        List.of(IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        List.of(IngestUtils.getIngestRequestScopedCredentialName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }
}
