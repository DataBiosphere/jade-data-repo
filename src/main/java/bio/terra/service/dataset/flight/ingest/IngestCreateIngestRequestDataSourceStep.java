package bio.terra.service.dataset.flight.ingest;

import static bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.*;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
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

public class IngestCreateIngestRequestDataSourceStep implements Step {
  private AzureSynapsePdao azureSynapsePdao;
  private AzureBlobStorePdao azureBlobStorePdao;

  public IngestCreateIngestRequestDataSourceStep(
      AzureSynapsePdao azureSynapsePdao, AzureBlobStorePdao azureBlobStorePdao) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    IngestRequestModel ingestRequestModel = IngestUtils.getIngestRequestModel(context);
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    final BlobUrlParts signedBlobUrlParts;
    final ContainerType containerType;
    if (IngestUtils.noFilesToIngest.test(context)) {
      signedBlobUrlParts =
          azureBlobStorePdao.getOrSignUrlForSourceFactory(
              ingestRequestModel.getPath(), billingProfileModel.getTenantId());

    } else {
      String path = workingMap.get(IngestMapKeys.INGEST_SCRATCH_FILE_PATH, String.class);
      AzureStorageAccountResource storageAccount =
          workingMap.get(IngestMapKeys.STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
      signedBlobUrlParts =
          azureBlobStorePdao.getOrSignUrlForTargetFactory(
              path, billingProfileModel, storageAccount, ContainerType.SCRATCH);
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
        Arrays.asList(IngestUtils.getIngestRequestDataSourceName(context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        Arrays.asList(IngestUtils.getIngestRequestScopedCredentialName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }
}
