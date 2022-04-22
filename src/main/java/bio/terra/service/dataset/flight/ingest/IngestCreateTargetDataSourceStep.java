package bio.terra.service.dataset.flight.ingest;

import static bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.*;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
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

public class IngestCreateTargetDataSourceStep implements Step {
  private final AzureSynapsePdao azureSynapsePdao;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final ContainerType containerType;
  private final AuthenticatedUserRequest userRequest;

  public IngestCreateTargetDataSourceStep(
      AzureSynapsePdao azureSynapsePdao,
      AzureBlobStorePdao azureBlobStorePdao,
      ContainerType containerType,
      AuthenticatedUserRequest userRequest) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.containerType = containerType;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String flightId = context.getFlightId();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    AzureStorageAccountResource storageAccountResource =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);

    String parquetDestinationLocation = storageAccountResource.getStorageAccountUrl();
    BlobUrlParts targetSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDestinationLocation,
            billingProfile,
            storageAccountResource,
            containerType,
            userRequest);
    try {
      azureSynapsePdao.createExternalDataSource(
          targetSignUrlBlob,
          IngestUtils.getScopedCredentialName(containerType, flightId),
          IngestUtils.getDataSourceName(containerType, flightId));
    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropDataSources(
        List.of(IngestUtils.getDataSourceName(containerType, context.getFlightId())));
    azureSynapsePdao.dropScopedCredentials(
        List.of(IngestUtils.getScopedCredentialName(containerType, context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }
}
