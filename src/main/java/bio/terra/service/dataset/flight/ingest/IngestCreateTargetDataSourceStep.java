package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.storage.blob.BlobUrlParts;
import java.sql.SQLException;
import java.util.Arrays;

public class IngestCreateTargetDataSourceStep implements Step {
  private AzureSynapsePdao azureSynapsePdao;
  private AzureBlobStorePdao azureBlobStorePdao;
  private DatasetService datasetService;
  private ResourceService resourceService;

  public IngestCreateTargetDataSourceStep(
      AzureSynapsePdao azureSynapsePdao,
      AzureBlobStorePdao azureBlobStorePdao,
      DatasetService datasetService,
      ResourceService resourceService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.datasetService = datasetService;
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    String flightId = context.getFlightId();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);

    AzureStorageAccountResource storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfile, flightId);
    workingMap.put(IngestMapKeys.STORAGE_ACCOUNT_RESOURCE, storageAccountResource);

    String parquetDestinationLocation =
        IngestUtils.getParquetTargetLocationURL(storageAccountResource);
    BlobUrlParts targetSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            parquetDestinationLocation, billingProfile, storageAccountResource);
    try {
      azureSynapsePdao.createExternalDataSource(
          targetSignUrlBlob,
          IngestUtils.getTargetScopedCredentialName(flightId),
          IngestUtils.getTargetDataSourceName(flightId));
    } catch (SQLException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    azureSynapsePdao.dropTables(
        Arrays.asList(IngestUtils.getSynapseTableName(context.getFlightId())));

    return StepResult.getStepResultSuccess();
  }
}
