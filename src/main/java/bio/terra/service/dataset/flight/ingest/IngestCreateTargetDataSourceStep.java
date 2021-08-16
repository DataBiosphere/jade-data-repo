package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestCreateTargetDataSourceStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(IngestCreateTargetDataSourceStep.class);
  private AzureSynapsePdao azureSynapsePdao;
  private DatasetService datasetService;
  private ResourceService resourceService;

  public IngestCreateTargetDataSourceStep(
      AzureSynapsePdao azureSynapsePdao,
      DatasetService datasetService,
      ResourceService resourceService) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
    this.resourceService = resourceService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);

    AzureStorageAccountResource storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfile, context.getFlightId());
    String parquetDestinationLocation =
        IngestUtils.getParquetTargetLocationURL(storageAccountResource);
    BlobUrlParts targetSignUrlBlob =
        azureSynapsePdao.getOrSignUrlForTargetFactory(
            parquetDestinationLocation, billingProfile, storageAccountResource);
    try {
      azureSynapsePdao.createExternalDataSource(
          targetSignUrlBlob,
          IngestUtils.getTargetScopedCredentialName(context.getFlightId()),
          IngestUtils.getTargetDataSourceName(context.getFlightId()));
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
