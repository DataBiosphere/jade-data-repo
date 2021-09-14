package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.profile.flight.ProfileMapKeys;
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

    BlobUrlParts ingestRequestSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForSourceFactory(
            ingestRequestModel.getPath(), billingProfileModel.getTenantId());

    try {
      azureSynapsePdao.createExternalDataSource(
          ingestRequestSignUrlBlob,
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
