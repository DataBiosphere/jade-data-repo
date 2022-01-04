package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.AzureBlobStoreBufferedReader;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;

// Populate the files to be loaded from the incoming array
public class IngestPopulateFileStateFromFileAzureStep extends IngestPopulateFileStateFromFileStep {
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestPopulateFileStateFromFileAzureStep(
      LoadService loadService,
      int maxBadLoadFileLineErrorsReported,
      int batchSize,
      AzureBlobStorePdao azureBlobStorePdao,
      ObjectMapper bulkLoadObjectMapper,
      AuthenticatedUserRequest userRequest) {
    super(loadService, maxBadLoadFileLineErrorsReported, batchSize, bulkLoadObjectMapper);
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // get variables required for building the signed url for the ingest request file
    FlightMap inputParameters = context.getInputParameters();
    FlightMap workingMap = context.getWorkingMap();
    BulkLoadRequestModel loadRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    // Validate control file url and sign if not already signed
    String blobStoreUrl = loadRequest.getLoadControlFile();
    IngestUtils.validateBlobAzureBlobFileURL(blobStoreUrl);
    String ingestRequestSignedUrl =
        azureBlobStorePdao.getOrSignUrlStringForSourceFactory(
            blobStoreUrl, billingProfileModel.getTenantId(), userRequest);

    // Stream from control file and build list of files to be ingested
    try (BufferedReader reader = new AzureBlobStoreBufferedReader(ingestRequestSignedUrl)) {
      readFile(reader, context, CloudPlatformWrapper.of(CloudPlatform.AZURE));

    } catch (IOException ex) {
      throw new BulkLoadControlFileException(
          "Failure accessing the load control file in Azure blob storage", ex);
    }

    return StepResult.getStepResultSuccess();
  }
}
