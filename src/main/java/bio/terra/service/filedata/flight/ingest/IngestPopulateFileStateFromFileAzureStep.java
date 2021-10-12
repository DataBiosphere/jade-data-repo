package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadRequestModel;
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

  public IngestPopulateFileStateFromFileAzureStep(
      LoadService loadService,
      int maxBadLines,
      int batchSize,
      AzureBlobStorePdao azureBlobStorePdao,
      ObjectMapper bulkLoadObjectMapper) {
    super(loadService, maxBadLines, batchSize, bulkLoadObjectMapper);
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    // get variables required for building the signed url for the ingest request file
    FlightMap inputParameters = context.getInputParameters();
    FlightMap workingMap = context.getWorkingMap();
    BulkLoadRequestModel loadRequest = JobMapKeys.REQUEST.get(inputParameters);
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    // Validate control file url and sign if not already signed
    String blobStoreUrl = loadRequest.getLoadControlFile();
    IngestUtils.validateBlobAzureBlobFileURL(blobStoreUrl);
    String ingestRequestSignedUrl =
        azureBlobStorePdao.getOrSignUrlStringForSourceFactory(
            blobStoreUrl, billingProfileModel.getTenantId());

    // Stream from control file and build list of files to be ingested
    try (BufferedReader reader = new AzureBlobStoreBufferedReader(ingestRequestSignedUrl)) {
      readFile(reader, context);

    } catch (IOException ex) {
      throw new BulkLoadControlFileException(
          "Failure accessing the load control file in Azure blob storage", ex);
    }

    return StepResult.getStepResultSuccess();
  }
}
