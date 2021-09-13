package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

// Populate the files to be loaded from the incoming array
public class IngestPopulateFileStateFromFileAzureStep extends IngestPopulateFileStateFromFileStep {
  private final LoadService loadService;
  private final AzureBlobStorePdao azureBlobStorePdao;

  public IngestPopulateFileStateFromFileAzureStep(
      LoadService loadService,
      int maxBadLines,
      int batchSize,
      AzureBlobStorePdao azureBlobStorePdao) {
    super(loadService, maxBadLines, batchSize);
    this.loadService = loadService;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();
    FlightMap workingMap = context.getWorkingMap();
    BulkLoadRequestModel loadRequest =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);
    BillingProfileModel billingProfileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);

    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    String blobStoreUrl = loadRequest.getLoadControlFile();
    IngestUtils.validateBlobAzureBlobFileURL(blobStoreUrl);
    BlobUrlParts ingestRequestSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForSourceFactory(
            blobStoreUrl, billingProfileModel.getTenantId());
    BlobClient blobClient =
        new BlobClientBuilder().endpoint(ingestRequestSignUrlBlob.toUrl().toString()).buildClient();

    InputStream inputStream = blobClient.openInputStream();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      readFile(reader, loadId);

    } catch (IOException ex) {
      throw new BulkLoadControlFileException("Failure accessing the load control file", ex);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    loadService.cleanFiles(loadId);
    return StepResult.getStepResultSuccess();
  }
}
