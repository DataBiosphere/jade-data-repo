package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.ErrorCollector;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.load.LoadService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.stream.Stream;

public class IngestPopulateFileStateFromFlightMapAzureStep
    extends IngestPopulateFileStateFromFlightMapStep {

  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userRequest;
  private final int maxBadLoadFileLineErrorsReported;

  public IngestPopulateFileStateFromFlightMapAzureStep(
      LoadService loadService,
      FileService fileService,
      AzureBlobStorePdao azureBlobStorePdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      int batchSize,
      AuthenticatedUserRequest userRequest,
      int maxBadLoadFileLineErrorsReported) {
    super(
        loadService,
        fileService,
        objectMapper,
        dataset,
        batchSize,
        maxBadLoadFileLineErrorsReported);
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
  }

  @Override
  Stream<BulkLoadFileModel> getModelsStream(
      IngestRequestModel ingestRequest,
      List<Column> fileRefColumns,
      ErrorCollector errorCollector) {
    String tenantId =
        IngestUtils.getIngestBillingProfileFromDataset(dataset, ingestRequest)
            .getTenantId()
            .toString();
    return IngestUtils.getBulkFileLoadModelsStream(
        azureBlobStorePdao,
        objectMapper,
        ingestRequest,
        userRequest,
        tenantId,
        fileRefColumns,
        errorCollector);
  }
}
