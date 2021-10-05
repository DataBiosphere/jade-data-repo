package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class IngestPopulateFileStateFromFlightMapAzureStep
    extends IngestPopulateFileStateFromFlightMapStep {

  private final AzureBlobStorePdao azureBlobStorePdao;

  public IngestPopulateFileStateFromFlightMapAzureStep(
      LoadService loadService,
      AzureBlobStorePdao azureBlobStorePdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      int batchSize,
      Predicate<FlightContext> skipCondition,
      FileService fileService) {
    super(loadService, objectMapper, dataset, batchSize, skipCondition, fileService);
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  Stream<BulkLoadFileModel> getModelsStream(
      IngestRequestModel ingestRequest, List<String> fileRefColumns, List<String> errors) {
    String tenantId =
        IngestUtils.getIngestBillingProfileFromDataset(dataset, ingestRequest)
            .getTenantId()
            .toString();
    return IngestUtils.getBulkFileLoadModelsStream(
        azureBlobStorePdao, objectMapper, ingestRequest, tenantId, fileRefColumns, errors);
  }
}
