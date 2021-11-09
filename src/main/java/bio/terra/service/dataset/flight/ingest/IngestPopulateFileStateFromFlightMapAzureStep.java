package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class IngestPopulateFileStateFromFlightMapAzureStep
    extends IngestPopulateFileStateFromFlightMapStep {

  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestPopulateFileStateFromFlightMapAzureStep(
      LoadService loadService,
      FileService fileService,
      AzureBlobStorePdao azureBlobStorePdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      int batchSize,
      AuthenticatedUserRequest userRequest,
      Predicate<FlightContext> doCondition) {
    super(loadService, fileService, objectMapper, dataset, batchSize, doCondition);
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
  }

  @Override
  Stream<BulkLoadFileModel> getModelsStream(
      IngestRequestModel ingestRequest, List<Column> fileRefColumns, List<String> errors) {
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
        errors);
  }
}
