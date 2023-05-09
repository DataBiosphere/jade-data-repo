package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.ErrorCollector;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class IngestJsonFileSetupAzureStep extends IngestJsonFileSetupStep {

  private final ObjectMapper objectMapper;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestJsonFileSetupAzureStep(
      ObjectMapper objectMapper,
      AzureBlobStorePdao azureBlobStorePdao,
      Dataset dataset,
      AuthenticatedUserRequest userRequest,
      int maxBadLoadFileLineErrorsReported) {
    super(dataset, maxBadLoadFileLineErrorsReported);
    this.objectMapper = objectMapper;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
  }

  @Override
  long getFileModelsCount(
      IngestRequestModel ingestRequest,
      List<Column> fileRefColumns,
      ErrorCollector errorCollector) {
    String tenantId =
        IngestUtils.getIngestBillingProfileFromDataset(dataset, ingestRequest)
            .getTenantId()
            .toString();
    return IngestUtils.validateAndCountBulkFileLoadModelsFromPath(
        azureBlobStorePdao,
        objectMapper,
        ingestRequest,
        userRequest,
        tenantId,
        fileRefColumns,
        errorCollector,
        dataset);
  }
}
