package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class IngestJsonFileSetupAzureStep extends IngestJsonFileSetupStep {

  private final ObjectMapper objectMapper;
  private final AzureBlobStorePdao azureBlobStorePdao;

  public IngestJsonFileSetupAzureStep(
      ObjectMapper objectMapper, AzureBlobStorePdao azureBlobStorePdao, Dataset dataset) {
    super(dataset);
    this.objectMapper = objectMapper;
    this.azureBlobStorePdao = azureBlobStorePdao;
  }

  @Override
  long getFileModelsCount(
      IngestRequestModel ingestRequest, List<Column> fileRefColumns, List<String> errors) {
    String tenantId =
        IngestUtils.getIngestBillingProfileFromDataset(dataset, ingestRequest)
            .getTenantId()
            .toString();
    return IngestUtils.countBulkFileLoadModelsFromPath(
        azureBlobStorePdao, objectMapper, ingestRequest, tenantId, fileRefColumns, errors);
  }
}
