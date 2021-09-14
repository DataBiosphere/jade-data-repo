package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IngestBuildAndWriteScratchLoadFileAzureStep extends IngestBuildAndWriteScratchLoadFileStep {
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final Dataset dataset;

  public IngestBuildAndWriteScratchLoadFileAzureStep(
      ObjectMapper objectMapper,
      AzureBlobStorePdao azureBlobStorePdao,
      Dataset dataset,
      Predicate<FlightContext> skipCondition) {
    super(objectMapper, skipCondition);
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.dataset = dataset;
  }

  @Override
  Stream<JsonNode> getJsonNodesFromCloudFile(IngestRequestModel ingestRequest, List<String> errors) {
    String tenantId = IngestUtils.getIngestBillingProfileFromDataset(dataset, ingestRequest).getTenantId().toString();
    return IngestUtils.getJsonNodesStreamFromFile(azureBlobStorePdao, objectMapper, ingestRequest, tenantId, errors);
  }

  @Override
  String getOutputFilePath(FlightContext flightContext) {
    AzureStorageAccountResource storageAccountResource =
        flightContext.getWorkingMap().get(FileMapKeys.STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);


    return null;
  }

  @Override
  void writeCloudFile(FlightContext flightContext, String path, Stream<String> lines) {

  }
}
