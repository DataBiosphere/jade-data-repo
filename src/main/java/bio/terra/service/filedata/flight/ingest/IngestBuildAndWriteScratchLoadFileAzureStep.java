package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobUrlParts;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class IngestBuildAndWriteScratchLoadFileAzureStep
    extends IngestBuildAndWriteScratchLoadFileStep {
  private final AzureBlobStorePdao azureBlobStorePdao;
  AzureContainerPdao azureContainerPdao;
  private final Dataset dataset;

  public IngestBuildAndWriteScratchLoadFileAzureStep(
      ObjectMapper objectMapper,
      AzureBlobStorePdao azureBlobStorePdao,
      AzureContainerPdao azureContainerPdao,
      Dataset dataset,
      Predicate<FlightContext> skipCondition) {
    super(objectMapper, skipCondition);
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.azureContainerPdao = azureContainerPdao;
    this.dataset = dataset;
  }

  @Override
  Stream<JsonNode> getJsonNodesFromCloudFile(
      IngestRequestModel ingestRequest, List<String> errors) {
    String tenantId =
        IngestUtils.getIngestBillingProfileFromDataset(dataset, ingestRequest)
            .getTenantId()
            .toString();
    return IngestUtils.getJsonNodesStreamFromFile(
        azureBlobStorePdao, objectMapper, ingestRequest, tenantId, errors);
  }

  @Override
  String getOutputFilePath(FlightContext flightContext) {
    FlightMap workingMap = flightContext.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount =
        workingMap.get(IngestMapKeys.STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    BlobContainerClient containerClient =
        azureContainerPdao.getOrCreateContainer(
            billingProfile, storageAccount, AzureStorageAccountResource.ContainerType.SCRATCH);
    String url =
        containerClient.getBlobClient(flightContext.getFlightId() + "-scratch.json").getBlobUrl();
    return azureBlobStorePdao.getOrSignUrlStringForTargetFactory(
        url, billingProfile, storageAccount);
  }

  @Override
  void writeCloudFile(FlightContext flightContext, String signedPath, Stream<String> lines) {
    azureBlobStorePdao.writeBlobLines(BlobUrlParts.parse(signedPath), lines);
  }
}
