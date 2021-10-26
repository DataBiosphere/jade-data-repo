package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.azure.storage.blob.BlobContainerClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class IngestBuildAndWriteScratchLoadFileAzureStep
    extends IngestBuildAndWriteScratchLoadFileStep {
  private final AzureBlobStorePdao azureBlobStorePdao;
  AzureContainerPdao azureContainerPdao;

  public IngestBuildAndWriteScratchLoadFileAzureStep(
      ObjectMapper objectMapper,
      AzureBlobStorePdao azureBlobStorePdao,
      AzureContainerPdao azureContainerPdao,
      Dataset dataset,
      Predicate<FlightContext> doCondition) {
    super(objectMapper, dataset, doCondition);
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.azureContainerPdao = azureContainerPdao;
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
        workingMap.get(FileMapKeys.STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);
    BlobContainerClient containerClient =
        azureContainerPdao.getOrCreateContainer(
            billingProfile, storageAccount, AzureStorageAccountResource.ContainerType.SCRATCH);
    return containerClient
        .getBlobClient(flightContext.getFlightId() + "-scratch.json")
        .getBlobUrl();
  }

  @Override
  void writeCloudFile(FlightContext flightContext, String path, Stream<String> lines) {
    FlightMap workingMap = flightContext.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount =
        workingMap.get(FileMapKeys.STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);
    String signedPath =
        azureBlobStorePdao.signFile(
            billingProfile,
            storageAccount,
            path,
            AzureStorageAccountResource.ContainerType.SCRATCH,
            Duration.ofHours(1L),
            billingProfile.getBiller());
    azureBlobStorePdao.writeBlobLines(signedPath, lines);
  }
}
