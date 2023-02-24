package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.ErrorCollector;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobSasTokenOptions;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.stream.Stream;

public class IngestBuildAndWriteScratchLoadFileAzureStep
    extends IngestBuildAndWriteScratchLoadFileStep {
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AzureContainerPdao azureContainerPdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestBuildAndWriteScratchLoadFileAzureStep(
      ObjectMapper objectMapper,
      AzureBlobStorePdao azureBlobStorePdao,
      AzureContainerPdao azureContainerPdao,
      Dataset dataset,
      AuthenticatedUserRequest userRequest,
      int maxBadLoadFileLineErrorsReported) {
    super(objectMapper, dataset, maxBadLoadFileLineErrorsReported);
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.azureContainerPdao = azureContainerPdao;
    this.userRequest = userRequest;
  }

  @Override
  Stream<JsonNode> getJsonNodesFromCloudFile(
      IngestRequestModel ingestRequest, ErrorCollector errorCollector) {
    String tenantId =
        IngestUtils.getIngestBillingProfileFromDataset(dataset, ingestRequest)
            .getTenantId()
            .toString();
    return IngestUtils.getJsonNodesStreamFromFile(
        azureBlobStorePdao,
        objectMapper,
        ingestRequest.getPath(),
        userRequest,
        tenantId,
        errorCollector);
  }

  @Override
  String getOutputFilePath(FlightContext flightContext) {
    FlightMap workingMap = flightContext.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    BlobContainerClient containerClient =
        azureContainerPdao.getOrCreateContainer(
            billingProfile, storageAccount, AzureStorageAccountResource.ContainerType.SCRATCH);
    return containerClient
        .getBlobClient(flightContext.getFlightId() + "/ingest-scratch.json")
        .getBlobUrl();
  }

  @Override
  void writeCloudFile(FlightContext flightContext, String path, Stream<String> lines) {
    FlightMap workingMap = flightContext.getWorkingMap();
    BillingProfileModel billingProfile =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    AzureStorageAccountResource storageAccount =
        workingMap.get(
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, AzureStorageAccountResource.class);
    String signedPath =
        azureBlobStorePdao.signFile(
            billingProfile,
            storageAccount,
            path,
            AzureStorageAccountResource.ContainerType.SCRATCH,
            new BlobSasTokenOptions(
                Duration.ofHours(1),
                new BlobSasPermission().setReadPermission(true).setWritePermission(true),
                userRequest.getEmail()));
    azureBlobStorePdao.writeBlobLines(signedPath, lines);
  }
}
