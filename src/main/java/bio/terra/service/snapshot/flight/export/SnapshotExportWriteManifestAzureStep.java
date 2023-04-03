package bio.terra.service.snapshot.flight.export;

import bio.terra.common.FlightUtils;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.SnapshotExportResponseModel;
import bio.terra.model.SnapshotExportResponseModelFormat;
import bio.terra.model.SnapshotExportResponseModelFormatParquet;
import bio.terra.model.SnapshotExportResponseModelFormatParquetLocation;
import bio.terra.model.SnapshotExportResponseModelFormatParquetLocationTables;
import bio.terra.model.SnapshotModel;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.azure.storage.blob.BlobUrlParts;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public record SnapshotExportWriteManifestAzureStep(
    UUID snapshotId,
    SnapshotService snapshotService,
    ObjectMapper objectMapper,
    AzureBlobStorePdao azureBlobStorePdao,
    ResourceService resourceService,
    ProfileService profileService,
    AuthenticatedUserRequest userReq)
    implements DefaultUndoStep {

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();

    Map<String, List<String>> paths =
        FlightUtils.getTyped(workingMap, SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_PARQUET_PATHS);
    ContainerType containerType = ContainerType.METADATA;

    UUID billingProfileId = workingMap.get(JobMapKeys.BILLING_ID.getKeyName(), UUID.class);
    BillingProfileModel billingProfile = profileService.getProfileById(billingProfileId, userReq);
    AzureStorageAccountResource storageAccountResource =
        resourceService
            .getSnapshotStorageAccount(snapshotId)
            .orElseThrow(() -> new NotFoundException("Snapshot storage account not found"));
    String exportManifestPath = "manifests/%s/manifest.json".formatted(context.getFlightId());
    String fullExportManifestPath =
        "%s/%s/%s"
            .formatted(
                storageAccountResource.getStorageAccountUrl(), containerType, exportManifestPath);
    BlobUrlParts snapshotSignedUrlBlob =
        azureBlobStorePdao.getOrSignUrlForTargetFactory(
            fullExportManifestPath, billingProfile, storageAccountResource, containerType, userReq);

    List<SnapshotExportResponseModelFormatParquetLocationTables> tables =
        paths.entrySet().stream()
            .map(
                entry ->
                    new SnapshotExportResponseModelFormatParquetLocationTables()
                        .name(entry.getKey())
                        .paths(entry.getValue()))
            .collect(Collectors.toList());

    SnapshotModel snapshot = snapshotService.retrieveAvailableSnapshotModel(snapshotId, userReq);

    SnapshotExportResponseModel responseModel =
        new SnapshotExportResponseModel()
            .snapshot(snapshot)
            .format(
                new SnapshotExportResponseModelFormat()
                    .parquet(
                        new SnapshotExportResponseModelFormatParquet()
                            .manifest(snapshotSignedUrlBlob.toUrl().toString())
                            .location(
                                new SnapshotExportResponseModelFormatParquetLocation()
                                    .tables(tables))));

    try {
      String manifestContents =
          objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(responseModel);

      azureBlobStorePdao.writeBlobLines(
          snapshotSignedUrlBlob.toUrl().toString(), Arrays.stream(manifestContents.split("\n")));
    } catch (JsonProcessingException ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    workingMap.put(
        SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_MANIFEST_PATH,
        snapshotSignedUrlBlob.toUrl().toString());
    responseModel.validatedPrimaryKeys(false);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), responseModel);

    return StepResult.getStepResultSuccess();
  }
}
