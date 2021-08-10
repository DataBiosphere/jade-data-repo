package bio.terra.common;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.dataset.StorageResource;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.validation.Errors;

public abstract class CloudPlatformWrapper {

  public static final CloudPlatform DEFAULT = CloudPlatform.GCP;

  public static CloudPlatformWrapper of(String cloudPlatformValue) {
    CloudPlatform cloudPlatform;
    if (cloudPlatformValue != null) {
      cloudPlatform =
          Optional.ofNullable(CloudPlatform.fromValue(cloudPlatformValue.toLowerCase()))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format("Invalid cloud platform %s", cloudPlatformValue)));
    } else {
      cloudPlatform = null;
    }
    return of(cloudPlatform);
  }

  public static CloudPlatformWrapper of(CloudPlatform cloudPlatform) {
    if (cloudPlatform == null) {
      cloudPlatform = DEFAULT;
    }
    switch (cloudPlatform) {
      case AZURE:
        return AzurePlatform.INSTANCE;
      case GCP:
      default:
        return GcpPlatform.INSTANCE;
    }
  }

  public boolean isGcp() {
    return false;
  }

  public boolean isAzure() {
    return false;
  }

  public abstract void ensureValidRegion(String region, Errors errors);

  void ensureValidRegion(
      CloudPlatform platform, String region, List<String> supportedRegions, Errors errors) {
    if (!supportedRegions.contains(region.toLowerCase())) {
      errors.rejectValue(
          "region",
          "InvalidRegionForPlatform",
          "Valid regions for " + platform + " are: " + String.join(", ", supportedRegions));
    }
  }

  public boolean is(CloudPlatform cloudPlatform) {
    return cloudPlatform == getCloudPlatform();
  }

  public abstract CloudPlatform getCloudPlatform();

  public abstract List<? extends StorageResource<?, ?>> createStorageResourceValues(
      DatasetRequestModel datasetRequest);

  // This doesn't really do anything for now, but as we move more things into Azure,
  // It will be helpful to use this to grab just the GCP resources we're still using.
  public static List<? extends StorageResource<?, ?>> getGoogleResourcesForAzure(
      DatasetRequestModel datasetRequestModel) {
    return CloudPlatformWrapper.of(CloudPlatform.GCP)
        .createStorageResourceValues(datasetRequestModel);
  }

  public abstract GoogleRegion getGoogleRegionFromDatasetRequestModel(
      DatasetRequestModel datasetRequestModel);

  static class GcpPlatform extends CloudPlatformWrapper {
    static final GcpPlatform INSTANCE = new GcpPlatform();

    @Override
    public CloudPlatform getCloudPlatform() {
      return CloudPlatform.GCP;
    }

    @Override
    public boolean isGcp() {
      return true;
    }

    @Override
    public void ensureValidRegion(String region, Errors errors) {
      ensureValidRegion(CloudPlatform.GCP, region, GoogleRegion.SUPPORTED_REGIONS, errors);
    }

    @Override
    public List<? extends StorageResource<?, ?>> createStorageResourceValues(
        DatasetRequestModel datasetRequest) {
      final GoogleRegion region = getGoogleRegionFromDatasetRequestModel(datasetRequest);
      return Arrays.stream(GoogleCloudResource.values())
          .map(
              resource -> {
                final GoogleRegion finalRegion;
                switch (resource) {
                  case FIRESTORE:
                    finalRegion = region.getRegionOrFallbackFirestoreRegion();
                    break;
                  case BUCKET:
                    finalRegion = region.getRegionOrFallbackBucketRegion();
                    break;
                  default:
                    finalRegion = region;
                }
                return new GoogleStorageResource(null, resource, finalRegion);
              })
          .collect(Collectors.toList());
    }

    @Override
    public GoogleRegion getGoogleRegionFromDatasetRequestModel(
        DatasetRequestModel datasetRequestModel) {
      return GoogleRegion.fromValueWithDefault(datasetRequestModel.getRegion());
    }
  }

  static class AzurePlatform extends CloudPlatformWrapper {
    static final AzurePlatform INSTANCE = new AzurePlatform();

    @Override
    public CloudPlatform getCloudPlatform() {
      return CloudPlatform.AZURE;
    }

    @Override
    public boolean isAzure() {
      return true;
    }

    @Override
    public void ensureValidRegion(String region, Errors errors) {
      ensureValidRegion(CloudPlatform.AZURE, region, AzureRegion.SUPPORTED_REGIONS, errors);
    }

    @Override
    public List<? extends StorageResource<?, ?>> createStorageResourceValues(
        DatasetRequestModel datasetRequest) {
      final AzureRegion region = AzureRegion.fromValueWithDefault(datasetRequest.getRegion());
      // TODO: once we no longer require GCP resources to back Azure datasets, stop concatenating
      return Stream.concat(
              Stream.of(
                      AzureCloudResource.APPLICATION_DEPLOYMENT, AzureCloudResource.STORAGE_ACCOUNT)
                  .map(resource -> new AzureStorageResource(null, resource, region)),
              CloudPlatformWrapper.getGoogleResourcesForAzure(datasetRequest).stream())
          .collect(Collectors.toList());
    }

    @Override
    public GoogleRegion getGoogleRegionFromDatasetRequestModel(
        DatasetRequestModel datasetRequestModel) {
      return GoogleRegion.fromValueWithDefault(datasetRequestModel.getGcpRegion());
    }
  }
}
