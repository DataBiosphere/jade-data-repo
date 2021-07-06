package bio.terra.service.resourcemanagement;

import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile({"terra", "google"})
public class DataLocationSelector {
  private final GoogleResourceConfiguration resourceConfiguration;
  private final DatasetBucketDao datasetBucketDao;
  private final BufferService bufferService;
  private static final String GS_BUCKET_PATTERN = "[a-z0-9\\-\\.\\_]{3,63}";

  @Autowired
  public DataLocationSelector(
      GoogleResourceConfiguration resourceConfiguration,
      DatasetBucketDao datasetBucketDao,
      BufferService bufferService) {
    this.resourceConfiguration = resourceConfiguration;
    this.datasetBucketDao = datasetBucketDao;
    this.bufferService = bufferService;
  }

  public String projectIdForFile(
      Dataset dataset, String sourceDatasetGoogleProjectId, BillingProfileModel billingProfile)
      throws GoogleResourceException {
    // Case 1
    // Condition: Requested billing profile matches source dataset's billing profile
    // Action: Re-use dataset's project
    UUID sourceDatasetBillingProfileId = dataset.getProjectResource().getProfileId();
    UUID requestedBillingProfileId = billingProfile.getId();
    if (sourceDatasetBillingProfileId.equals(requestedBillingProfileId)) {
      return sourceDatasetGoogleProjectId;
    }

    // Case 2
    // Condition: Ingest Billing profile != source dataset billing profile && project *already
    // exists*
    // Action: Re-use bucket's project
    String bucketGoogleProjectId =
        datasetBucketDao.getProjectResourceForBucket(dataset.getId(), billingProfile.getId());
    if (bucketGoogleProjectId != null) {
      return bucketGoogleProjectId;
    }

    // Case 3 -
    // Condition: Ingest Billing profile != source dataset billing profile && project does NOT exist
    // Action: Request a new project
    HandoutRequestBody request =
        new HandoutRequestBody().handoutRequestId(UUID.randomUUID().toString());
    ResourceInfo resource = bufferService.handoutResource(request);
    return resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
  }

  public String bucketForFile(String projectId) throws GoogleResourceNamingException {
    String bucketName = projectId + "-bucket";
    if (!bucketName.matches(GS_BUCKET_PATTERN)) {
      throw new GoogleResourceNamingException(
          "Google bucket name '"
              + bucketName
              + "' does not match required pattern for google buckets.");
    }
    return bucketName;
  }
}
