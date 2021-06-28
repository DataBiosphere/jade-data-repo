package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import com.google.common.hash.Hashing;

import java.util.UUID;

@Component
@Profile({"terra", "google"})
public class OneProjectPerResourceSelector implements DataLocationSelector {
    private final GoogleResourceConfiguration resourceConfiguration;
    private final DatasetBucketDao datasetBucketDao;
    private static final String GS_PROJECT_PATTERN = "[a-z0-9\\-]{6,30}";
    private static final String GS_BUCKET_PATTERN = "[a-z0-9\\-\\.\\_]{3,63}";
    /**
     * *Borrowed from terra-resource-buffer*
     * The size of project when generating random characters. Choose size as 8 based on AoU's
     * historical experience, increase if 8 is not enough for a pool's naming.
     */
    static final int RANDOM_ID_SIZE = 8;

    @Autowired
    public OneProjectPerResourceSelector(GoogleResourceConfiguration resourceConfiguration,
                                         DatasetBucketDao datasetBucketDao) {
        this.resourceConfiguration = resourceConfiguration;
        this.datasetBucketDao = datasetBucketDao;
    }

    @Override
        public String projectIdForDataset() throws GoogleResourceNamingException {
        return getNewProjectId();
    }

    @Override
    public String projectIdForSnapshot() throws GoogleResourceNamingException {
        return getNewProjectId();
    }

    @Override
    public String projectIdForFile(
        Dataset dataset, String sourceDatasetGoogleProjectId, BillingProfileModel billingProfile)
        throws GoogleResourceException, GoogleResourceNamingException {
        // Case 1
        // Condition: Requested billing profile matches source dataset's billing profile
        // Action: Re-use dataset's project
        UUID sourceDatasetBillingProfileId = dataset.getProjectResource().getProfileId();
        UUID requestedBillingProfileId = billingProfile.getId();
        if (sourceDatasetBillingProfileId.equals(requestedBillingProfileId)) {
            return sourceDatasetGoogleProjectId;
        }

        // Case 2
        // Condition: Ingest Billing profile != source dataset billing profile && project *already exists*
        // Action: Re-use bucket's project
        String bucketGoogleProjectId = datasetBucketDao.getProjectResourceForBucket(dataset.getId(),
            billingProfile.getId());
        if (bucketGoogleProjectId != null) {
            return bucketGoogleProjectId;
        }

        //Case 3 -
        // Condition: Ingest Billing profile != source dataset billing profile && project does NOT exist
        // Action: Create new project
        return getNewProjectId();
    }

    @Override
    public String bucketForFile(String projectId)
        throws GoogleResourceNamingException {
        String bucketName = projectId + "-bucket";
        if (!bucketName.matches(GS_BUCKET_PATTERN)) {
            throw new GoogleResourceNamingException("Google bucket name '" + bucketName +
                "' does not match required pattern for google buckets.");
        }
        return bucketName;
    }

    private String getNewProjectId() throws GoogleResourceNamingException {
        String projectDatasetSuffix = "-" + generateRandomId();

        // The project id below is an application level prefix or, if that is empty, the name of the core project
        String projectId = resourceConfiguration.getDataProjectPrefixToUse() + projectDatasetSuffix;

        //Since the prefix can be set adhoc, let's check that the project name matches Google's required pattern
        if (!projectId.matches(GS_PROJECT_PATTERN)) {
            throw new GoogleResourceNamingException("Google project name '" + projectId +
                    "' does not match required pattern for google projects.");
        }
        return projectId;
    }

    //Borrowed from terra-resource-buffer, to mimic their project naming behavior before we switch
    private String generateRandomId() {
        return Hashing.sha256()
            .hashUnencodedChars(UUID.randomUUID().toString())
            .toString()
            .substring(0, RANDOM_ID_SIZE);
    }
}
