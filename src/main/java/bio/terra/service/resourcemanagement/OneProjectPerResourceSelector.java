package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.resourcemanagement.exception.GoogleProjectNamingException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
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
        public String projectIdForDataset() throws GoogleProjectNamingException {
        return getNewProjectId();
    }

    @Override
    public String projectIdForSnapshot() throws GoogleProjectNamingException {
        return getNewProjectId();
    }

    @Override
    public String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile)
        throws GoogleResourceException, GoogleProjectNamingException {
        String googleProjectId = datasetBucketDao.getProjectResourceForBucket(dataset.getId(),
            UUID.fromString(billingProfile.getId()));
        if (googleProjectId != null) {
            return googleProjectId;
        } else {
            return getNewProjectId();
        }
    }

    @Override
    public String bucketForFile(Dataset dataset, BillingProfileModel billingProfile)
        throws GoogleProjectNamingException {
        return projectIdForFile(dataset, billingProfile) + "-bucket";
    }

    private String getNewProjectId() throws GoogleProjectNamingException {
        String projectDatasetSuffix = "-" + generateRandomId();

        // The project id below is an application level prefix or, if that is empty, the name of the core project
        String projectId = resourceConfiguration.getDataProjectPrefixToUse() + projectDatasetSuffix;

        //Since the prefix can be set adhoc, let's check that the project name matches Google's required pattern
        if (!projectId.matches(GS_PROJECT_PATTERN)) {
            throw new GoogleProjectNamingException("Google project name '" + projectId +
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
