package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.stairway.ShortUUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Profile({"terra", "google"})
public class OneProjectPerResourceSelector implements DataLocationSelector {
    private final GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    public OneProjectPerResourceSelector(GoogleResourceConfiguration resourceConfiguration) {
        this.resourceConfiguration = resourceConfiguration;
    }

    @Override
        public String projectIdForDataset() {
        return getNewProjectId();
    }

    @Override
    public String projectIdForSnapshot() {
        return getNewProjectId();
    }

    @Override
    public String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile) {
        UUID sourceDatasetBillingProfileId = dataset.getProjectResource().getProfileId();
        UUID requestedBillingProfileId = UUID.fromString(billingProfile.getId());
        if (sourceDatasetBillingProfileId.equals(requestedBillingProfileId)) {
            return dataset.getProjectResource().getGoogleProjectId();
        } else {
            return getNewProjectId();
        }

    }

    @Override
    public String bucketForFile(Dataset dataset, BillingProfileModel billingProfile) {
        return projectIdForFile(dataset, billingProfile) + "-bucket";
    }

    private String getNewProjectId() {
        String projectDatasetSuffix = "-" + ShortUUID.get();
        // The project id below is an application level prefix or, if that is empty, the name of the core project
        return resourceConfiguration.getDataProjectPrefixToUse() + projectDatasetSuffix;
    }
}
