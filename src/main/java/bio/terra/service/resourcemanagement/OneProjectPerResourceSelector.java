package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
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
    private final ResourceService resourceService;

    @Autowired
    public OneProjectPerResourceSelector(GoogleResourceConfiguration resourceConfiguration,
                                          ResourceService resourceService) {
        this.resourceConfiguration = resourceConfiguration;
        this.resourceService = resourceService;
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
            GoogleProjectResource project = resourceService.getProjectResource(dataset.getProjectResourceId());
            return project.getGoogleProjectId();
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
