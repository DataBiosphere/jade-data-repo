package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Profile({"test", "google"}) //TODO - Set this to our prod envs!
public class OneProjectPerDatasetIdSelector implements DataLocationSelector {
    private final GoogleResourceConfiguration resourceConfiguration;
    private final ResourceService resourceService;

    @Autowired
    public OneProjectPerDatasetIdSelector(GoogleResourceConfiguration resourceConfiguration,
                                          ResourceService resourceService) {
        this.resourceConfiguration = resourceConfiguration;
        this.resourceService = resourceService;
    }

    @Override
        public String projectIdForDataset(UUID datasetId, BillingProfileModel billingProfile) {
        return getSuffixForResourceId(datasetId.toString());
    }

    @Override
    public String projectIdForSnapshot(UUID snapshotId, BillingProfileModel billingProfile) {
        return getSuffixForResourceId(snapshotId.toString());
    }

    @Override
    public String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile) {
        UUID sourceDatasetBillingProfileId = dataset.getProjectResource().getProfileId();
        UUID requestedBillingProfileId = UUID.fromString(billingProfile.getId());
        if (sourceDatasetBillingProfileId.equals(requestedBillingProfileId)) {
            GoogleProjectResource project = resourceService.getProjectResource(dataset.getProjectResourceId());
            return project.getGoogleProjectId();
        } else {
            return getSuffixForResourceId(dataset.getId().toString()) + "-storage";
        }

    }

    @Override
    public String bucketForFile(Dataset dataset, BillingProfileModel billingProfile) {
        return projectIdForFile(dataset, billingProfile) + "-bucket";
    }

    //TODO - Is this how we would want to name them? This looks like "broad-jade-dev-UUID"
    private String getSuffixForResourceId(String resourceId) {
        String projectDatasetSuffix = "-" + resourceId.replaceAll("[^a-z-0-9]", "-");
        // The project id below is an application level prefix or, if that is empty, the name of the core project
        return resourceConfiguration.getDataProjectPrefixToUse() + projectDatasetSuffix;
    }
}
