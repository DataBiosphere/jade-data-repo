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
@Profile({"sh"})
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
        public String projectIdForDataset(Dataset dataset, BillingProfileModel billingProfile) {
        return getSuffixForDatasetId(dataset.getId().toString());
    }

    @Override
    public String projectIdForSnapshot(String snapshotName, Dataset dataset, BillingProfileModel billingProfile) {
        GoogleProjectResource project = resourceService.getProjectResource(dataset.getProjectResourceId());
        return project.getGoogleProjectId();
    }

    @Override
    public String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile) {
        GoogleProjectResource project = resourceService.getProjectResource(dataset.getProjectResourceId());
        return project.getGoogleProjectId();
    }

    @Override
    public String bucketForFile(Dataset dataset, BillingProfileModel billingProfile) {
        return projectIdForFile(dataset, billingProfile) + "-bucket";
    }

    private String getSuffixForDatasetId(String datasetId) {
        String projectDatasetSuffix = "-" + datasetId.replaceAll("[^a-z-0-9]", "-");
        // The project id below is an application level prefix or, if that is empty, the name of the core project
        return resourceConfiguration.getDataProjectPrefixToUse() + projectDatasetSuffix;
    }
}
