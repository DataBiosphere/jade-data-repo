package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Primary
@Profile("!terra")
@Component
public class OneDataLocationSelector implements DataLocationSelector {

    private final GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    public OneDataLocationSelector(GoogleResourceConfiguration resourceConfiguration) {
        this.resourceConfiguration = resourceConfiguration;
    }

    private String oneProject() {
        return resourceConfiguration.getSingleDataProjectId();
    }

    @Override
    public String projectIdForDataset(String datasetName, BillingProfileModel billingProfile) {
        return oneProject();
    }

    @Override
    public String projectIdForSnapshot(String snapshotName, BillingProfileModel billingProfile) {
        return oneProject();
    }

    @Override
    public String projectIdForFile(String datasetName, BillingProfileModel billingProfile) {
        return oneProject();
    }

    @Override
    public String bucketForFile(String datasetName, BillingProfileModel billingProfile) {
        return oneProject() + "-bucket";
    }
}
