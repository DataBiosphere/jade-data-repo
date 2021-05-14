package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Primary
@Profile({"!terra", "!test"})
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
    public String projectIdForDataset(UUID datsetId, BillingProfileModel billingProfile) {
        return oneProject();
    }

    @Override
    public String projectIdForSnapshot(UUID snapshotId, BillingProfileModel billingProfile) {
        return oneProject();
    }

    @Override
    public String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile) {
        return oneProject();
    }

    @Override
    public String bucketForFile(Dataset dataset, BillingProfileModel billingProfile) {
        return oneProject() + "-bucket";
    }
}
