package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;

public interface DataLocationSelector {

    String projectIdForDataset(Dataset dataset, BillingProfileModel billingProfile);

    String projectIdForSnapshot(String snapshotName, Dataset dataset, BillingProfileModel billingProfile);

    String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile);

    String bucketForFile(Dataset dataset, BillingProfileModel billingProfile);
}
