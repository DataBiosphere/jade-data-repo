package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;

public interface DataLocationSelector {

    String projectIdForDataset(String datasetName, BillingProfileModel billingProfile);

    String projectIdForSnapshot(String snapshotName, BillingProfileModel billingProfile);

    String projectIdForFile(String datasetName, BillingProfileModel billingProfile);

    String bucketForFile(String datasetName, BillingProfileModel billingProfile);
}
