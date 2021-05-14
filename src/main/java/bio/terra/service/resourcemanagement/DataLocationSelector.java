package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;

import java.util.UUID;

public interface DataLocationSelector {

    String projectIdForDataset(UUID datasetId, BillingProfileModel billingProfile);

    String projectIdForSnapshot(UUID snapshotId, BillingProfileModel billingProfile);

    String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile);

    String bucketForFile(Dataset dataset, BillingProfileModel billingProfile);
}
