package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import java.util.UUID;

public interface DataLocationSelector {

    String projectIdForDataset(String datasetName, BillingProfileModel billingProfile);

    String projectIdForSnapshot(String snapshotName, BillingProfileModel billingProfile);

    String projectIdForFile(String datasetName, BillingProfileModel billingProfile);

    String bucketForFile(UUID datasetId, BillingProfileModel billingProfile);
}
