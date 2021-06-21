package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNamingException;

public interface DataLocationSelector {

    String projectIdForDataset() throws GoogleResourceNamingException;

    String projectIdForSnapshot() throws GoogleResourceNamingException;

    String projectIdForFile(Dataset dataset,
                            String sourceDatasetGoogleProjectId,
                            BillingProfileModel billingProfile)
        throws GoogleResourceException, GoogleResourceNamingException;

    String bucketForFile(String projectId)
        throws GoogleResourceNamingException;
}
