package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.exception.GoogleProjectNamingException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;

public interface DataLocationSelector {

    String projectIdForDataset() throws GoogleProjectNamingException;

    String projectIdForSnapshot() throws GoogleProjectNamingException;

    String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile)
        throws GoogleResourceException, GoogleProjectNamingException;

    String bucketForFile(Dataset dataset, BillingProfileModel billingProfile) throws GoogleProjectNamingException;
}
