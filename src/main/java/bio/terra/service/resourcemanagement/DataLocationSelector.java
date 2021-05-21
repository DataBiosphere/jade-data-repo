package bio.terra.service.resourcemanagement;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.exception.GoogleProjectNamingException;

public interface DataLocationSelector {

    String projectIdForDataset() throws GoogleProjectNamingException;

    String projectIdForSnapshot() throws GoogleProjectNamingException;

    String projectIdForFile(Dataset dataset, BillingProfileModel billingProfile) throws GoogleProjectNamingException;

    String bucketForFile(Dataset dataset, BillingProfileModel billingProfile) throws GoogleProjectNamingException;
}
