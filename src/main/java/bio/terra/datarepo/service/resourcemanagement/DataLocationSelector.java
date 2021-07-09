package bio.terra.datarepo.service.resourcemanagement;

import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.resourcemanagement.exception.GoogleResourceException;
import bio.terra.datarepo.service.resourcemanagement.exception.GoogleResourceNamingException;

public interface DataLocationSelector {

  String projectIdForDataset() throws GoogleResourceNamingException;

  String projectIdForSnapshot() throws GoogleResourceNamingException;

  String projectIdForFile(
      Dataset dataset, String sourceDatasetGoogleProjectId, BillingProfileModel billingProfile)
      throws GoogleResourceException, GoogleResourceNamingException;

  String bucketForFile(String projectId) throws GoogleResourceNamingException;
}
