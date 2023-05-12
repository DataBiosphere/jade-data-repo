package bio.terra.service.dataset;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class DatasetTest {

  @Test
  public void testHasDedicatedGcpServiceAccount() {
    var datasetSummary = new DatasetSummary();
    var projectResource = new GoogleProjectResource();
    var dataset = new Dataset(datasetSummary).projectResource(projectResource);

    datasetSummary.cloudPlatform(CloudPlatform.AZURE);
    assertFalse(
        "Azure datasets do not have a dedicated GCP service account",
        dataset.hasDedicatedGcpServiceAccount());

    datasetSummary.cloudPlatform(CloudPlatform.GCP);
    assertFalse(
        "GCP datasets by default use the general TDR service account",
        dataset.hasDedicatedGcpServiceAccount());

    projectResource.dedicatedServiceAccount(false);
    assertFalse(
        "GCP dataset uses the general TDR service account",
        dataset.hasDedicatedGcpServiceAccount());

    projectResource.dedicatedServiceAccount(true);
    assertTrue(
        "GCP datasets uses a dedicated GCP service account",
        dataset.hasDedicatedGcpServiceAccount());
  }
}
