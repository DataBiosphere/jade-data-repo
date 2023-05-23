package bio.terra.service.dataset;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import bio.terra.model.CloudPlatform;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("bio.terra.common.category.Unit")
public class DatasetTest {

  @Test
  void testHasDedicatedGcpServiceAccount() {
    var datasetSummary = new DatasetSummary();
    var projectResource =
        new GoogleProjectResource()
            .serviceAccount("testserviceaccount@gmail.com")
            .dedicatedServiceAccount(true);
    var dataset = new Dataset(datasetSummary).projectResource(projectResource);

    datasetSummary.cloudPlatform(CloudPlatform.AZURE);
    assertTrue(
        "Azure datasets have a dedicated GCP service account",
        dataset.hasDedicatedGcpServiceAccount());

    datasetSummary.cloudPlatform(CloudPlatform.GCP);
    assertTrue(
        "GCP datasets by default use a dedicated GCP service account",
        dataset.hasDedicatedGcpServiceAccount());

    projectResource.dedicatedServiceAccount(false);
    assertFalse(
        "GCP dataset uses the general TDR service account",
        dataset.hasDedicatedGcpServiceAccount());

    projectResource.dedicatedServiceAccount(true);
    assertTrue(
        "GCP dataset uses a dedicated GCP service account",
        dataset.hasDedicatedGcpServiceAccount());
  }
}
