package bio.terra.service.dataset;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class DatasetTest {

  @Test
  void testHasDedicatedGcpServiceAccount() {
    var datasetSummary = new DatasetSummary();
    var projectResource = new GoogleProjectResource();
    var dataset = new Dataset(datasetSummary).projectResource(projectResource);

    datasetSummary.cloudPlatform(CloudPlatform.AZURE);
    assertFalse(
        dataset.hasDedicatedGcpServiceAccount(),
        "Azure datasets do not have a dedicated GCP service account");

    datasetSummary.cloudPlatform(CloudPlatform.GCP);
    assertFalse(
        dataset.hasDedicatedGcpServiceAccount(),
        "GCP datasets by default use the general TDR service account");

    projectResource.dedicatedServiceAccount(false);
    assertFalse(
        dataset.hasDedicatedGcpServiceAccount(),
        "GCP dataset uses the general TDR service account");

    projectResource.dedicatedServiceAccount(true);
    assertTrue(
        dataset.hasDedicatedGcpServiceAccount(),
        "GCP dataset uses a dedicated GCP service account");
  }
}
