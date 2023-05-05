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
  public void testShouldValidateIngesterFileAccess() {
    var datasetSummary = new DatasetSummary();
    var projectResource = new GoogleProjectResource();
    var dataset = new Dataset(datasetSummary).projectResource(projectResource);

    datasetSummary.cloudPlatform(CloudPlatform.AZURE);
    assertFalse(
        "Azure datasets do not validate that user can access ingested files",
        dataset.shouldValidateIngesterFileAccess());

    datasetSummary.cloudPlatform(CloudPlatform.GCP);
    assertTrue(
        "GCP datasets by default validate that user can access ingested files",
        dataset.shouldValidateIngesterFileAccess());

    projectResource.dedicatedServiceAccount(false);
    assertTrue(
        "GCP datasets with the general TDR SA validate that user can access ingested files",
        dataset.shouldValidateIngesterFileAccess());

    projectResource.dedicatedServiceAccount(true);
    assertFalse(
        "GCP datasets with dedicated SAs do not validate that user can access ingested files",
        dataset.shouldValidateIngesterFileAccess());
  }
}
