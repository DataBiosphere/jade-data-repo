package bio.terra.service.resourcemanagement;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;

@Category(Unit.class)
public class MetadataDataAccessUtilsTest {

  @InjectMocks
  private MetadataDataAccessUtils metadataDataAccessUtils;

  private Dataset azureDataset;
  private DatasetSummary azureDatasetSummary;

  public void setup() throws Exception {
    UUID azureDatsetId = UUID.randomUUID();
    azureDatasetSummary = new DatasetSummary().storage(List.of(new AzureStorageResource(
        azureDatsetId,
        AzureCloudResource.STORAGE_ACCOUNT,
        AzureRegion.DEFAULT_AZURE_REGION)));
    azureDataset = new Dataset(azureDatasetSummary);

    MockitoAnnotations.openMocks(metadataDataAccessUtils).close();
  }

  @Test
  public void testAzureAccessInfo() {
    metadataDataAccessUtils.accessInfoFromDataset(azureDataset);
  }

}
