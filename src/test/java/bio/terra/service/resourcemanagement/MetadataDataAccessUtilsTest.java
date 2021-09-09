package bio.terra.service.resourcemanagement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.category.Unit;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobSasUrlFactory;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@Category(Unit.class)
public class MetadataDataAccessUtilsTest {

  @InjectMocks private MetadataDataAccessUtils metadataDataAccessUtils;

  @Mock private static ResourceService resourceService;

  @Mock private static AzureBlobStorePdao azureBlobStorePdao;

  @Mock private BlobContainerClientFactory blobContainerClientFactory;

  @Mock private BlobSasUrlFactory blobSasUrlFactory;

  private Dataset azureDataset;
  private DatasetSummary azureDatasetSummary;
  private BillingProfileModel defaultProfileModel;

  @Before
  public void setup() throws Exception {
    UUID azureDatsetId = UUID.randomUUID();
    UUID billingProfileModelId = UUID.randomUUID();
    defaultProfileModel =
        new BillingProfileModel().profileName("default profile").id(billingProfileModelId);
    azureDatasetSummary =
        new DatasetSummary()
            .storage(
                List.of(
                    new AzureStorageResource(
                        azureDatsetId,
                        AzureCloudResource.STORAGE_ACCOUNT,
                        AzureRegion.DEFAULT_AZURE_REGION)))
            .defaultProfileId(billingProfileModelId)
            .billingProfiles(List.of(defaultProfileModel));
    DatasetTable sampleTable = new DatasetTable().id(UUID.randomUUID()).name("sample");
    azureDataset =
        new Dataset(azureDatasetSummary).tables(List.of(sampleTable)).name("test-dataset");

    metadataDataAccessUtils = new MetadataDataAccessUtils(resourceService, azureBlobStorePdao);
  }

  @Test
  public void testAzureAccessInfo() {
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource().resourceId(UUID.randomUUID()).name("michaelstorage");
    when(resourceService.getStorageAccount(any(), any())).thenReturn(storageAccountResource);

    when(azureBlobStorePdao.signFile(
            any(),
            any(),
            eq("https://michaelstorage.blob.core.windows.net/metadata/parquet"),
            eq(ContainerType.METADATA),
            any(),
            any()))
        .thenReturn("https://michaelstorage.blob.core.windows.net/metadata/parquet/signedUrl");
    when(azureBlobStorePdao.signFile(
            any(),
            any(),
            eq("https://michaelstorage.blob.core.windows.net/metadata/parquet/sample"),
            eq(ContainerType.METADATA),
            any(),
            any()))
        .thenReturn(
            "https://michaelstorage.blob.core.windows.net/metadata/parquet/sample/signedUrl");

    AccessInfoParquetModel infoModel =
        metadataDataAccessUtils.accessInfoFromDataset(azureDataset).getParquet();

    Assert.assertEquals("test-dataset", infoModel.getDatasetName());
    Assert.assertEquals("michaelstorage.test-dataset", infoModel.getDatasetId());
    Assert.assertEquals(
        "https://michaelstorage.blob.core.windows.net/metadata/parquet/signedUrl",
        infoModel.getSignedUrl());
    Assert.assertEquals(
        "https://michaelstorage.blob.core.windows.net/metadata/parquet/sample/signedUrl",
        infoModel.getTables().get(0).getSignedUrl());
  }
}
