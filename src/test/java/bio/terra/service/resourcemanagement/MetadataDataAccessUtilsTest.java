package bio.terra.service.resourcemanagement;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.category.Unit;
import bio.terra.model.AccessInfoAzureModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobSasUrlFactory;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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
    MockitoAnnotations.openMocks(this).close();
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
    when(resourceService.getStorageAccount(any(), any()))
        .thenReturn(java.util.Optional.of(storageAccountResource));

    when(azureBlobStorePdao.getTargetDataClientFactory(any(), any(), any(), anyBoolean()))
        .thenReturn(blobContainerClientFactory);

    when(blobContainerClientFactory.getBlobSasUrlFactory()).thenReturn(blobSasUrlFactory);
    when(blobSasUrlFactory.createSasUrlForBlob(eq("parquet"), any()))
        .thenReturn("blob.core.windows");
    when(blobSasUrlFactory.createSasUrlForBlob(eq("parquet/sample"), any()))
        .thenReturn("blob.core.windows/sample");

    AccessInfoAzureModel infoModel =
        metadataDataAccessUtils.accessInfoFromDataset(azureDataset).getAzure();

    Assert.assertEquals("test-dataset", infoModel.getDatasetName());
    Assert.assertEquals("michaelstorage.test-dataset", infoModel.getDatasetId());
    Assert.assertEquals("blob.core.windows", infoModel.getSignedUrl());
    Assert.assertEquals("blob.core.windows/sample", infoModel.getTables().get(0).getSignedUrl());
  }
}
