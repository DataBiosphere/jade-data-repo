package bio.terra.service.resourcemanagement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
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
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import java.util.List;
import java.util.UUID;
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
  private static final AuthenticatedUserRequest TEST_USER =
      new AuthenticatedUserRequest().subjectId("DatasetUnit").email("dataset@unit.com");

  @InjectMocks private MetadataDataAccessUtils metadataDataAccessUtils;

  @Mock private static ResourceService resourceService;

  @Mock private static AzureBlobStorePdao azureBlobStorePdao;
  @Mock private static ProfileService profileService;

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

    metadataDataAccessUtils =
        new MetadataDataAccessUtils(resourceService, azureBlobStorePdao, profileService);
  }

  @Test
  public void testAzureAccessInfo() {
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource().resourceId(UUID.randomUUID()).name("michaelstorage");
    when(resourceService.getDatasetStorageAccount(any(), any())).thenReturn(storageAccountResource);

    when(azureBlobStorePdao.signFile(
            any(),
            any(),
            eq("https://michaelstorage.blob.core.windows.net/metadata/parquet"),
            eq(ContainerType.METADATA),
            any()))
        .thenReturn("https://michaelstorage.blob.core.windows.net/metadata/parquet/signedUrl?sast");
    when(azureBlobStorePdao.signFile(
            any(),
            any(),
            eq("https://michaelstorage.blob.core.windows.net/metadata/parquet/sample"),
            eq(ContainerType.METADATA),
            any()))
        .thenReturn(
            "https://michaelstorage.blob.core.windows.net/metadata/parquet/sample/signedUrl?sast");

    AccessInfoParquetModel infoModel =
        metadataDataAccessUtils.accessInfoFromDataset(azureDataset, TEST_USER).getParquet();

    assertThat(infoModel.getDatasetName(), equalTo("test-dataset"));
    assertThat(infoModel.getDatasetId(), equalTo("michaelstorage.test-dataset"));
    assertThat(
        infoModel.getUrl(),
        equalTo("https://michaelstorage.blob.core.windows.net/metadata/parquet/signedUrl"));
    assertThat(infoModel.getSasToken(), equalTo("sast"));
  }
}
