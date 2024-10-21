package bio.terra.service.resourcemanagement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class MetadataDataAccessUtilsTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private MetadataDataAccessUtils metadataDataAccessUtils;

  @Mock private static ResourceService resourceService;

  @Mock private static AzureBlobStorePdao azureBlobStorePdao;

  private Dataset azureDataset;

  @BeforeEach
  void setup() {
    UUID azureDatsetId = UUID.randomUUID();
    UUID billingProfileModelId = UUID.randomUUID();
    BillingProfileModel defaultProfileModel =
        new BillingProfileModel().profileName("default profile").id(billingProfileModelId);
    DatasetSummary azureDatasetSummary =
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
        new MetadataDataAccessUtils(
            resourceService, azureBlobStorePdao, mock(ProfileService.class));
  }

  @Test
  void testAzureAccessInfo() {
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource()
            .resourceId(UUID.randomUUID())
            .name("michaelstorage")
            .topLevelContainer("tlc");
    when(resourceService.getDatasetStorageAccount(any(), any())).thenReturn(storageAccountResource);

    when(azureBlobStorePdao.signFile(
            any(),
            any(),
            eq("https://michaelstorage.blob.core.windows.net/tlc/metadata/parquet"),
            any()))
        .thenReturn(
            "https://michaelstorage.blob.core.windows.net/tlc/metadata/parquet/signedUrl?sast");
    when(azureBlobStorePdao.signFile(
            any(),
            any(),
            eq("https://michaelstorage.blob.core.windows.net/tlc/metadata/parquet/sample"),
            any()))
        .thenReturn(
            "https://michaelstorage.blob.core.windows.net/tlc/metadata/parquet/sample/signedUrl?sast");

    AccessInfoParquetModel infoModel =
        metadataDataAccessUtils.accessInfoFromDataset(azureDataset, TEST_USER).getParquet();

    assertThat(infoModel.getDatasetName(), equalTo("test-dataset"));
    assertThat(infoModel.getDatasetId(), equalTo("michaelstorage.test-dataset"));
    assertThat(
        infoModel.getUrl(),
        equalTo("https://michaelstorage.blob.core.windows.net/tlc/metadata/parquet/signedUrl"));
    assertThat(infoModel.getSasToken(), equalTo("sast"));
  }
}
