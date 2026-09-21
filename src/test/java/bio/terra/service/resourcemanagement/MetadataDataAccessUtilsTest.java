package bio.terra.service.resourcemanagement;

import static org.mockito.Mockito.mock;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.profile.ProfileService;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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
}
