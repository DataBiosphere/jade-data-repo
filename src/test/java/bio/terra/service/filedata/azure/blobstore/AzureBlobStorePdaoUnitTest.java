package bio.terra.service.filedata.azure.blobstore;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.filedata.FileIdService;
import bio.terra.service.filedata.azure.util.BlobCrl;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.FolderType;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AzureBlobStorePdaoUnitTest {
  @Mock private ProfileDao profileDao;
  @Mock private AzureContainerPdao azureContainerPdao;
  @Mock private AzureResourceConfiguration azureResourceConfiguration;
  @Mock private AzureResourceDao azureResourceDao;
  @Mock private AzureAuthService azureAuthService;
  @Mock private AzureBlobService azureBlobService;
  @Mock private FileIdService fileIdService;
  @Mock private GcsProjectFactory gcsProjectFactory;
  @Mock private GcsPdao gcsPdao;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final UUID BILLING_PROFILE_ID = UUID.randomUUID();
  private static final UUID SUBSCRIPTION_ID = UUID.randomUUID();
  private static final String RESOURCE_GROUP_NAME = "resourceGroupName";
  private static final String APPLICATION_DEPLOYMENT_ID =
      String.format("subscriptions/%s/resourceGroups/%s", SUBSCRIPTION_ID, RESOURCE_GROUP_NAME);
  private static final String SIGNED_URL =
      "https://signedUrl.blob.core.windows.net/uuid/blobPath?sig=SIGNATURE";

  private BillingProfileModel billingProfileModel;
  private AzureApplicationDeploymentResource applicationDeploymentResource;
  private AzureStorageAccountResource storageAccountResource;
  private AzureBlobStorePdao azureBlobStorePdao;

  @BeforeEach
  void setup() {
    billingProfileModel = new BillingProfileModel().id(BILLING_PROFILE_ID);
    azureBlobStorePdao =
        new AzureBlobStorePdao(
            profileDao,
            azureContainerPdao,
            azureResourceConfiguration,
            azureResourceDao,
            azureAuthService,
            azureBlobService,
            fileIdService,
            gcsProjectFactory,
            gcsPdao);

    applicationDeploymentResource =
        new AzureApplicationDeploymentResource()
            .azureApplicationDeploymentId(APPLICATION_DEPLOYMENT_ID)
            .azureResourceGroupName(RESOURCE_GROUP_NAME);
    storageAccountResource =
        new AzureStorageAccountResource()
            .applicationResource(applicationDeploymentResource)
            .name("storageAccountResourceName")
            .profileId(BILLING_PROFILE_ID);
  }

  void mocksForDeleteBlobParquet() {
    when(profileDao.getBillingProfileById(BILLING_PROFILE_ID)).thenReturn(billingProfileModel);
    when(azureContainerPdao.getDestinationContainerSignedUrl(any(), any(), any()))
        .thenReturn(SIGNED_URL);

    BlobCrl blobCrl = mock(BlobCrl.class);
    when(azureBlobService.getBlobCrl(any())).thenReturn(blobCrl);
    when(blobCrl.deleteBlobsWithPrefix(any())).thenReturn(true);
  }

  @Test
  void testDeleteBlobParquet() {
    mocksForDeleteBlobParquet();
    boolean result =
        azureBlobStorePdao.deleteBlobParquet(
            FolderType.SCRATCH, "parquetBlobPath", storageAccountResource, TEST_USER);
    assertTrue(result);
  }

  @Test
  void testDeleteScratchParquet() {
    mocksForDeleteBlobParquet();
    boolean result =
        azureBlobStorePdao.deleteScratchParquet(
            "parquetScratchPath", storageAccountResource, TEST_USER);
    assertTrue(result);
  }

  @Test
  void testDeleteMetadataParquet() {
    mocksForDeleteBlobParquet();
    boolean result =
        azureBlobStorePdao.deleteMetadataParquet(
            "parquetMetadataPath", storageAccountResource, TEST_USER);
    assertTrue(result);
  }
}
