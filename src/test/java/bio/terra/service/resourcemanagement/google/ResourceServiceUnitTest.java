package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetStorageAccountDao;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.AzureDataLocationSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import bio.terra.service.snapshot.SnapshotStorageAccountDao;
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
class ResourceServiceUnitTest {

  private ResourceService resourceService;

  @Mock private GoogleBucketService bucketService;

  @Mock private AzureStorageAccountService storageAccountService;

  @Mock private DatasetStorageAccountDao datasetStorageAccountDao;

  @Mock private AzureApplicationDeploymentService applicationDeploymentService;

  private final UUID billingProfileId = UUID.randomUUID();

  private final UUID datasetId = UUID.randomUUID();

  private final DatasetSummary datasetSummary =
      new DatasetSummary()
          .storage(
              List.of(
                  new GoogleStorageResource(
                      datasetId, GoogleCloudResource.BUCKET, GoogleRegion.DEFAULT_GOOGLE_REGION),
                  new GoogleStorageResource(
                      datasetId, GoogleCloudResource.FIRESTORE, GoogleRegion.DEFAULT_GOOGLE_REGION),
                  new AzureStorageResource(
                      datasetId,
                      AzureCloudResource.STORAGE_ACCOUNT,
                      AzureRegion.DEFAULT_AZURE_REGION)));
  private final Dataset dataset = new Dataset(datasetSummary).id(datasetId);

  private final BillingProfileModel profileModel =
      ProfileFixtures.randomAzureBillingProfile().id(billingProfileId);
  private final UUID applicationId = UUID.randomUUID();
  private final UUID storageAccountId = UUID.randomUUID();
  private static final String MANAGED_RESOURCE_GROUP_NAME = "mgd-grp-1";
  private static final String STORAGE_ACCOUNT_NAME = "sa";
  private final AzureApplicationDeploymentResource applicationResource =
      new AzureApplicationDeploymentResource()
          .id(applicationId)
          .azureApplicationDeploymentName(profileModel.getApplicationDeploymentName())
          .azureResourceGroupName(MANAGED_RESOURCE_GROUP_NAME)
          .profileId(billingProfileId);
  private final AzureStorageAccountResource storageAccountResource =
      new AzureStorageAccountResource()
          .resourceId(storageAccountId)
          .name(STORAGE_ACCOUNT_NAME)
          .applicationResource(applicationResource);

  @BeforeEach
  void setup() {
    resourceService =
        new ResourceService(
            mock(AzureDataLocationSelector.class),
            mock(GoogleProjectService.class),
            bucketService,
            applicationDeploymentService,
            storageAccountService,
            mock(SamConfiguration.class),
            datasetStorageAccountDao,
            mock(SnapshotStorageAccountDao.class),
            mock(GoogleResourceManagerService.class),
            mock(AzureContainerPdao.class),
            mock(ProfileDao.class));
  }

  @Test
  void testGrabBucket() throws Exception {
    GoogleProjectResource projectResource = new GoogleProjectResource();
    GoogleBucketResource bucketResource =
        new GoogleBucketResource().projectResource(projectResource);
    when(bucketService.getOrCreateBucket(
            any(), any(), any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(bucketResource);
    dataset.projectResource(projectResource);

    GoogleBucketResource foundBucket =
        resourceService.getOrCreateBucketForFile(dataset, projectResource, "flightId", null);
    assertThat(foundBucket, is(bucketResource));
  }

  @Test
  void testGetOrCreateStorageAccount() throws Exception {
    when(storageAccountService.getOrCreateStorageAccount(any(), any(), any(), any(), any()))
        .thenReturn(storageAccountResource);
    when(storageAccountService.getStorageAccountResourceById(storageAccountId, true))
        .thenReturn(storageAccountResource);
    when(applicationDeploymentService.getOrRegisterApplicationDeployment(any()))
        .thenReturn(applicationResource);
    when(datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(dataset.getId()))
        .thenReturn(List.of(storageAccountId));
    AzureStorageAccountResource createdStorageAccount =
        resourceService.getOrCreateDatasetStorageAccount(dataset, profileModel, "flightId");
    assertThat(createdStorageAccount, is(storageAccountResource));
  }
}
