package bio.terra.service.resourcemanagement.google;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

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
import bio.terra.service.resourcemanagement.AzureDataLocationSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ResourceServiceUnitTest {

  @InjectMocks private ResourceService resourceService;

  @Mock private GoogleBucketService bucketService;

  @Mock private AzureStorageAccountService storageAccountService;

  @Mock private DatasetStorageAccountDao datasetStorageAccountDao;

  @Mock private AzureApplicationDeploymentService applicationDeploymentService;

  @Mock private GoogleProjectService googleProjectService;

  @Mock private AzureDataLocationSelector azureDataLocationSelector;

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

  private final GoogleProjectResource projectResource =
      new GoogleProjectResource().profileId(billingProfileId).googleProjectId("randProjectId");

  private final UUID bucketName = UUID.randomUUID();
  private final UUID bucketId = UUID.randomUUID();
  private final GoogleBucketResource bucketResource =
      new GoogleBucketResource()
          .resourceId(bucketId)
          .name(bucketName.toString())
          .projectResource(projectResource);

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

  public void setup() throws InterruptedException {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testGrabBucket() throws Exception {
    when(bucketService.getOrCreateBucket(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(bucketResource);
    dataset.projectResource(projectResource);

    GoogleBucketResource foundBucket =
        resourceService.getOrCreateBucketForFile(dataset, projectResource, "flightId", null);
    Assert.assertEquals(bucketResource, foundBucket);
  }

  @Test
  public void testGetOrCreateStorageAccount() throws Exception {
    when(storageAccountService.getOrCreateStorageAccount(any(), any(), any(), any()))
        .thenReturn(storageAccountResource);
    when(storageAccountService.getStorageAccountResourceById(storageAccountId, true))
        .thenReturn(storageAccountResource);
    when(applicationDeploymentService.getOrRegisterApplicationDeployment(any()))
        .thenReturn(applicationResource);
    when(datasetStorageAccountDao.getStorageAccountResourceIdForDatasetId(dataset.getId()))
        .thenReturn(List.of(storageAccountId));
    AzureStorageAccountResource createdStorageAccount =
        resourceService.getOrCreateDatasetStorageAccount(dataset, profileModel, "flightId");
    Assert.assertEquals(storageAccountResource, createdStorageAccount);
  }
}
