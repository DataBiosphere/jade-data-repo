package bio.terra.service.resourcemanagement.google;

import static bio.terra.service.resourcemanagement.ResourceService.SNAPSHOT_GCP_IAM_ROLES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.AzureStorageResource;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.ResourceService;
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
class ResourceServiceTest {

  private ResourceService resourceService;

  @Mock private GoogleBucketService bucketService;

  @Mock private GoogleResourceManagerService resourceManagerService;

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

  @BeforeEach
  void setup() {
    resourceService =
        new ResourceService(
            mock(GoogleProjectService.class),
            bucketService,
            mock(SamConfiguration.class),
            resourceManagerService,
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
  void assignRolesForSnapshot() throws InterruptedException {
    String dataProject = "test-project";
    List<String> policyEmails = List.of("test-email@example.com");

    resourceService.assignRolesForSnapshot(dataProject, policyEmails);

    // Verify that the IAM permissions were updated
    verify(resourceManagerService)
        .updateIamPermissions(
            argThat(
                arg ->
                    arg.containsKey(SNAPSHOT_GCP_IAM_ROLES.get(0))
                        && arg.containsKey(SNAPSHOT_GCP_IAM_ROLES.get(1))),
            eq(dataProject),
            eq(GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS));
  }

  @Test
  void revokeRolesForSnapshot() throws InterruptedException {
    String dataProject = "test-project";
    List<String> policyEmails = List.of("test-email@example.com");

    resourceService.revokeRolesForSnapshot(dataProject, policyEmails);

    // Verify that the IAM permissions were updated
    verify(resourceManagerService)
        .updateIamPermissions(
            argThat(
                arg ->
                    arg.containsKey(SNAPSHOT_GCP_IAM_ROLES.get(0))
                        && arg.containsKey(SNAPSHOT_GCP_IAM_ROLES.get(1))),
            eq(dataProject),
            eq(GoogleProjectService.PermissionOp.REVOKE_PERMISSIONS));
  }
}
