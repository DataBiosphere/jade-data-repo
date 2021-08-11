package bio.terra.service.filedata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSObject;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.iam.exception.IamForbiddenException;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotProject;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@Category(Unit.class)
public class DrsServiceTest {

  @Mock private SnapshotService snapshotService;
  @Mock private FileService fileService;
  @Mock private IamService samService;
  @Mock private ResourceService resourceService;
  @Mock private ConfigurationService configurationService;
  @Mock private JobService jobService;
  @Mock private PerformanceLogger performanceLogger;
  @Mock private ProfileService profileService;
  @Mock private AzureResourceConfiguration azureResourceConfiguration;
  @Mock private AzureContainerPdao azureContainerPdao;
  @Mock private AzureBlobStorePdao azureBlobStorePdao;

  private final DrsIdService drsIdService = new DrsIdService(new ApplicationConfiguration());

  private DrsService drsService;

  private String drsObjectId;

  private FSFile azureFsFile;

  private FSFile googleFsFile;

  private UUID snapshotId;

  private final AuthenticatedUserRequest authUser =
      new AuthenticatedUserRequest().token(Optional.of("token"));

  @Before
  public void before() throws Exception {
    drsService =
        new DrsService(
            snapshotService,
            fileService,
            drsIdService,
            samService,
            resourceService,
            configurationService,
            jobService,
            performanceLogger,
            profileService,
            azureResourceConfiguration,
            azureContainerPdao,
            azureBlobStorePdao);
    when(jobService.getActivePodCount()).thenReturn(1);
    when(configurationService.getParameterValue(ConfigEnum.DRS_LOOKUP_MAX)).thenReturn(1);

    snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();
    UUID bucketResourceId = UUID.randomUUID();
    DrsId drsId = new DrsId("", "v1", snapshotId.toString(), fileId.toString());
    drsObjectId = drsId.toDrsObjectId();

    SnapshotProject snapshotProject = new SnapshotProject();
    when(snapshotService.retrieveAvailableSnapshotProject(snapshotId)).thenReturn(snapshotProject);

    azureFsFile =
        new FSFile()
            .createdDate(Instant.now())
            .description("description")
            .path("file.txt")
            .cloudPath("https://core.windows.net/blahblah")
            .size(100L)
            .fileId(fileId)
            .bucketResourceId(bucketResourceId.toString());
    when(fileService.lookupSnapshotFSItem(snapshotProject, drsId.getFsObjectId(), 1))
        .thenReturn(azureFsFile);

    googleFsFile =
        new FSFile()
            .createdDate(Instant.now())
            .description("description")
            .path("file.txt")
            .cloudPath("gs://path/to/file.txt")
            .size(100L)
            .fileId(fileId)
            .bucketResourceId(bucketResourceId.toString());
    when(fileService.lookupSnapshotFSItem(snapshotProject, drsId.getFsObjectId(), 1))
        .thenReturn(googleFsFile);

    GoogleBucketResource bucketResource =
        new GoogleBucketResource().region(GoogleRegion.DEFAULT_GOOGLE_REGION);
    when(resourceService.lookupBucketMetadata(any())).thenReturn(bucketResource);
  }

  @Test
  public void testLookupPositive() {
    DRSObject drsObject = drsService.lookupObjectByDrsId(authUser, drsObjectId, false);
    assertThat(drsObject.getId(), is(drsObjectId));
    assertThat(drsObject.getSize(), is(googleFsFile.getSize()));
    assertThat(drsObject.getName(), is(googleFsFile.getPath()));
  }

  @Test
  public void testLookupNegative() {
    doThrow(IamForbiddenException.class)
        .when(samService)
        .verifyAuthorization(
            authUser, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);
    assertThrows(
        IamForbiddenException.class,
        () -> drsService.lookupObjectByDrsId(authUser, drsObjectId, false));
  }

  @Test
  public void testSignAzureUrl() throws InterruptedException {
    UUID defaultProfileModelId = UUID.randomUUID();
    Dataset dataset =
        new Dataset(
            new DatasetSummary()
                .billingProfiles(
                    List.of(
                        new BillingProfileModel()
                            .id(defaultProfileModelId)
                            .cloudPlatform(CloudPlatform.AZURE)))
                .defaultProfileId(defaultProfileModelId));
    Snapshot snapshot =
        new Snapshot().snapshotSources(List.of(new SnapshotSource().dataset(dataset)));
    DrsId drsId = drsIdService.fromObjectId(drsObjectId);
    when(snapshotService.retrieve(UUID.fromString(drsId.getSnapshotId()))).thenReturn(snapshot);
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource().region(AzureRegion.DEFAULT_AZURE_REGION);
    when(fileService.lookupSnapshotFSItem(any(), any(), eq(1))).thenReturn(azureFsFile);
    when(resourceService.lookupStorageAccountMetadata(any())).thenReturn(storageAccountResource);
    String urlString = "https://blahblah.core.windows.com/data/file.json";
    when(azureBlobStorePdao.signFile(any(), any(), any())).thenReturn(urlString);

    DRSAccessURL result = drsService.getAccessUrlForObjectId(authUser, drsObjectId, "az-centralus");
    assertEquals(urlString, result.getUrl());
  }
}
