package bio.terra.datarepo.service.filedata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.app.configuration.ApplicationConfiguration;
import bio.terra.datarepo.app.logging.PerformanceLogger;
import bio.terra.datarepo.app.model.GoogleRegion;
import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.model.DRSObject;
import bio.terra.datarepo.service.configuration.ConfigEnum;
import bio.terra.datarepo.service.configuration.ConfigurationService;
import bio.terra.datarepo.service.iam.AuthenticatedUserRequest;
import bio.terra.datarepo.service.iam.IamAction;
import bio.terra.datarepo.service.iam.IamResourceType;
import bio.terra.datarepo.service.iam.IamService;
import bio.terra.datarepo.service.iam.exception.IamForbiddenException;
import bio.terra.datarepo.service.job.JobService;
import bio.terra.datarepo.service.resourcemanagement.ResourceService;
import bio.terra.datarepo.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.datarepo.service.snapshot.SnapshotProject;
import bio.terra.datarepo.service.snapshot.SnapshotService;
import java.time.Instant;
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

  private final DrsIdService drsIdService = new DrsIdService(new ApplicationConfiguration());

  private DrsService drsService;

  private String drsObjectId;

  private FSFile fsFile;

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
            performanceLogger);
    when(jobService.getActivePodCount()).thenReturn(1);
    when(configurationService.getParameterValue(ConfigEnum.DRS_LOOKUP_MAX)).thenReturn(1);

    snapshotId = UUID.randomUUID();
    UUID fileId = UUID.randomUUID();
    DrsId drsId = new DrsId("", "v1", snapshotId.toString(), fileId.toString());
    drsObjectId = drsId.toDrsObjectId();

    SnapshotProject snapshotProject = new SnapshotProject();
    when(snapshotService.retrieveAvailableSnapshotProject(snapshotId)).thenReturn(snapshotProject);

    fsFile =
        new FSFile()
            .createdDate(Instant.now())
            .description("description")
            .path("file.txt")
            .gspath("gs://path/to/file.txt")
            .size(100L)
            .fileId(fileId);
    when(fileService.lookupSnapshotFSItem(snapshotProject, drsId.getFsObjectId(), 1))
        .thenReturn(fsFile);

    GoogleBucketResource bucketResource =
        new GoogleBucketResource().region(GoogleRegion.DEFAULT_GOOGLE_REGION);
    when(resourceService.lookupBucketMetadata(any())).thenReturn(bucketResource);
  }

  @Test
  public void testLookupPositive() {
    DRSObject drsObject = drsService.lookupObjectByDrsId(authUser, drsObjectId, false);
    assertThat(drsObject.getId(), is(drsObjectId));
    assertThat(drsObject.getSize(), is(fsFile.getSize()));
    assertThat(drsObject.getName(), is(fsFile.getPath()));
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
}
