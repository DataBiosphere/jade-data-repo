package bio.terra.service.filedata;

import static bio.terra.service.filedata.DrsService.URL_TTL;
import static bio.terra.service.filedata.google.gcs.GcsConstants.REQUESTED_BY_QUERY_PARAM;
import static bio.terra.service.filedata.google.gcs.GcsConstants.USER_PROJECT_QUERY_PARAM;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.app.controller.exception.TooManyRequestsException;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.TestUtils;
import bio.terra.common.UriUtils;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessMethod.TypeEnum;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSAuthorizations;
import bio.terra.model.DRSAuthorizations.SupportedTypesEnum;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSObject;
import bio.terra.model.DRSPassportRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.DrsDao.DrsAlias;
import bio.terra.service.filedata.DrsService.SnapshotCacheResult;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import bio.terra.service.filedata.exception.InvalidDrsObjectException;
import bio.terra.service.filedata.google.gcs.GcsConstants;
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotProject;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.SnapshotSummary;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.class)
@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
@SuppressFBWarnings(
    value = "DMI",
    justification =
        "This fails with not allowing absolute paths but they're not file paths in our case")
public class DrsServiceTest {

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  private static final String RAS_ISSUER = "https://stsstg.nih.gov";
  private static final String SNAPSHOT_DATA_PROJECT = "google-project";

  @Mock private SnapshotService snapshotService;
  @Mock private FileService fileService;
  @Mock private IamService samService;
  @Mock private ResourceService resourceService;
  @Mock private ConfigurationService configurationService;
  @Mock private JobService jobService;
  @Mock private PerformanceLogger performanceLogger;
  @Mock private AzureBlobStorePdao azureBlobStorePdao;
  @Mock private GcsProjectFactory gcsProjectFactory;
  @Mock private EcmConfiguration ecmConfiguration;
  @Mock private DrsDao drsDao;
  @Mock private ApplicationConfiguration appConfig;
  @Mock private MeterRegistry meterRegistry;
  @Mock private AtomicInteger currentDrsRequestCount;

  private DrsIdService drsIdService;

  private DrsService drsService;

  private String googleDrsObjectId;

  private String azureDrsObjectId;

  private FSFile azureFsFile;

  private FSFile googleFsFile;

  private BillingProfileModel billingProfile;

  private UUID snapshotId;

  private UUID googleFileId;

  private DRSPassportRequestModel drsPassportRequestModel;

  @BeforeEach
  void before() throws Exception {
    when(meterRegistry.gauge(eq(DrsService.REQUEST_COUNT_GAUGE_NAME), any()))
        .thenReturn(currentDrsRequestCount);
    drsIdService = new DrsIdService(appConfig);
    drsService =
        spy(
            new DrsService(
                snapshotService,
                fileService,
                drsIdService,
                samService,
                resourceService,
                configurationService,
                jobService,
                performanceLogger,
                azureBlobStorePdao,
                gcsProjectFactory,
                ecmConfiguration,
                drsDao,
                meterRegistry));
    when(jobService.getActivePodCount()).thenReturn(1);
    when(configurationService.getParameterValue(ConfigEnum.DRS_LOOKUP_MAX)).thenReturn(1);

    snapshotId = UUID.randomUUID();
    SnapshotProject snapshotProject = new SnapshotProject();
    when(snapshotService.retrieveSnapshotProject(any())).thenReturn(snapshotProject);

    billingProfile = new BillingProfileModel().id(UUID.randomUUID());
    when(snapshotService.retrieve(snapshotId))
        .thenReturn(
            mockSnapshot(
                snapshotId, billingProfile.getId(), CloudPlatform.GCP, SNAPSHOT_DATA_PROJECT));

    String bucketResourceId = UUID.randomUUID().toString();
    String storageAccountResourceId = UUID.randomUUID().toString();
    googleFileId = UUID.randomUUID();
    DrsId googleDrsId = new DrsId("", "v1", snapshotId.toString(), googleFileId.toString(), false);
    googleDrsObjectId = googleDrsId.toDrsObjectId();

    UUID azureFileId = UUID.randomUUID();
    DrsId azureDrsId = new DrsId("", "v1", snapshotId.toString(), azureFileId.toString(), false);
    azureDrsObjectId = azureDrsId.toDrsObjectId();

    googleFsFile =
        new FSFile()
            .createdDate(Instant.now())
            .description("description")
            .path("file.txt")
            .cloudPath("gs://path/to/file.txt")
            .cloudPlatform(CloudPlatform.GCP)
            .size(100L)
            .fileId(googleFileId)
            .bucketResourceId(bucketResourceId);
    when(fileService.lookupSnapshotFSItem(snapshotProject, googleDrsId.getFsObjectId(), 1))
        .thenReturn(googleFsFile);

    azureFsFile =
        new FSFile()
            .createdDate(Instant.now())
            .description("description")
            .path("file.txt")
            .cloudPath("https://core.windows.net/blahblah")
            .cloudPlatform(CloudPlatform.AZURE)
            .size(1000L)
            .fileId(azureFileId)
            .bucketResourceId(storageAccountResourceId);
    when(fileService.lookupSnapshotFSItem(snapshotProject, azureDrsId.getFsObjectId(), 1))
        .thenReturn(azureFsFile);

    when(resourceService.lookupBucketMetadata(bucketResourceId))
        .thenReturn(new GoogleBucketResource().region(GoogleRegion.DEFAULT_GOOGLE_REGION));
    when(resourceService.lookupStorageAccountMetadata(storageAccountResourceId))
        .thenReturn(new AzureStorageAccountResource().region(AzureRegion.DEFAULT_AZURE_REGION));

    drsPassportRequestModel =
        new DRSPassportRequestModel().addPassportsItem("longPassportToken").expand(false);

    when(ecmConfiguration.rasIssuer()).thenReturn(RAS_ISSUER);
  }

  @Test
  void testLookupPositive() {
    DRSObject googleDrsObject = drsService.lookupObjectByDrsId(TEST_USER, googleDrsObjectId, false);
    assertThat(googleDrsObject.getId(), is(googleDrsObjectId));
    assertThat(googleDrsObject.getSize(), is(googleFsFile.getSize()));
    assertThat(googleDrsObject.getName(), is(googleFsFile.getPath()));

    DRSObject azureDrsObject = drsService.lookupObjectByDrsId(TEST_USER, azureDrsObjectId, false);
    assertThat(azureDrsObject.getId(), is(azureDrsObjectId));
    assertThat(azureDrsObject.getSize(), is(azureFsFile.getSize()));
    assertThat(azureDrsObject.getName(), is(azureFsFile.getPath()));
  }

  @Test
  void testLookupNegative() {
    doThrow(IamForbiddenException.class)
        .when(samService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);
    assertThrows(
        IamForbiddenException.class,
        () -> drsService.lookupObjectByDrsId(TEST_USER, googleDrsObjectId, false));

    assertThrows(
        IamForbiddenException.class,
        () -> drsService.lookupObjectByDrsId(TEST_USER, azureDrsObjectId, false));
  }

  @Test
  void lookupObjectByDrsIdTooManyRequests() {
    when(currentDrsRequestCount.get()).thenReturn(1);
    assertThrows(
        TooManyRequestsException.class,
        () -> drsService.lookupObjectByDrsId(TEST_USER, googleDrsObjectId, false));
  }

  @Test
  void testLookupWithSeveralBillingSnapshots() throws InterruptedException {
    UUID billingIdA = UUID.randomUUID();
    UUID snpId1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
    UUID billingIdB = UUID.randomUUID();
    UUID snpId2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
    UUID snpId3 = UUID.fromString("33333333-3333-3333-3333-333333333333");
    // User doesn't have access to this id.  Test that id doesn't return
    UUID snpId4 = UUID.fromString("44444444-4444-4444-4444-444444444444");
    doAnswer(
            a -> {
              if (a.getArgument(1).equals(IamResourceType.DATASNAPSHOT)
                  && a.getArgument(2).equals(snpId4.toString())
                  && a.getArgument(3).equals(IamAction.READ_DATA)) {
                throw new IamForbiddenException("Not allowed");
              }
              return null;
            })
        .when(samService)
        .verifyAuthorization(any(), any(), any(), any());

    Snapshot snp1 =
        mockSnapshot(snpId1, billingIdA, CloudPlatform.GCP, "google-project-1").globalFileIds(true);
    Snapshot snp2 =
        mockSnapshot(snpId2, billingIdB, CloudPlatform.GCP, "google-project-2").globalFileIds(true);
    Snapshot snp3 =
        mockSnapshot(snpId3, billingIdB, CloudPlatform.GCP, "google-project-3").globalFileIds(true);
    Snapshot snp4 =
        mockSnapshot(snpId4, billingIdB, CloudPlatform.GCP, "google-project-4").globalFileIds(true);
    when(snapshotService.retrieve(eq(snpId1))).thenReturn(snp1);
    when(snapshotService.retrieve(eq(snpId2))).thenReturn(snp2);
    when(snapshotService.retrieve(eq(snpId3))).thenReturn(snp3);
    when(snapshotService.retrieve(eq(snpId4))).thenReturn(snp4);
    when(snapshotService.retrieveSnapshotProject(any()))
        .then(
            a ->
                new SnapshotProject()
                    .id(a.getArgument(0))
                    .dataProject("data_%s".formatted(a.getArgument(0).toString().substring(0, 1))));
    // Used for snapshots 1 and 2
    GoogleBucketResource bucketA =
        new GoogleBucketResource().resourceId(UUID.randomUUID()).region(GoogleRegion.US_CENTRAL1);
    // Used for snapshots 3 and 4
    GoogleBucketResource bucketB =
        new GoogleBucketResource().resourceId(UUID.randomUUID()).region(GoogleRegion.EUROPE_WEST1);
    when(resourceService.lookupBucketMetadata(bucketA.getResourceId().toString()))
        .thenReturn(bucketA);
    when(resourceService.lookupBucketMetadata(bucketB.getResourceId().toString()))
        .thenReturn(bucketB);
    DrsId drsId = new DrsId("", "v2", null, googleFileId.toString(), false);
    when(drsDao.retrieveReferencedSnapshotIds(any()))
        .thenReturn(List.of(snpId1, snpId2, snpId3, snpId4));

    // Mock the files returned from the various snapshots
    when(fileService.lookupSnapshotFSItem(any(), any(), anyInt()))
        .then(
            a -> {
              SnapshotProject project = a.getArgument(0);
              String bucketId;
              if (List.of(snpId1, snpId2).contains(project.getId())) {
                bucketId = bucketA.getResourceId().toString();
              } else {
                bucketId = bucketB.getResourceId().toString();
              }
              return new FSFile()
                  .createdDate(Instant.now())
                  .description("description")
                  .path("file.txt")
                  .cloudPath("gs://%s_bucket/to/file.txt".formatted(project.getDataProject()))
                  .cloudPlatform(CloudPlatform.GCP)
                  .size(100L)
                  .fileId(googleFileId)
                  .bucketResourceId(bucketId);
            });

    DRSObject drsObject = drsService.lookupObjectByDrsId(TEST_USER, drsId.toDrsObjectId(), false);
    assertThat(drsObject.getId(), is(drsId.toDrsObjectId()));
    assertThat(drsObject.getSize(), is(googleFsFile.getSize()));
    assertThat(drsObject.getName(), is(googleFsFile.getPath()));
    // 6 access methods should be present: 2 for snapshots 1, 2 and 3 and none for 4 since the user
    // does not have permissions
    assertThat("access methods were combined", drsObject.getAccessMethods(), hasSize(6));
    assertThat(
        "all regions are accounted for",
        drsObject.getAccessMethods().stream().map(DRSAccessMethod::getRegion).distinct().toList(),
        containsInAnyOrder(
            GoogleRegion.US_CENTRAL1.getValue(), GoogleRegion.EUROPE_WEST1.getValue()));
    // Ensure that the correct billing snapshots are used in the accessId
    assertThat(
        "snapshot 1 drs id points uses snapshot 1 for billing",
        drsObject.getAccessMethods().stream()
            .filter(a -> a.getAccessUrl().getUrl().equals("gs://data_1_bucket/to/file.txt"))
            .findFirst()
            .get()
            .getAccessId(),
        containsString(snpId1.toString()));
    assertThat(
        "snapshot 2 drs id points uses snapshot 2 for billing",
        drsObject.getAccessMethods().stream()
            .filter(a -> a.getAccessUrl().getUrl().equals("gs://data_2_bucket/to/file.txt"))
            .findFirst()
            .get()
            .getAccessId(),
        containsString(snpId2.toString()));
    assertThat(
        "snapshot 3 drs id points uses snapshot 2 for billing",
        drsObject.getAccessMethods().stream()
            .filter(a -> a.getAccessUrl().getUrl().equals("gs://data_3_bucket/to/file.txt"))
            .findFirst()
            .get()
            .getAccessId(),
        containsString(snpId2.toString()));
  }

  @Test
  void testLookupWithMultiCloudDrs() throws InterruptedException {
    UUID billingIdA = UUID.randomUUID();
    UUID snpId1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
    UUID billingIdB = UUID.randomUUID();
    UUID snpId2 = UUID.fromString("22222222-2222-2222-2222-222222222222");

    Snapshot snp1 =
        mockSnapshot(snpId1, billingIdA, CloudPlatform.GCP, "google-project-1").globalFileIds(true);
    Snapshot snp2 = mockSnapshot(snpId2, billingIdB, CloudPlatform.AZURE, null).globalFileIds(true);
    when(snapshotService.retrieve(eq(snpId1))).thenReturn(snp1);
    when(snapshotService.retrieve(eq(snpId2))).thenReturn(snp2);
    when(snapshotService.retrieveSnapshotProject(any()))
        .then(
            a ->
                new SnapshotProject()
                    .id(a.getArgument(0))
                    .dataProject("data_%s".formatted(a.getArgument(0).toString().substring(0, 1))));
    // Used for snapshot 1
    GoogleBucketResource bucketA =
        new GoogleBucketResource().resourceId(UUID.randomUUID()).region(GoogleRegion.US_CENTRAL1);
    // Used for snapshot 2
    AzureStorageAccountResource bucketB =
        new AzureStorageAccountResource().resourceId(UUID.randomUUID()).region(AzureRegion.ASIA);
    when(resourceService.lookupBucketMetadata(bucketA.getResourceId().toString()))
        .thenReturn(bucketA);
    when(resourceService.lookupStorageAccountMetadata(bucketB.getResourceId().toString()))
        .thenReturn(bucketB);
    DrsId drsId = new DrsId("", "v2", null, googleFileId.toString(), false);
    when(drsDao.retrieveReferencedSnapshotIds(any())).thenReturn(List.of(snpId1, snpId2));

    // Mock the files returned from the various snapshots
    when(fileService.lookupSnapshotFSItem(any(), any(), anyInt()))
        .then(
            a -> {
              SnapshotProject project = a.getArgument(0);
              String bucketId;
              CloudPlatform cloudPlatform;
              if (project.getId().equals(snpId1)) {
                bucketId = bucketA.getResourceId().toString();
                cloudPlatform = CloudPlatform.GCP;
              } else {
                bucketId = bucketB.getResourceId().toString();
                cloudPlatform = CloudPlatform.AZURE;
              }
              return new FSFile()
                  .createdDate(Instant.now())
                  .description("description")
                  .path("file.txt")
                  .cloudPath("gs://%s_bucket/to/file.txt".formatted(project.getDataProject()))
                  .cloudPlatform(cloudPlatform)
                  .size(100L)
                  .fileId(googleFileId)
                  .bucketResourceId(bucketId);
            });

    DRSObject drsObject = drsService.lookupObjectByDrsId(TEST_USER, drsId.toDrsObjectId(), false);
    assertThat(drsObject.getId(), is(drsId.toDrsObjectId()));
    assertThat(drsObject.getSize(), is(googleFsFile.getSize()));
    assertThat(drsObject.getName(), is(googleFsFile.getPath()));
    // 3 access methods should be present: 2 for snapshot 1 and 1 for snapshot 2 (Azure drs ids
    // have only one access method)
    assertThat("access methods were combined", drsObject.getAccessMethods(), hasSize(3));
    assertThat(
        "all regions are accounted for",
        drsObject.getAccessMethods().stream().map(DRSAccessMethod::getRegion).distinct().toList(),
        containsInAnyOrder(GoogleRegion.US_CENTRAL1.getValue(), AzureRegion.ASIA.getValue()));
  }

  @Test
  void testBillingSnapshotDetection() throws InterruptedException {
    UUID billingIdA = UUID.randomUUID();
    UUID snpId1 = UUID.fromString("11111111-1111-1111-1111-111111111111");
    UUID billingIdB = UUID.randomUUID();
    UUID snpId2 = UUID.fromString("22222222-2222-2222-2222-222222222222");
    UUID snpId3 = UUID.fromString("33333333-3333-3333-3333-333333333333");
    UUID billingIdC = UUID.randomUUID();
    UUID snpId4 = UUID.fromString("44444444-4444-4444-4444-444444444444");
    UUID snpId5 = UUID.fromString("55555555-5555-5555-5555-555555555555");

    Snapshot snp1 = mockSnapshot(snpId1, billingIdA, CloudPlatform.GCP, "google-project-1");
    Snapshot snp2 = mockSnapshot(snpId2, billingIdB, CloudPlatform.GCP, "google-project-2");
    Snapshot snp3 = mockSnapshot(snpId3, billingIdB, CloudPlatform.GCP, "google-project-3");
    Snapshot snp4 = mockSnapshot(snpId4, billingIdC, CloudPlatform.GCP, "google-project-4");
    Snapshot snp5 = mockSnapshot(snpId5, billingIdA, CloudPlatform.GCP, "google-project-4");
    Map<UUID, UUID> snapshotIds =
        drsService.chooseBillingSnapshotsPerSnapshot(
            List.of(
                new SnapshotCacheResult(snp1),
                new SnapshotCacheResult(snp2),
                new SnapshotCacheResult(snp3),
                new SnapshotCacheResult(snp4),
                new SnapshotCacheResult(snp5)));

    assertThat(
        "correct billing account was selected",
        snapshotIds,
        equalTo(
            Map.of(
                snpId1, snpId1,
                snpId2, snpId2,
                snpId3, snpId2,
                snpId4, snpId4,
                snpId5, snpId1)));
  }

  @Test
  void testLookupSnapshot() {
    List<SnapshotCacheResult> cacheResults =
        drsService.lookupSnapshotsForDRSObject(drsIdService.fromObjectId(googleDrsObjectId));
    assertThat("retrieves correct number of snapshots", cacheResults, hasSize(1));
    assertThat("retrieves correct snapshot", cacheResults.get(0).getId(), equalTo(snapshotId));
  }

  @Test
  void testLookupSnapshotNotFound() {
    UUID randomSnapshotId = UUID.randomUUID();
    when(snapshotService.retrieve(randomSnapshotId)).thenThrow(SnapshotNotFoundException.class);
    DrsId drsIdWithInvalidSnapshotId =
        new DrsId("", "v1", randomSnapshotId.toString(), googleFileId.toString(), false);
    String drsObjectIdWithInvalidSnapshotId = drsIdWithInvalidSnapshotId.toDrsObjectId();
    assertThrows(
        DrsObjectNotFoundException.class,
        () ->
            drsService.lookupSnapshotsForDRSObject(
                drsIdService.fromObjectId(drsObjectIdWithInvalidSnapshotId)));
  }

  @Test
  void verifyValidPassport() {
    // valid passport + phs id + consent code
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(
            new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99"));
    DRSPassportRequestModel drsPassportRequestModel =
        new DRSPassportRequestModel().addPassportsItem("longPassportToken").expand(false);
    when(snapshotService.verifyPassportAuth(any(), any()))
        .thenReturn(new ValidatePassportResult().putAuditInfoItem("test", "log").valid(true));
    drsService.verifyPassportAuth(snapshotId, drsPassportRequestModel);
  }

  @Test
  void invalidPassport() {
    // invalid passport
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(
            new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99"));
    when(snapshotService.verifyPassportAuth(any(), any()))
        .thenReturn(new ValidatePassportResult().putAuditInfoItem("test", "log").valid(false));
    DRSPassportRequestModel drsPassportRequestModel =
        new DRSPassportRequestModel().addPassportsItem("longPassportToken").expand(false);
    assertThrows(
        UnauthorizedException.class,
        () -> drsService.verifyPassportAuth(snapshotId, drsPassportRequestModel));
  }

  private void verifyAuthorizationsWithoutPassport(DRSAuthorizations auths) {
    assertThat(
        "BearerAuth is only type supported",
        auths.getSupportedTypes(),
        contains(DRSAuthorizations.SupportedTypesEnum.BEARERAUTH));
    assertThat(
        "Unauthorized for passport means no passport issuer",
        auths.getPassportAuthIssuers(),
        equalTo(null));
  }

  @Test
  void lookupAuthorizationsByDrsIdWithoutPHSOrConsentCode() {
    SnapshotSummaryModel snapshotSummary = new SnapshotSummaryModel().id(snapshotId);
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    assertThat(
        "Passport authorization not available without PHS ID or consent code",
        !SnapshotSummary.passportAuthorizationAvailable(snapshotSummary));
    verifyAuthorizationsWithoutPassport(drsService.lookupAuthorizationsByDrsId(googleDrsObjectId));
  }

  @Test
  void lookupAuthorizationsByDrsIdWithoutConsentCode() {
    SnapshotSummaryModel snapshotSummary =
        new SnapshotSummaryModel().id(snapshotId).phsId("phs100789");
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    assertThat(
        "Passport authorization not available without consent code",
        !SnapshotSummary.passportAuthorizationAvailable(snapshotSummary));
    verifyAuthorizationsWithoutPassport(drsService.lookupAuthorizationsByDrsId(googleDrsObjectId));
  }

  @Test
  void lookupAuthorizationsByDrsIdWithoutPHS() {
    SnapshotSummaryModel snapshotSummary =
        new SnapshotSummaryModel().id(snapshotId).consentCode("c99");
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    assertThat(
        "Passport authorization not available without PHS ID",
        !SnapshotSummary.passportAuthorizationAvailable(snapshotSummary));
    verifyAuthorizationsWithoutPassport(drsService.lookupAuthorizationsByDrsId(googleDrsObjectId));
  }

  @Test
  void lookupAuthorizationsByDrsIdWithPassportIdentifiers() {
    SnapshotSummaryModel snapshotSummary =
        new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99");
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    DRSAuthorizations auths = drsService.lookupAuthorizationsByDrsId(googleDrsObjectId);

    assertThat(
        "Passport authorization available with PHS ID and consent code",
        SnapshotSummary.passportAuthorizationAvailable(snapshotSummary));
    assertThat(
        "PassportAuth and BearerAuth are supported",
        auths.getSupportedTypes(),
        contains(
            DRSAuthorizations.SupportedTypesEnum.PASSPORTAUTH,
            DRSAuthorizations.SupportedTypesEnum.BEARERAUTH));
    assertThat(
        "Passport issuer supplied when authorized for passport",
        auths.getPassportAuthIssuers(),
        contains(RAS_ISSUER));
  }

  @Test
  void lookupAuthorizationsByDrsIdTooManyRequests() {
    when(currentDrsRequestCount.get()).thenReturn(1);
    assertThrows(
        TooManyRequestsException.class,
        () -> drsService.lookupAuthorizationsByDrsId(googleDrsObjectId));
  }

  @Test
  void lookupObjectByDrsId() {
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(new SnapshotSummaryModel().id(snapshotId));
    DRSObject object = drsService.lookupObjectByDrsId(TEST_USER, googleDrsObjectId, false);
    DRSAccessMethod accessMethod = object.getAccessMethods().get(0);
    assertThat(
        "Only BEARER authorization is included",
        accessMethod.getAuthorizations().getSupportedTypes().size(),
        equalTo(1));
  }

  @Test
  void lookupObjectByDrsIdPassport() {
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(
            new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99"));
    // provide valid passport
    when(snapshotService.verifyPassportAuth(any(), any()))
        .thenReturn(new ValidatePassportResult().putAuditInfoItem("test", "log").valid(true));
    DRSObject object =
        drsService.lookupObjectByDrsIdPassport(googleDrsObjectId, drsPassportRequestModel);
    DRSAccessMethod accessMethod = object.getAccessMethods().get(0);
    assertThat(
        "Correct access method is returned",
        "gcp-passport-us-central1*" + snapshotId,
        equalTo(accessMethod.getAccessId()));
    assertThat(
        "Both authorization types are included",
        accessMethod.getAuthorizations().getSupportedTypes().size(),
        equalTo(2));
    assertThat("Correct drs object is returned", googleDrsObjectId, equalTo(object.getId()));
  }

  @Test
  void lookupObjectByDrsIdPassportInvalid() {
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(
            new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99"));
    // provide invalid passport
    when(snapshotService.verifyPassportAuth(any(), any()))
        .thenReturn(new ValidatePassportResult().putAuditInfoItem("test", "log").valid(false));
    assertThrows(
        UnauthorizedException.class,
        () -> drsService.lookupObjectByDrsIdPassport(googleDrsObjectId, drsPassportRequestModel));
  }

  @Test
  void lookupObjectByDrsIdPassportTooManyRequests() {
    when(currentDrsRequestCount.get()).thenReturn(1);
    assertThrows(
        TooManyRequestsException.class,
        () -> drsService.lookupObjectByDrsIdPassport(googleDrsObjectId, drsPassportRequestModel));
  }

  @Test
  void postAccessUrlForObjectId() throws MalformedURLException {
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(
            new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99"));
    // provide valid passport
    when(snapshotService.verifyPassportAuth(any(), any()))
        .thenReturn(new ValidatePassportResult().putAuditInfoItem("test", "log").valid(true));
    // mock storage
    Snapshot snapshot =
        mockSnapshot(snapshotId, billingProfile.getId(), CloudPlatform.GCP, SNAPSHOT_DATA_PROJECT);
    String snapshotProject = snapshot.getProjectResource().getGoogleProjectId();
    Storage storage = mock(Storage.class);
    when(gcsProjectFactory.getStorage(snapshot.getProjectResource().getGoogleProjectId()))
        .thenReturn(storage);
    String sourcePath = googleFsFile.getCloudPath();
    String expectedUrl = "https://storage.googleapis.com/path/to/file.txt";
    when(storage.signUrl(
            eq(BlobInfo.newBuilder(GcsUriUtils.parseBlobUri(sourcePath)).build()),
            eq(15L),
            eq(TimeUnit.MINUTES),
            any(),
            any()))
        .thenReturn(new URL(expectedUrl));
    when(drsService.initStorage(snapshotProject)).thenReturn(storage);
    DRSAccessURL url =
        drsService.postAccessUrlForObjectId(
            googleDrsObjectId,
            "gcp-passport-us-central1*" + snapshotId,
            drsPassportRequestModel,
            null);

    assertThat("returns url", url.getUrl(), containsString(expectedUrl));
  }

  @Test
  void postAccessUrlForObjectIdInvalidPassport() {
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(
            new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99"));
    // provide invalid passport
    when(snapshotService.verifyPassportAuth(any(), any()))
        .thenReturn(new ValidatePassportResult().putAuditInfoItem("test", "log").valid(false));

    assertThrows(
        UnauthorizedException.class,
        () ->
            drsService.postAccessUrlForObjectId(
                googleDrsObjectId, "gcp-passport-us-central1", drsPassportRequestModel, null));
  }

  private static Stream<Arguments> testSignGcpUrl() {
    return Stream.of(
        arguments(null, false), arguments("myProject", false), arguments("myProject", true));
  }

  @ParameterizedTest
  @MethodSource
  void testSignGcpUrl(String billingProject, boolean selfHostedDataset) {
    SnapshotSummaryModel snapshotSummary =
        new SnapshotSummaryModel().id(snapshotId).selfHosted(selfHostedDataset);
    Snapshot snapshot =
        mockSnapshot(snapshotId, billingProfile.getId(), CloudPlatform.GCP, SNAPSHOT_DATA_PROJECT);
    snapshot.getSourceDataset().getDatasetSummary().selfHosted(selfHostedDataset);

    Storage storage = mock(Storage.class);
    Bucket bucket = mock(Bucket.class);
    when(bucket.getLocation()).thenReturn("us-central1");
    String sourcePath = googleFsFile.getCloudPath();

    if (selfHostedDataset) {
      String datasetProject = snapshot.getSourceDataset().getProjectResource().getGoogleProjectId();
      snapshotSummary.selfHosted(selfHostedDataset);
      when(storage.get(eq(BlobId.fromGsUtilUri(sourcePath).getBucket()), any())).thenReturn(bucket);
      // This mock is needed in order to get the region from the bucket
      when(gcsProjectFactory.getStorage(datasetProject)).thenReturn(storage);

      when(samService.signUrlForBlob(TEST_USER, billingProject, sourcePath, URL_TTL))
          .thenReturn(
              "https://storage.googleapis.com/path/to/file.txt"
                  + "?requestedBy=%s&userProject=%s"
                      .formatted(
                          URLEncoder.encode(TEST_USER.getEmail(), StandardCharsets.UTF_8),
                          billingProject));
    } else {
      String snapshotProject = snapshot.getProjectResource().getGoogleProjectId();
      // Note: we have to do some shenanigans with private methods in order for this to work since
      // Google doesn't implement equals/hashcode on their {@link SignUrlOption} class
      when(storage.signUrl(
              eq(BlobInfo.newBuilder(GcsUriUtils.parseBlobUri(sourcePath)).build()),
              eq(15L),
              eq(TimeUnit.MINUTES),
              any(),
              any()))
          .thenAnswer(
              a -> {
                Map<String, String> queryParams =
                    TestUtils.extractValueFromPrivateObject(
                        a.getArgument(3, Storage.SignUrlOption.class), "value");
                return new URL(
                    "https://storage.googleapis.com/path/to/file.txt"
                        + "?%s=%s&%s=%s"
                            .formatted(
                                REQUESTED_BY_QUERY_PARAM,
                                queryParams.get(REQUESTED_BY_QUERY_PARAM),
                                USER_PROJECT_QUERY_PARAM,
                                queryParams.get(USER_PROJECT_QUERY_PARAM)));
              });
      when(drsService.initStorage(snapshotProject)).thenReturn(storage);
    }
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    DRSObject object = drsService.lookupObjectByDrsId(TEST_USER, googleDrsObjectId, false);
    DRSAccessMethod accessMethod = object.getAccessMethods().get(0);
    assertThat(
        "Only BEARER authorization is included",
        accessMethod.getAuthorizations().getSupportedTypes(),
        equalTo(List.of(SupportedTypesEnum.BEARERAUTH)));

    DRSAccessURL url =
        drsService.getAccessUrlForObjectId(
            TEST_USER, googleDrsObjectId, "gcp-us-central1*" + snapshotId, billingProject);

    assertThat(
        "returns url",
        url.getUrl(),
        containsString("https://storage.googleapis.com/path/to/file.txt"));
    if (billingProject == null) {
      assertThat(
          "contains snapshot's billing project",
          UriUtils.getValueFromQueryParameter(url.getUrl(), USER_PROJECT_QUERY_PARAM),
          equalTo(SNAPSHOT_DATA_PROJECT));
    } else {
      assertThat(
          "contains user specified billing project",
          UriUtils.getValueFromQueryParameter(url.getUrl(), USER_PROJECT_QUERY_PARAM),
          equalTo(billingProject));
    }
    assertThat(
        "contains requester email",
        UriUtils.getValueFromQueryParameter(url.getUrl(), GcsConstants.REQUESTED_BY_QUERY_PARAM),
        equalTo(TEST_USER.getEmail()));
  }

  @Test
  void testSignAzureUrl() throws InterruptedException {
    UUID defaultProfileModelId = UUID.randomUUID();
    Snapshot snapshot =
        mockSnapshot(snapshotId, defaultProfileModelId, CloudPlatform.AZURE, SNAPSHOT_DATA_PROJECT);
    // Make this a global file id snapshot to test access ids
    snapshot.globalFileIds(true);
    DrsId drsId = drsIdService.fromObjectId(azureDrsObjectId);
    when(snapshotService.retrieve(UUID.fromString(drsId.getSnapshotId()))).thenReturn(snapshot);
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource().region(AzureRegion.DEFAULT_AZURE_REGION);
    when(fileService.lookupSnapshotFSItem(any(), any(), eq(1))).thenReturn(azureFsFile);
    when(resourceService.lookupStorageAccountMetadata(any())).thenReturn(storageAccountResource);
    String urlString = "https://blahblah.core.windows.com/data/file.json";
    when(azureBlobStorePdao.signFile(any(), any(), any(), any())).thenReturn(urlString);

    DRSAccessURL result =
        drsService.getAccessUrlForObjectId(
            TEST_USER, azureDrsObjectId, "az-centralus*" + snapshotId, null);
    assertEquals(urlString, result.getUrl());
  }

  @Test
  void testSnapshotCache() throws Exception {
    List<String> googleDrsObjectIds =
        IntStream.range(0, 5)
            .mapToObj(
                i -> {
                  UUID googleFileId = UUID.randomUUID();
                  DrsId googleDrsId =
                      new DrsId("", "v1", snapshotId.toString(), googleFileId.toString(), false);
                  return googleDrsId.toDrsObjectId();
                })
            .toList();

    when(fileService.lookupSnapshotFSItem(any(), any(), eq(1))).thenReturn(googleFsFile);
    for (var drsId : googleDrsObjectIds) {
      drsService.lookupObjectByDrsId(TEST_USER, drsId, false);
    }
    verify(snapshotService, times(1)).retrieve(any());
    verify(snapshotService, times(1)).retrieveSnapshotProject(any());

    List<String> azureDrsObjectIds =
        IntStream.range(0, 5)
            .mapToObj(
                i -> {
                  UUID azureFileId = UUID.randomUUID();
                  DrsId azureDrsId =
                      new DrsId("", "v1", snapshotId.toString(), azureFileId.toString(), false);
                  return azureDrsId.toDrsObjectId();
                })
            .toList();

    when(fileService.lookupSnapshotFSItem(any(), any(), eq(1))).thenReturn(azureFsFile);
    for (var drsId : azureDrsObjectIds) {
      drsService.lookupObjectByDrsId(TEST_USER, drsId, false);
    }
    verify(snapshotService, times(1)).retrieve(any());
    verify(snapshotService, times(1)).retrieveSnapshotProject(any());
  }

  @Test
  void testMergeDrsObjects() {
    DRSObject drsObject1 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.ASIA_SOUTH1,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    DRSObject drsObject2 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.US_CENTRAL1,
            Instant.parse("2022-01-02T00:00:00.00Z"));
    DRSObject drsObject3 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file.txt",
            123L,
            "foomd5",
            CloudPlatform.AZURE,
            AzureRegion.EUROPE,
            Instant.parse("2022-01-03T00:00:00.00Z"));

    assertThat(
        "drs objects get merged",
        drsService.mergeDRSObjects(List.of(drsObject1, drsObject2, drsObject3)),
        equalTo(
            new DRSObject()
                .id("v2_file1")
                .name("file.txt")
                .selfUri("drs://hostname/v2_file1")
                .size(123L)
                .checksums(List.of(new DRSChecksum().type("md5").checksum("foomd5")))
                .version("0")
                .mimeType("text/plain")
                .description("")
                .createdTime(Instant.parse("2022-01-01T00:00:00.00Z").toString())
                .updatedTime(Instant.parse("2022-01-03T00:00:00.00Z").toString())
                .accessMethods(
                    List.of(
                        new DRSAccessMethod()
                            .type(TypeEnum.HTTPS)
                            .accessId("gcp-asia-south1")
                            .region("asia-south1"),
                        new DRSAccessMethod()
                            .type(TypeEnum.HTTPS)
                            .accessId("az-europe")
                            .region("europe"),
                        new DRSAccessMethod()
                            .type(TypeEnum.HTTPS)
                            .accessId("gcp-us-central1")
                            .region("us-central1")))
                .aliases(List.of("/my/path/file.txt"))));
  }

  @Test
  void testMergeDrsObjectsWithConflictingChecksums() {
    DRSObject drsObject1 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file.txt",
            123L,
            "foomd5-1",
            CloudPlatform.GCP,
            GoogleRegion.ASIA_SOUTH1,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    DRSObject drsObject2 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file.txt",
            123L,
            "foomd5-2",
            CloudPlatform.GCP,
            GoogleRegion.US_CENTRAL1,
            Instant.parse("2022-01-02T00:00:00.00Z"));

    assertThrows(
        InvalidDrsObjectException.class,
        () -> drsService.mergeDRSObjects(List.of(drsObject1, drsObject2)));
  }

  @Test
  void testMergeDrsObjectsWithConflictingPath() {
    DRSObject drsObject1 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.ASIA_SOUTH1,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    DRSObject drsObject2 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file2.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.US_CENTRAL1,
            Instant.parse("2022-01-02T00:00:00.00Z"));

    assertThrows(
        InvalidDrsObjectException.class,
        () -> drsService.mergeDRSObjects(List.of(drsObject1, drsObject2)));
  }

  @Test
  void testDateMerging() {
    DRSObject drsObject1 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.ASIA_SOUTH1,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    DRSObject drsObject2 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.US_CENTRAL1,
            Instant.parse("2022-01-02T00:00:00.00Z"));
    assertThat(
        "min date returns correctly",
        DrsService.getMinCreatedTime(List.of(drsObject1, drsObject2)),
        equalTo("2022-01-01T00:00:00Z"));
    assertThat(
        "max date returns correctly",
        DrsService.getMaxUpdatedTime(List.of(drsObject1, drsObject2)),
        equalTo("2022-01-02T00:00:00Z"));
  }

  @Test
  void testUniqueExtraction() {
    DRSObject drsObject1 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.ASIA_SOUTH1,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    DRSObject drsObject2 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            124L,
            "foomd5",
            CloudPlatform.AZURE,
            AzureRegion.CENTRAL_US,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    assertThat(
        "distinct value is correctly returned",
        DrsService.extractUniqueDrsObjectValue(List.of(drsObject1, drsObject2), DRSObject::getId),
        equalTo("v2_file1"));
    assertThrows(
        InvalidDrsObjectException.class,
        () ->
            DrsService.extractUniqueDrsObjectValue(
                List.of(drsObject1, drsObject2), DRSObject::getSize));
  }

  @Test
  void testDistinctListExtraction() {
    DRSObject drsObject1 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.ASIA_SOUTH1,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    DRSObject drsObject2 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            123L,
            "foomd5",
            CloudPlatform.AZURE,
            AzureRegion.CENTRAL_US,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    DRSObject drsObject3 =
        createFileDrsObject(
            "v2_file1",
            "/my/path/file1.txt",
            123L,
            "foomd5",
            CloudPlatform.GCP,
            GoogleRegion.ASIA_SOUTH1,
            Instant.parse("2022-01-01T00:00:00.00Z"));
    assertThat(
        "access methods are correctly returned",
        DrsService.extractDistinctListOfDrsObjectValues(
            List.of(drsObject1, drsObject2, drsObject3),
            DRSObject::getAccessMethods,
            Comparator.comparing(DRSAccessMethod::getRegion)
                .thenComparing(DRSAccessMethod::getAccessId)),
        equalTo(
            List.of(
                new DRSAccessMethod()
                    .region(GoogleRegion.ASIA_SOUTH1.getValue())
                    .accessId("gcp-" + GoogleRegion.ASIA_SOUTH1.getValue())
                    .type(TypeEnum.HTTPS),
                new DRSAccessMethod()
                    .region(AzureRegion.CENTRAL_US.getValue())
                    .accessId("az-" + AzureRegion.CENTRAL_US.getValue())
                    .type(TypeEnum.HTTPS))));

    assertThat(
        "access methods are correctly returned when swapping comparators",
        DrsService.extractDistinctListOfDrsObjectValues(
            List.of(drsObject1, drsObject2, drsObject3),
            DRSObject::getAccessMethods,
            Comparator.comparing(DRSAccessMethod::getAccessId)
                .thenComparing(DRSAccessMethod::getRegion)),
        equalTo(
            List.of(
                new DRSAccessMethod()
                    .region(AzureRegion.CENTRAL_US.getValue())
                    .accessId("az-" + AzureRegion.CENTRAL_US.getValue())
                    .type(TypeEnum.HTTPS),
                new DRSAccessMethod()
                    .region(GoogleRegion.ASIA_SOUTH1.getValue())
                    .accessId("gcp-" + GoogleRegion.ASIA_SOUTH1.getValue())
                    .type(TypeEnum.HTTPS))));
  }

  @Test
  void testLookupDrsObjectByAlias() {
    String alias = "foo";
    String flightId = "myflight";
    // Before mocking the call should fail
    assertThrows(
        InvalidDrsIdException.class, () -> drsService.lookupObjectByDrsId(TEST_USER, alias, false));

    when(drsDao.retrieveDrsAliasByAlias(alias))
        .thenReturn(
            new DrsAlias(
                UUID.randomUUID(),
                alias,
                drsIdService.fromObjectId(googleDrsObjectId),
                Instant.now(),
                TEST_USER.getEmail(),
                flightId));

    DRSObject googleDrsObject = drsService.lookupObjectByDrsId(TEST_USER, alias, false);
    assertThat(googleDrsObject.getId(), is(googleDrsObjectId));
    assertThat(googleDrsObject.getSize(), is(googleFsFile.getSize()));
    assertThat(googleDrsObject.getName(), is(googleFsFile.getPath()));
  }

  @SuppressFBWarnings(
      value = "NP",
      justification = "It's incorrectly complaining about potential NPEs")
  private DRSObject createFileDrsObject(
      String id,
      String path,
      long size,
      String md5,
      CloudPlatform platform,
      CloudRegion region,
      Instant createTime) {
    String platformPrefix = platform == CloudPlatform.AZURE ? "az" : "gcp";
    return new DRSObject()
        .id(id)
        .name(Paths.get(path).getFileName().toString())
        .selfUri("drs://hostname/" + id)
        .size(size)
        .checksums(List.of(new DRSChecksum().type("md5").checksum(md5)))
        .version("0")
        .mimeType("text/plain")
        .description("")
        .createdTime(createTime.toString())
        .updatedTime(createTime.toString())
        .accessMethods(
            List.of(
                new DRSAccessMethod()
                    .type(TypeEnum.HTTPS)
                    .accessId(platformPrefix + "-" + region.getValue())
                    .region(region.getValue())))
        .aliases(List.of(path));
  }

  private Snapshot mockSnapshot(
      UUID id, UUID billingProfileId, CloudPlatform cloudPlatform, String snapshotProject) {
    var billingProfile = new BillingProfileModel().id(billingProfileId);
    return new Snapshot()
        .id(id)
        .profileId(billingProfile.getId())
        .projectResource(new GoogleProjectResource().googleProjectId(snapshotProject))
        .snapshotSources(
            List.of(
                new SnapshotSource()
                    .dataset(
                        new Dataset(
                                new DatasetSummary()
                                    .selfHosted(false)
                                    .defaultProfileId(billingProfile.getId())
                                    .cloudPlatform(cloudPlatform)
                                    .billingProfiles(List.of(billingProfile)))
                            .projectResource(
                                new GoogleProjectResource()
                                    .googleProjectId("dataset-google-project")))));
  }
}
