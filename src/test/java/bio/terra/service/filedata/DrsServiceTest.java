package bio.terra.service.filedata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.app.logging.PerformanceLogger;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSAuthorizations;
import bio.terra.model.DRSObject;
import bio.terra.model.DRSPassportRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
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
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DrsServiceTest {

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

  private final DrsIdService drsIdService = new DrsIdService(new ApplicationConfiguration());

  private DrsService drsService;

  private String googleDrsObjectId;

  private String azureDrsObjectId;

  private FSFile azureFsFile;

  private FSFile googleFsFile;

  private UUID snapshotId;

  private UUID googleFileId;

  private DRSPassportRequestModel drsPassportRequestModel;

  private final AuthenticatedUserRequest authUser =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  private String rasIssuer = "https://stsstg.nih.gov";

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
            azureBlobStorePdao,
            gcsProjectFactory,
            ecmConfiguration);
    when(jobService.getActivePodCount()).thenReturn(1);
    when(configurationService.getParameterValue(ConfigEnum.DRS_LOOKUP_MAX)).thenReturn(1);

    snapshotId = UUID.randomUUID();
    SnapshotProject snapshotProject = new SnapshotProject();
    when(snapshotService.retrieveAvailableSnapshotProject(snapshotId)).thenReturn(snapshotProject);

    BillingProfileModel billingProfile = new BillingProfileModel().id(UUID.randomUUID());
    when(snapshotService.retrieve(snapshotId))
        .thenReturn(
            new Snapshot()
                .id(snapshotId)
                .projectResource(new GoogleProjectResource().googleProjectId("google-project"))
                .snapshotSources(
                    List.of(
                        new SnapshotSource()
                            .dataset(
                                new Dataset(
                                        new DatasetSummary()
                                            .selfHosted(false)
                                            .defaultProfileId(billingProfile.getId())
                                            .billingProfiles(List.of(billingProfile)))
                                    .projectResource(
                                        new GoogleProjectResource()
                                            .googleProjectId("dataset-google-project"))))));

    String bucketResourceId = UUID.randomUUID().toString();
    String storageAccountResourceId = UUID.randomUUID().toString();
    googleFileId = UUID.randomUUID();
    DrsId googleDrsId = new DrsId("", "v1", snapshotId.toString(), googleFileId.toString());
    googleDrsObjectId = googleDrsId.toDrsObjectId();

    UUID azureFileId = UUID.randomUUID();
    DrsId azureDrsId = new DrsId("", "v1", snapshotId.toString(), azureFileId.toString());
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

    when(ecmConfiguration.getRasIssuer()).thenReturn(rasIssuer);
  }

  @Test
  public void testLookupPositive() {
    DRSObject googleDrsObject = drsService.lookupObjectByDrsId(authUser, googleDrsObjectId, false);
    assertThat(googleDrsObject.getId(), is(googleDrsObjectId));
    assertThat(googleDrsObject.getSize(), is(googleFsFile.getSize()));
    assertThat(googleDrsObject.getName(), is(googleFsFile.getPath()));

    DRSObject azureDrsObject = drsService.lookupObjectByDrsId(authUser, azureDrsObjectId, false);
    assertThat(azureDrsObject.getId(), is(azureDrsObjectId));
    assertThat(azureDrsObject.getSize(), is(azureFsFile.getSize()));
    assertThat(azureDrsObject.getName(), is(azureFsFile.getPath()));
  }

  @Test
  public void testLookupNegative() {
    doThrow(IamForbiddenException.class)
        .when(samService)
        .verifyAuthorization(
            authUser, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);
    assertThrows(
        IamForbiddenException.class,
        () -> drsService.lookupObjectByDrsId(authUser, googleDrsObjectId, false));

    assertThrows(
        IamForbiddenException.class,
        () -> drsService.lookupObjectByDrsId(authUser, azureDrsObjectId, false));
  }

  @Test
  public void testLookupSnapshot() {
    DrsService.SnapshotCacheResult cacheResult =
        drsService.lookupSnapshotForDRSObject(googleDrsObjectId);
    assertThat("retrieves correct snapshot", cacheResult.getId(), equalTo(snapshotId));
  }

  @Test
  public void testLookupSnapshotNotFound() {
    UUID randomSnapshotId = UUID.randomUUID();
    when(snapshotService.retrieve(randomSnapshotId)).thenThrow(SnapshotNotFoundException.class);
    DrsId drsIdWithInvalidSnapshotId =
        new DrsId("", "v1", randomSnapshotId.toString(), googleFileId.toString());
    String drsObjectIdWithInvalidSnapshotId = drsIdWithInvalidSnapshotId.toDrsObjectId();
    assertThrows(
        DrsObjectNotFoundException.class,
        () -> drsService.lookupSnapshotForDRSObject(drsObjectIdWithInvalidSnapshotId));
  }

  @Test
  public void testLookupSnapshotInvalidDrsId() {
    UUID randomSnapshotId = UUID.randomUUID();
    DrsId drsIdWithInvalidDrsId =
        new DrsId("", "", randomSnapshotId.toString(), googleFileId.toString());
    String drsObjectIdWithInvalidDrsId = drsIdWithInvalidDrsId.toDrsObjectId();
    assertThrows(
        InvalidDrsIdException.class,
        () -> drsService.lookupSnapshotForDRSObject(drsObjectIdWithInvalidDrsId));
  }

  @Test
  public void verifyValidPassport() {
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
  public void invalidPassport() {
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
  public void lookupAuthorizationsByDrsIdWithoutPHSOrConsentCode() {
    SnapshotSummaryModel snapshotSummary = new SnapshotSummaryModel().id(snapshotId);
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    assertThat(
        "Passport authorization not available without PHS ID or consent code",
        !SnapshotSummary.passportAuthorizationAvailable(snapshotSummary));
    verifyAuthorizationsWithoutPassport(drsService.lookupAuthorizationsByDrsId(googleDrsObjectId));
  }

  @Test
  public void lookupAuthorizationsByDrsIdWithoutConsentCode() {
    SnapshotSummaryModel snapshotSummary =
        new SnapshotSummaryModel().id(snapshotId).phsId("phs100789");
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    assertThat(
        "Passport authorization not available without consent code",
        !SnapshotSummary.passportAuthorizationAvailable(snapshotSummary));
    verifyAuthorizationsWithoutPassport(drsService.lookupAuthorizationsByDrsId(googleDrsObjectId));
  }

  @Test
  public void lookupAuthorizationsByDrsIdWithoutPHS() {
    SnapshotSummaryModel snapshotSummary =
        new SnapshotSummaryModel().id(snapshotId).consentCode("c99");
    when(snapshotService.retrieveSnapshotSummary(snapshotId)).thenReturn(snapshotSummary);

    assertThat(
        "Passport authorization not available without PHS ID",
        !SnapshotSummary.passportAuthorizationAvailable(snapshotSummary));
    verifyAuthorizationsWithoutPassport(drsService.lookupAuthorizationsByDrsId(googleDrsObjectId));
  }

  @Test
  public void lookupAuthorizationsByDrsIdWithPassportIdentifiers() {
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
        contains(rasIssuer));
  }

  @Test
  public void lookupObjectByDrsId() {
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(new SnapshotSummaryModel().id(snapshotId));
    DRSObject object = drsService.lookupObjectByDrsId(authUser, googleDrsObjectId, false);
    DRSAccessMethod accessMethod = object.getAccessMethods().get(0);
    assertThat(
        "Only BEARER authorization is included",
        accessMethod.getAuthorizations().getSupportedTypes().size(),
        equalTo(1));
  }

  @Test
  public void lookupObjectByDrsIdPassport() {
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
        "gcp-passport-us-central1",
        equalTo(accessMethod.getAccessId()));
    assertThat(
        "Both authorization types are included",
        accessMethod.getAuthorizations().getSupportedTypes().size(),
        equalTo(2));
    assertThat("Correct drs object is returned", googleDrsObjectId, equalTo(object.getId()));
  }

  @Test
  public void lookupObjectByDrsIdPassportInvalid() {
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
  public void postAccessUrlForObjectId() {
    when(snapshotService.retrieveSnapshotSummary(snapshotId))
        .thenReturn(
            new SnapshotSummaryModel().id(snapshotId).phsId("phs100789").consentCode("c99"));
    // provide valid passport
    when(snapshotService.verifyPassportAuth(any(), any()))
        .thenReturn(new ValidatePassportResult().putAuditInfoItem("test", "log").valid(true));
    DRSAccessURL url =
        drsService.postAccessUrlForObjectId(
            googleDrsObjectId, "gcp-passport-us-central1", drsPassportRequestModel);

    assertThat(
        "returns url",
        url.getUrl(),
        containsString("https://storage.googleapis.com/path/to/file.txt"));
  }

  @Test
  public void postAccessUrlForObjectIdInvalidPassport() {
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
                googleDrsObjectId, "gcp-passport-us-central1", drsPassportRequestModel));
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
                    .defaultProfileId(defaultProfileModelId))
            .projectResource(new GoogleProjectResource().googleProjectId("dataset-google-project"));
    Snapshot snapshot =
        new Snapshot()
            .id(snapshotId)
            .snapshotSources(List.of(new SnapshotSource().dataset(dataset)));
    DrsId drsId = drsIdService.fromObjectId(azureDrsObjectId);
    when(snapshotService.retrieve(UUID.fromString(drsId.getSnapshotId()))).thenReturn(snapshot);
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource().region(AzureRegion.DEFAULT_AZURE_REGION);
    when(fileService.lookupSnapshotFSItem(any(), any(), eq(1))).thenReturn(azureFsFile);
    when(resourceService.lookupStorageAccountMetadata(any())).thenReturn(storageAccountResource);
    String urlString = "https://blahblah.core.windows.com/data/file.json";
    when(azureBlobStorePdao.signFile(any(), any(), any(), any(), any())).thenReturn(urlString);

    DRSAccessURL result =
        drsService.getAccessUrlForObjectId(authUser, azureDrsObjectId, "az-centralus");
    assertEquals(urlString, result.getUrl());
  }

  @Test
  public void testSnapshotCache() throws Exception {
    List<String> googleDrsObjectIds =
        IntStream.range(0, 5)
            .mapToObj(
                i -> {
                  UUID googleFileId = UUID.randomUUID();
                  DrsId googleDrsId =
                      new DrsId("", "v1", snapshotId.toString(), googleFileId.toString());
                  return googleDrsId.toDrsObjectId();
                })
            .collect(Collectors.toList());

    when(fileService.lookupSnapshotFSItem(any(), any(), eq(1))).thenReturn(googleFsFile);
    for (var drsId : googleDrsObjectIds) {
      drsService.lookupObjectByDrsId(authUser, drsId, false);
    }
    verify(snapshotService, times(1)).retrieve(any());
    verify(snapshotService, times(1)).retrieveAvailableSnapshotProject(any());

    List<String> azureDrsObjectIds =
        IntStream.range(0, 5)
            .mapToObj(
                i -> {
                  UUID azureFileId = UUID.randomUUID();
                  DrsId azureDrsId =
                      new DrsId("", "v1", snapshotId.toString(), azureFileId.toString());
                  return azureDrsId.toDrsObjectId();
                })
            .collect(Collectors.toList());

    when(fileService.lookupSnapshotFSItem(any(), any(), eq(1))).thenReturn(azureFsFile);
    for (var drsId : azureDrsObjectIds) {
      drsService.lookupObjectByDrsId(authUser, drsId, false);
    }
    verify(snapshotService, times(1)).retrieve(any());
    verify(snapshotService, times(1)).retrieveAvailableSnapshotProject(any());
  }
}
