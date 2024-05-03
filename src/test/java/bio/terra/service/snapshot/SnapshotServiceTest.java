package bio.terra.service.snapshot;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.SqlSortDirection;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import bio.terra.model.AccessInfoBigQueryModel;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.model.AddAuthDomainResponseModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.InaccessibleWorkspacePolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.SnapshotIdsAndRolesModel;
import bio.terra.model.SnapshotLinkDuosDatasetResponse;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.model.WorkspacePolicyModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.auth.ras.EcmService;
import bio.terra.service.auth.ras.RasDbgapPermissions;
import bio.terra.service.auth.ras.exception.InvalidAuthorizationMethod;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.duos.DuosClient;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.SynapseDataResultModel;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.job.JobBuilder;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.rawls.RawlsService;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.SnapshotService.SnapshotAccessibleResult;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.snapshot.flight.authDomain.SnapshotAddDataAccessControlsFlight;
import bio.terra.service.snapshot.flight.create.SnapshotCreateFlight;
import bio.terra.service.snapshot.flight.duos.SnapshotDuosMapKeys;
import bio.terra.service.snapshot.flight.duos.SnapshotUpdateDuosDatasetFlight;
import bio.terra.service.snapshotbuilder.SnapshotBuilderSettingsDao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDataResultModel;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightMap;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.HttpClientErrorException;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotServiceTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  private static final String SNAPSHOT_NAME = "snapshotName";
  private static final String SNAPSHOT_DESCRIPTION = "snapshotDescription";
  private static final String DATASET_NAME = "datasetName";
  private static final String SNAPSHOT_DATA_PROJECT = "tdrdataproject";
  private static final String SNAPSHOT_TABLE_NAME = "tableA";
  private static final String SNAPSHOT_COLUMN_NAME = "columnA";
  private static final String PHS_ID = "phs123456";
  private static final String CONSENT_CODE = "c99";
  private static final String PASSPORT = "passportJwt";
  private static final String DUOS_ID = "DUOS-123456";

  @Mock private JobService jobService;
  @Mock private DatasetService datasetService;
  @Mock private MetadataDataAccessUtils metadataDataAccessUtils;
  @Mock private SnapshotDao snapshotDao;
  @Mock private SnapshotTableDao snapshotTableDao;
  @Mock private IamService iamService;
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private EcmService ecmService;
  @Mock private RawlsService rawlsService;
  @Mock private DuosClient duosClient;

  private final UUID snapshotId = UUID.randomUUID();
  private final UUID datasetId = UUID.randomUUID();
  private final UUID snapshotTableId = UUID.randomUUID();
  private final UUID profileId = UUID.randomUUID();
  private final Instant createdDate = Instant.now();
  private final DuosFirecloudGroupModel duosFirecloudGroup =
      DuosFixtures.createDbFirecloudGroup(DUOS_ID);

  private SnapshotService service;

  @BeforeEach
  void beforeEach() {
    service =
        new SnapshotService(
            jobService,
            datasetService,
            mock(FireStoreDependencyDao.class),
            mock(BigQuerySnapshotPdao.class),
            snapshotDao,
            snapshotTableDao,
            metadataDataAccessUtils,
            iamService,
            ecmService,
            azureSynapsePdao,
            rawlsService,
            duosClient,
            mock(SnapshotBuilderSettingsDao.class));
  }

  @Test
  void testRetrieveSnapshot() {
    mockSnapshot();
    assertThat(
        service.retrieveSnapshotModel(snapshotId, TEST_USER),
        equalTo(
            expectedMockSnapshotModelBase()
                .source(
                    List.of(
                        new SnapshotSourceModel()
                            .dataset(
                                new DatasetSummaryModel()
                                    .id(datasetId)
                                    .name(DATASET_NAME)
                                    .createdDate(createdDate.toString())
                                    .storage(
                                        List.of(
                                            new StorageResourceModel()
                                                .region(
                                                    GoogleRegion.DEFAULT_GOOGLE_REGION.toString())
                                                .cloudResource(
                                                    GoogleCloudResource.BUCKET.toString())
                                                .cloudPlatform(CloudPlatform.GCP))))))
                .tables(
                    List.of(
                        new TableModel()
                            .name(SNAPSHOT_TABLE_NAME)
                            .primaryKey(List.of())
                            .columns(
                                List.of(
                                    new ColumnModel()
                                        .name(SNAPSHOT_COLUMN_NAME)
                                        .datatype(TableDataType.STRING)
                                        .arrayOf(true)
                                        .required(true)))))
                .relationships(Collections.emptyList())
                .profileId(profileId)
                .dataProject(SNAPSHOT_DATA_PROJECT)
                .duosFirecloudGroup(duosFirecloudGroup)));
  }

  @Test
  void testRetrieveSnapshotNoFields() {
    mockSnapshot();
    assertThat(
        service.retrieveSnapshotModel(
            snapshotId, List.of(SnapshotRetrieveIncludeModel.NONE), TEST_USER),
        equalTo(expectedMockSnapshotModelBase()));
  }

  @Test
  void testRetrieveSnapshotDefaultFields() {
    mockSnapshot();
    assertThat(
        service.retrieveSnapshotModel(
            snapshotId,
            List.of(
                SnapshotRetrieveIncludeModel.SOURCES,
                SnapshotRetrieveIncludeModel.TABLES,
                SnapshotRetrieveIncludeModel.RELATIONSHIPS,
                SnapshotRetrieveIncludeModel.PROFILE,
                SnapshotRetrieveIncludeModel.DATA_PROJECT,
                SnapshotRetrieveIncludeModel.DUOS),
            TEST_USER),
        equalTo(service.retrieveSnapshotModel(snapshotId, TEST_USER)));
  }

  @Test
  void testRetrieveSnapshotOnlyCreationInfo() {
    mockSnapshot();
    assertThat(
        service.retrieveSnapshotModel(
            snapshotId, List.of(SnapshotRetrieveIncludeModel.CREATION_INFORMATION), TEST_USER),
        equalTo(
            expectedMockSnapshotModelBase()
                .creationInformation(
                    new SnapshotRequestContentsModel()
                        .mode(SnapshotRequestContentsModel.ModeEnum.BYFULLVIEW)
                        .datasetName(DATASET_NAME))));
  }

  @Test
  void testRetrieveSnapshotOnlyAccessInfo() {
    mockSnapshot();
    AccessInfoModel accessInfoModel =
        new AccessInfoModel()
            .bigQuery(
                new AccessInfoBigQueryModel()
                    .datasetName(SNAPSHOT_NAME)
                    .datasetId(SNAPSHOT_DATA_PROJECT + ":" + SNAPSHOT_NAME)
                    .projectId(SNAPSHOT_DATA_PROJECT)
                    .link(
                        "https://console.cloud.google.com/bigquery?project="
                            + SNAPSHOT_DATA_PROJECT
                            + "&ws=!"
                            + SNAPSHOT_NAME
                            + "&d="
                            + SNAPSHOT_NAME
                            + "&p="
                            + SNAPSHOT_DATA_PROJECT
                            + "&page=dataset")
                    .tables(
                        List.of(
                            new AccessInfoBigQueryModelTable()
                                .name(SNAPSHOT_TABLE_NAME)
                                .qualifiedName(
                                    SNAPSHOT_DATA_PROJECT
                                        + "."
                                        + SNAPSHOT_NAME
                                        + "."
                                        + SNAPSHOT_TABLE_NAME)
                                .link(
                                    "https://console.cloud.google.com/bigquery?project="
                                        + SNAPSHOT_DATA_PROJECT
                                        + "&ws=!"
                                        + SNAPSHOT_NAME
                                        + "&d="
                                        + SNAPSHOT_NAME
                                        + "&p="
                                        + SNAPSHOT_DATA_PROJECT
                                        + "&page=table&t="
                                        + SNAPSHOT_TABLE_NAME)
                                .id(
                                    SNAPSHOT_DATA_PROJECT
                                        + ":"
                                        + SNAPSHOT_NAME
                                        + "."
                                        + SNAPSHOT_TABLE_NAME)
                                .sampleQuery(
                                    "SELECT * FROM `"
                                        + SNAPSHOT_DATA_PROJECT
                                        + "."
                                        + SNAPSHOT_NAME
                                        + "."
                                        + SNAPSHOT_TABLE_NAME
                                        + "`"))));
    when(metadataDataAccessUtils.accessInfoFromSnapshot(any(), any())).thenReturn(accessInfoModel);
    assertThat(
        service.retrieveSnapshotModel(
            snapshotId, List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION), TEST_USER),
        equalTo(expectedMockSnapshotModelBase().accessInformation(accessInfoModel)));
  }

  @Test
  void testRetrieveSnapshotOnlyDuos() {
    mockSnapshot();
    assertThat(
        service.retrieveSnapshotModel(
            snapshotId, List.of(SnapshotRetrieveIncludeModel.DUOS), TEST_USER),
        equalTo(expectedMockSnapshotModelBase().duosFirecloudGroup(duosFirecloudGroup)));
  }

  @Test
  void testRetrieveSnapshotMultiInfo() {
    mockSnapshot();
    assertThat(
        service.retrieveSnapshotModel(
            snapshotId,
            List.of(
                SnapshotRetrieveIncludeModel.PROFILE, SnapshotRetrieveIncludeModel.DATA_PROJECT),
            TEST_USER),
        equalTo(
            expectedMockSnapshotModelBase()
                .profileId(profileId)
                .dataProject(SNAPSHOT_DATA_PROJECT)));
  }

  private void mockSnapshot() {
    when(snapshotDao.retrieveSnapshot(snapshotId))
        .thenReturn(
            new Snapshot()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate)
                .profileId(profileId)
                .projectResource(
                    new GoogleProjectResource()
                        .profileId(profileId)
                        .googleProjectId(SNAPSHOT_DATA_PROJECT))
                .snapshotSources(
                    List.of(
                        new SnapshotSource()
                            .dataset(
                                new Dataset(
                                    new DatasetSummary()
                                        .id(datasetId)
                                        .name(DATASET_NAME)
                                        .projectResourceId(profileId)
                                        .createdDate(createdDate)
                                        .storage(
                                            List.of(
                                                new GoogleStorageResource(
                                                    datasetId,
                                                    GoogleCloudResource.BUCKET,
                                                    GoogleRegion.DEFAULT_GOOGLE_REGION)))))))
                .snapshotTables(
                    List.of(
                        new SnapshotTable()
                            .name(SNAPSHOT_TABLE_NAME)
                            .id(snapshotTableId)
                            .columns(
                                List.of(
                                    new Column()
                                        .name(SNAPSHOT_COLUMN_NAME)
                                        .type(TableDataType.STRING)
                                        .arrayOf(true)
                                        .required(true)))))
                .creationInformation(
                    new SnapshotRequestContentsModel()
                        .mode(SnapshotRequestContentsModel.ModeEnum.BYFULLVIEW)
                        .datasetName(DATASET_NAME))
                .duosFirecloudGroupId(duosFirecloudGroup.getId())
                .duosFirecloudGroup(duosFirecloudGroup));
  }

  private SnapshotModel expectedMockSnapshotModelBase() {
    return new SnapshotModel()
        .id(snapshotId)
        .name(SNAPSHOT_NAME)
        .description(SNAPSHOT_DESCRIPTION)
        .createdDate(createdDate.toString());
  }

  @Test
  void enumerateSnapshots() throws Exception {
    IamRole role = IamRole.DISCOVERER;
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(snapshotId, Set.of(role));
    when(iamService.listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT))
        .thenReturn(resourcesAndRoles);
    when(ecmService.getRasDbgapPermissions(TEST_USER)).thenReturn(List.of());
    SnapshotSummary summary =
        new SnapshotSummary().id(snapshotId).createdDate(Instant.now()).storage(List.of());
    MetadataEnumeration<SnapshotSummary> metadataEnumeration = new MetadataEnumeration<>();
    metadataEnumeration.items(List.of(summary));
    when(snapshotDao.retrieveSnapshots(
            anyInt(),
            anyInt(),
            any(),
            any(),
            any(),
            any(),
            any(),
            eq(resourcesAndRoles.keySet()),
            any(),
            any()))
        .thenReturn(metadataEnumeration);
    var snapshots =
        service.enumerateSnapshots(TEST_USER, 0, 10, null, null, null, null, List.of(), null, null);
    assertThat(snapshots.getItems().get(0).getId(), equalTo(snapshotId));
    assertThat(snapshots.getRoleMap(), hasEntry(snapshotId.toString(), List.of(role.toString())));
  }

  @Test
  void patchSnapshotIamActions() {
    assertThat(
        "Patch without consent code update does not require passport identifier update permissions",
        service.patchSnapshotIamActions(new SnapshotPatchRequestModel()),
        containsInAnyOrder(IamAction.UPDATE_SNAPSHOT));

    assertThat(
        "Patch with consent code update to empty string requires passport identifier update permissions",
        service.patchSnapshotIamActions(new SnapshotPatchRequestModel().consentCode("")),
        containsInAnyOrder(IamAction.UPDATE_SNAPSHOT, IamAction.UPDATE_PASSPORT_IDENTIFIER));

    assertThat(
        "Patch with consent code update requires passport identifier update permissions",
        service.patchSnapshotIamActions(new SnapshotPatchRequestModel().consentCode("c99")),
        containsInAnyOrder(IamAction.UPDATE_SNAPSHOT, IamAction.UPDATE_PASSPORT_IDENTIFIER));

    assertThat(
        "Patch with description update requires UPDATE_SNAPSHOT",
        service.patchSnapshotIamActions(
            new SnapshotPatchRequestModel().description("a description")),
        containsInAnyOrder(IamAction.UPDATE_SNAPSHOT));
  }

  @Test
  void listAuthorizedSnapshotsWhenEcmReturns() throws Exception {
    UUID snapshotId = UUID.randomUUID();

    IamRole samAuthorizedRole = IamRole.STEWARD;
    Map<UUID, Set<IamRole>> samAuthorizedSnapshots = Map.of(snapshotId, Set.of(samAuthorizedRole));
    when(iamService.listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT))
        .thenReturn(samAuthorizedSnapshots);

    List<RasDbgapPermissions> permissions = List.of(new RasDbgapPermissions("c99", "phs123456"));
    when(ecmService.getRasDbgapPermissions(TEST_USER)).thenReturn(permissions);
    when(snapshotDao.getAccessibleSnapshots(permissions)).thenReturn(List.of(snapshotId));

    List<ErrorModel> errors = new ArrayList<>();

    Map<UUID, Set<IamRole>> authorizedSnapshots =
        service.listAuthorizedSnapshots(TEST_USER, errors);

    assertThat(
        "Expected snapshot is authorized", authorizedSnapshots.keySet(), contains(snapshotId));
    assertThat(
        "User can access snapshot via SAM and RAS roles",
        authorizedSnapshots.get(snapshotId),
        containsInAnyOrder(samAuthorizedRole, IamRole.READER));

    assertThat("No ECM errors encountered", errors, empty());
  }

  @Test
  void listAuthorizedSnapshotsWhenEcmThrows() throws Exception {
    Map<UUID, Set<IamRole>> samAuthorizedSnapshots =
        Map.of(UUID.randomUUID(), Set.of(IamRole.STEWARD));
    HttpClientErrorException ecmException = new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT);
    when(ecmService.getRasDbgapPermissions(TEST_USER)).thenThrow(ecmException);
    when(iamService.listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT))
        .thenReturn(samAuthorizedSnapshots);

    List<ErrorModel> errors = new ArrayList<>();
    assertThat(
        "ECM exception should not block snapshot enumeration",
        service.listAuthorizedSnapshots(TEST_USER, errors),
        equalTo(samAuthorizedSnapshots));

    assertThat("ECM error added to error list", errors.size(), equalTo(1));
    assertThat(
        "ECM error contents match expectations",
        errors.get(0).getMessage(),
        containsString("Error listing RAS-authorized snapshots"));
  }

  @Test
  void listRasAuthorizedSnapshots() throws Exception {
    List<RasDbgapPermissions> perms = List.of(new RasDbgapPermissions(CONSENT_CODE, PHS_ID));
    List<UUID> uuids = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

    // First attempt: No linked passport.
    when(ecmService.getRasDbgapPermissions(TEST_USER)).thenReturn(List.of());
    when(snapshotDao.getAccessibleSnapshots(List.of())).thenReturn(List.of());
    assertThat(
        "No linked passport yields empty result",
        service.listRasAuthorizedSnapshots(TEST_USER).entrySet(),
        empty());

    // Second attempt: Linked passport, no matching snapshots.
    when(ecmService.getRasDbgapPermissions(TEST_USER)).thenReturn(perms);
    when(snapshotDao.getAccessibleSnapshots(perms)).thenReturn(List.of());
    assertThat(
        "Linked passport but no matching snapshots yields empty result",
        service.listRasAuthorizedSnapshots(TEST_USER).entrySet(),
        empty());

    // Third attempt: Linked passport, matching snapshots.
    when(snapshotDao.getAccessibleSnapshots(perms)).thenReturn(uuids);
    Map<UUID, Set<IamRole>> idsAndRolesActual = service.listRasAuthorizedSnapshots(TEST_USER);
    assertThat(
        "All matching snapshot UUIDs are found in map keys",
        idsAndRolesActual.keySet(),
        is(Set.copyOf(uuids)));
    assertThat(
        "Snapshot accessibility by passport attributed to reader role",
        idsAndRolesActual.values(),
        everyItem(equalTo(Set.of(IamRole.READER))));
  }

  @Test
  void combineIdsAndRoles() {
    UUID[] uuids = {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

    assertThat("No arguments yield empty map", service.combineIdsAndRoles().entrySet(), empty());

    assertThat(
        "Empty map arguments yields empty map",
        service.combineIdsAndRoles(Map.of(), Map.of()).entrySet(),
        empty());

    assertThat(
        "IamRoles are properly written or combined",
        service.combineIdsAndRoles(
            Map.of(uuids[0], Set.of(IamRole.READER)),
            Map.of(uuids[0], Set.of()),
            Map.of(uuids[1], Set.of(IamRole.READER, IamRole.SNAPSHOT_CREATOR)),
            Map.of(uuids[1], Set.of(IamRole.READER, IamRole.CUSTODIAN)),
            Map.of(uuids[2], Set.of(IamRole.READER))),
        equalTo(
            Map.of(
                uuids[0], Set.of(IamRole.READER),
                uuids[1], Set.of(IamRole.READER, IamRole.SNAPSHOT_CREATOR, IamRole.CUSTODIAN),
                uuids[2], Set.of(IamRole.READER))));
  }

  @Test
  void verifyPassportAuthNoPassports() {
    SnapshotSummaryModel snapshotSummaryModel =
        new SnapshotSummaryModel().id(snapshotId).phsId(PHS_ID).consentCode(CONSENT_CODE);
    assertThrows(
        InvalidAuthorizationMethod.class,
        () -> service.verifyPassportAuth(snapshotSummaryModel, List.of()));
    verifyNoInteractions(ecmService);
  }

  @Test
  void verifyPassportAuthNoPhsId() {
    SnapshotSummaryModel snapshotSummaryModel =
        new SnapshotSummaryModel().id(snapshotId).consentCode(CONSENT_CODE);
    assertThrows(
        InvalidAuthorizationMethod.class,
        () -> service.verifyPassportAuth(snapshotSummaryModel, List.of("passportJwt")));
    verifyNoInteractions(ecmService);
  }

  @Test
  void verifyPassportAuthNoConsentCode() {
    SnapshotSummaryModel snapshotSummaryModel =
        new SnapshotSummaryModel().id(snapshotId).phsId(PHS_ID);
    assertThrows(
        InvalidAuthorizationMethod.class,
        () -> service.verifyPassportAuth(snapshotSummaryModel, List.of("passportJwt")));
    verifyNoInteractions(ecmService);
  }

  private void mockSnapshotSummary() {
    SnapshotSummary snapshotSummary =
        new SnapshotSummary().id(snapshotId).createdDate(createdDate).storage(List.of());
    when(snapshotDao.retrieveSummaryById(snapshotId)).thenReturn(snapshotSummary);
  }

  private void mockSnapshotSummaryWithPassportCriteria() {
    SnapshotSummary snapshotSummary =
        new SnapshotSummary()
            .id(snapshotId)
            .createdDate(createdDate)
            .storage(List.of())
            .phsId(PHS_ID)
            .consentCode(CONSENT_CODE);
    when(snapshotDao.retrieveSummaryById(snapshotId)).thenReturn(snapshotSummary);
  }

  @Test
  void retrieveUserSnapshotRolesSamReader() {
    List<String> samRoles = List.of(IamRole.ADMIN.toString(), IamRole.READER.toString());
    when(iamService.retrieveUserRoles(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId))
        .thenReturn(samRoles);

    assertThat(
        "SAM roles are returned",
        service.retrieveUserSnapshotRoles(snapshotId, TEST_USER),
        contains(samRoles.toArray()));
    verify(ecmService, never()).getRasProviderPassport(TEST_USER);
  }

  @Test
  void retrieveUserSnapshotRolesNoSamReaderNoPassport() {
    List<String> samRoles = List.of(IamRole.ADMIN.toString(), IamRole.STEWARD.toString());
    when(iamService.retrieveUserRoles(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId))
        .thenReturn(samRoles);

    mockSnapshotSummaryWithPassportCriteria();

    assertThat(
        "SAM roles are returned",
        service.retrieveUserSnapshotRoles(snapshotId, TEST_USER),
        contains(samRoles.toArray()));
    verify(ecmService, times(1)).getRasProviderPassport(TEST_USER);
  }

  @Test
  void retrieveUserSnapshotRolesPassportReader() {
    List<String> samRoles = List.of(IamRole.ADMIN.toString(), IamRole.STEWARD.toString());
    when(iamService.retrieveUserRoles(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId))
        .thenReturn(samRoles);

    mockSnapshotSummaryWithPassportCriteria();
    when(ecmService.getRasProviderPassport(TEST_USER)).thenReturn(PASSPORT);
    when(ecmService.validatePassport(any())).thenReturn(new ValidatePassportResult().valid(true));

    List<String> samRolesAndReader = new ArrayList<>(samRoles);
    samRolesAndReader.add(IamRole.READER.toString());
    assertThat(
        "SAM roles and reader are returned",
        service.retrieveUserSnapshotRoles(snapshotId, TEST_USER),
        contains(samRolesAndReader.toArray()));
    verify(ecmService, times(1)).getRasProviderPassport(TEST_USER);
  }

  @Test
  void retrieveSnapshotPolicies() {
    SamPolicyModel spm1 = mock(SamPolicyModel.class);
    SamPolicyModel spm2 = mock(SamPolicyModel.class);
    when(iamService.retrievePolicies(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId))
        .thenReturn(List.of(spm1, spm2));

    List<WorkspacePolicyModel> accessible =
        List.of(
            mock(WorkspacePolicyModel.class),
            mock(WorkspacePolicyModel.class),
            mock(WorkspacePolicyModel.class));
    List<InaccessibleWorkspacePolicyModel> inaccessible =
        List.of(
            mock(InaccessibleWorkspacePolicyModel.class),
            mock(InaccessibleWorkspacePolicyModel.class),
            mock(InaccessibleWorkspacePolicyModel.class));
    List<String> userGroups = List.of("userGroup1", "userGroup2");

    when(iamService.retrieveAuthDomain(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId))
        .thenReturn(userGroups);
    when(rawlsService.resolvePolicyEmails(spm1, TEST_USER))
        .thenReturn(
            new RawlsService.WorkspacePolicyModels(
                accessible.subList(0, 1), inaccessible.subList(0, 2)));
    when(rawlsService.resolvePolicyEmails(spm2, TEST_USER))
        .thenReturn(
            new RawlsService.WorkspacePolicyModels(
                accessible.subList(1, 3), inaccessible.subList(2, 3)));

    PolicyResponse response = service.retrieveSnapshotPolicies(snapshotId, TEST_USER);

    assertThat(
        "The auth domain for this snapshot is returned",
        response.getAuthDomain(),
        containsInAnyOrder(userGroups.toArray()));
    assertThat(
        "All accessible workspaces from SAM policy models are returned",
        response.getWorkspaces(),
        is(accessible));
    assertThat(
        "All inaccessible workspaces from SAM policy models are returned",
        response.getInaccessibleWorkspaces(),
        is(inaccessible));
  }

  @Test
  void snapshotInaccessibleByPassportWhenNoLinkedPassport() {
    mockSnapshotSummaryWithPassportCriteria();

    SnapshotAccessibleResult result = service.snapshotAccessibleByPassport(snapshotId, TEST_USER);

    assertThat(result.accessible(), is(false));
    assertThat(
        "No throwable causes for inaccessibility when no linked passport",
        result.causes(),
        empty());
    verify(ecmService, times(1)).getRasProviderPassport(TEST_USER);
    verify(ecmService, never()).validatePassport(any());
  }

  @Test
  void snapshotInaccessibleByPassportWhenPassportFetchThrows() {
    mockSnapshotSummaryWithPassportCriteria();
    HttpClientErrorException ecmEx =
        new HttpClientErrorException(
            HttpStatus.I_AM_A_TEAPOT, "ecmService.getRasProviderPassport threw");
    when(ecmService.getRasProviderPassport(TEST_USER)).thenThrow(ecmEx);

    SnapshotAccessibleResult result = service.snapshotAccessibleByPassport(snapshotId, TEST_USER);

    assertThat(result.accessible(), is(false));
    assertThat(
        "ECM exception message is a throwable cause of inaccessibility",
        result.causes(),
        contains(ecmEx.getMessage()));
    verify(ecmService, times(1)).getRasProviderPassport(TEST_USER);
    verify(ecmService, never()).validatePassport(any());
  }

  @Test
  void snapshotInaccessibleByPassportWhenPassportValidationThrows() {
    mockSnapshotSummaryWithPassportCriteria();
    when(ecmService.getRasProviderPassport(TEST_USER)).thenReturn(PASSPORT);
    HttpClientErrorException ecmEx =
        new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT, "ecmService.validatePassport threw");
    when(ecmService.validatePassport(any())).thenThrow(ecmEx);

    SnapshotAccessibleResult result = service.snapshotAccessibleByPassport(snapshotId, TEST_USER);

    assertThat(result.accessible(), is(false));
    assertThat(
        "ECM exception message is a throwable cause of inaccessibility",
        result.causes(),
        contains(ecmEx.getMessage()));
    verify(ecmService, times(1)).getRasProviderPassport(TEST_USER);
    verify(ecmService, times(1)).validatePassport(any());
  }

  @Test
  void snapshotInaccessibleByPassportWhenPassportInvalid() {
    mockSnapshotSummaryWithPassportCriteria();
    when(ecmService.getRasProviderPassport(TEST_USER)).thenReturn(PASSPORT);
    when(ecmService.validatePassport(any())).thenReturn(new ValidatePassportResult().valid(false));

    SnapshotAccessibleResult result = service.snapshotAccessibleByPassport(snapshotId, TEST_USER);

    assertThat(result.accessible(), is(false));
    assertThat(
        "Passport invalid message is a throwable cause of inaccessibility",
        result.causes(),
        contains(service.passportInvalidForSnapshotErrorMsg(TEST_USER.getEmail())));
    verify(ecmService, times(1)).getRasProviderPassport(TEST_USER);
    verify(ecmService, times(1)).validatePassport(any());
  }

  @Test
  void snapshotAccessibleByPassportWhenPassportValid() {
    mockSnapshotSummaryWithPassportCriteria();
    when(ecmService.getRasProviderPassport(TEST_USER)).thenReturn(PASSPORT);
    when(ecmService.validatePassport(any())).thenReturn(new ValidatePassportResult().valid(true));

    SnapshotAccessibleResult result = service.snapshotAccessibleByPassport(snapshotId, TEST_USER);

    assertThat(result.accessible(), is(true));
    assertThat(result.causes(), empty());
    verify(ecmService, times(1)).getRasProviderPassport(TEST_USER);
    verify(ecmService, times(1)).validatePassport(any());
  }

  @Test
  void verifySnapshotListableBySam() {
    mockSnapshotSummary();
    service.verifySnapshotListable(snapshotId, TEST_USER);

    verify(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString());
    verify(ecmService, never()).getRasProviderPassport(TEST_USER);
    verify(ecmService, never()).validatePassport(any());
  }

  @Test
  void verifySnapshotReadableBySam() {
    mockSnapshotSummary();
    service.verifySnapshotReadable(snapshotId, TEST_USER);

    verify(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);
    verify(ecmService, never()).getRasProviderPassport(TEST_USER);
    verify(ecmService, never()).validatePassport(any());
  }

  @Test
  void verifySnapshotUnlistableBySamAndAccessibleByPassport() {
    IamForbiddenException samEx = new IamForbiddenException("Snapshot unlistable via SAM");
    doThrow(samEx)
        .when(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString());

    verifySnapshotInaccessibleBySamAndAccessibleByPassport(
        () -> service.verifySnapshotListable(snapshotId, TEST_USER));

    verify(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString());
  }

  @Test
  void verifySnapshotUnreadableBySamAndAccessibleByPassport() {
    IamForbiddenException samEx = new IamForbiddenException("Snapshot unreadable via SAM");
    doThrow(samEx)
        .when(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);

    verifySnapshotInaccessibleBySamAndAccessibleByPassport(
        () -> service.verifySnapshotReadable(snapshotId, TEST_USER));

    verify(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);
  }

  /**
   * Assumes that iamService has been mocked to indicate that the snapshot is inaccessible via Sam.
   */
  private void verifySnapshotInaccessibleBySamAndAccessibleByPassport(
      Runnable verifySnapshotAccessible) {
    mockSnapshotSummaryWithPassportCriteria();
    when(ecmService.getRasProviderPassport(TEST_USER)).thenReturn(PASSPORT);

    when(ecmService.validatePassport(any())).thenReturn(new ValidatePassportResult().valid(true));

    verifySnapshotAccessible.run();

    verify(ecmService).getRasProviderPassport(TEST_USER);
    verify(ecmService).validatePassport(any());
  }

  @Test
  void verifySnapshotUnlistableBySamAndInaccessibleByPassport() {
    IamForbiddenException samEx = new IamForbiddenException("Snapshot unlistable via SAM");
    doThrow(samEx)
        .when(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString());

    verifySnapshotInaccessibleBySamAndPassport(
        samEx, () -> service.verifySnapshotListable(snapshotId, TEST_USER));

    verify(iamService)
        .verifyAuthorization(TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString());
  }

  @Test
  void verifySnapshotUnreadableBySamAndInaccessibleByPassport() {
    IamForbiddenException samEx = new IamForbiddenException("Snapshot unreadable via SAM");
    doThrow(samEx)
        .when(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);

    verifySnapshotInaccessibleBySamAndPassport(
        samEx, () -> service.verifySnapshotReadable(snapshotId, TEST_USER));

    verify(iamService)
        .verifyAuthorization(
            TEST_USER, IamResourceType.DATASNAPSHOT, snapshotId.toString(), IamAction.READ_DATA);
  }

  /**
   * Assumes that iamService has been mocked to indicate that the snapshot is inaccessible via Sam.
   */
  private void verifySnapshotInaccessibleBySamAndPassport(
      IamForbiddenException samEx, Executable verifySnapshotAccessible) {
    mockSnapshotSummaryWithPassportCriteria();
    when(ecmService.getRasProviderPassport(TEST_USER)).thenReturn(PASSPORT);

    // No SAM access and an invalid passport = inaccessible snapshot
    when(ecmService.validatePassport(any())).thenReturn(new ValidatePassportResult().valid(false));

    ForbiddenException thrown = assertThrows(ForbiddenException.class, verifySnapshotAccessible);
    assertThat(
        "SAM and ECM exception messages returned when ECM passport is invalid",
        thrown.getCauses(),
        contains(
            samEx.getMessage(), service.passportInvalidForSnapshotErrorMsg(TEST_USER.getEmail())));

    verify(ecmService).getRasProviderPassport(TEST_USER);
    verify(ecmService).validatePassport(any());
  }

  private SnapshotRequestModel getDuosSnapshotRequestModel(String duosId) {
    String sourceDatasetName = "TestSourceDataset";
    Dataset sourceDataset = new Dataset().name(sourceDatasetName);
    when(datasetService.retrieveByName(sourceDatasetName)).thenReturn(sourceDataset);
    return new SnapshotRequestModel()
        .name("TestSnapshot")
        .profileId(UUID.randomUUID())
        .duosId(duosId)
        .contents(List.of(new SnapshotRequestContentsModel().datasetName(sourceDatasetName)));
  }

  @Test
  void testCreateSnapshotWithoutDuosDataset() {
    SnapshotRequestModel request = getDuosSnapshotRequestModel(null);
    JobBuilder jobBuilder = mock(JobBuilder.class);
    when(jobService.newJob(anyString(), eq(SnapshotCreateFlight.class), eq(request), eq(TEST_USER)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(any(), any())).thenReturn(jobBuilder);
    String jobId = String.valueOf(UUID.randomUUID());
    when(jobBuilder.submit()).thenReturn(jobId);

    String result = service.createSnapshot(request, TEST_USER);
    assertThat("Job is submitted and id returned", result, equalTo(jobId));
    verify(duosClient, never()).getDataset(DUOS_ID, TEST_USER);
    verify(jobBuilder).submit();
  }

  @Test
  void testCreateSnapshotWithDuosDataset() {
    SnapshotRequestModel request = getDuosSnapshotRequestModel(DUOS_ID);
    JobBuilder jobBuilder = mock(JobBuilder.class);
    when(jobService.newJob(anyString(), eq(SnapshotCreateFlight.class), eq(request), eq(TEST_USER)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(any(), any())).thenReturn(jobBuilder);
    String jobId = String.valueOf(UUID.randomUUID());
    when(jobBuilder.submit()).thenReturn(jobId);

    String result = service.createSnapshot(request, TEST_USER);
    assertThat("Job is submitted and id returned", result, equalTo(jobId));
    verify(duosClient).getDataset(DUOS_ID, TEST_USER);
    verify(jobBuilder).submit();
  }

  @Test
  void testCreateSnapshotThrowsWhenDuosClientThrows() {
    SnapshotRequestModel request = getDuosSnapshotRequestModel(DUOS_ID);
    HttpClientErrorException expectedEx = new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT);
    when(duosClient.getDataset(DUOS_ID, TEST_USER)).thenThrow(expectedEx);
    assertThrows(HttpClientErrorException.class, () -> service.createSnapshot(request, TEST_USER));
    JobBuilder jobBuilder = mock(JobBuilder.class);
    verifyNoInteractions(jobBuilder);
  }

  private void mockSnapshotWithDuosDataset() {
    Snapshot snapshot =
        new Snapshot()
            .id(snapshotId)
            .duosFirecloudGroupId(duosFirecloudGroup.getId())
            .duosFirecloudGroup(duosFirecloudGroup);
    when(snapshotDao.retrieveSnapshot(snapshotId)).thenReturn(snapshot);
  }

  @Test
  void testUpdateSnapshotDuosDataset() {
    mockSnapshotWithDuosDataset();

    JobBuilder jobBuilder = mock(JobBuilder.class);
    SnapshotLinkDuosDatasetResponse jobResponse =
        new SnapshotLinkDuosDatasetResponse().unlinked(duosFirecloudGroup);
    when(jobBuilder.addParameter(any(), any())).thenReturn(jobBuilder);
    when(jobBuilder.submitAndWait(SnapshotLinkDuosDatasetResponse.class)).thenReturn(jobResponse);

    when(jobService.newJob(
            anyString(), eq(SnapshotUpdateDuosDatasetFlight.class), eq(null), eq(TEST_USER)))
        .thenReturn(jobBuilder);

    SnapshotLinkDuosDatasetResponse result =
        service.updateSnapshotDuosDataset(snapshotId, TEST_USER, DUOS_ID);
    assertThat("Job is submitted and response returned", result, equalTo(jobResponse));

    // Verify that we checked that a DUOS dataset exists for the DUOS ID
    verify(duosClient).getDataset(DUOS_ID, TEST_USER);
    // Verify critical parameters passed to job
    verify(jobBuilder).addParameter(JobMapKeys.SNAPSHOT_ID.getKeyName(), snapshotId);
    verify(jobBuilder).addParameter(SnapshotDuosMapKeys.DUOS_ID, DUOS_ID);
    verify(jobBuilder).addParameter(SnapshotDuosMapKeys.FIRECLOUD_GROUP_PREV, duosFirecloudGroup);
  }

  @Test
  void testUpdateSnapshotDuosDatasetUnset() {
    mockSnapshotWithDuosDataset();

    JobBuilder jobBuilder = mock(JobBuilder.class);
    SnapshotLinkDuosDatasetResponse jobResponse =
        new SnapshotLinkDuosDatasetResponse().unlinked(duosFirecloudGroup);
    when(jobBuilder.addParameter(any(), any())).thenReturn(jobBuilder);
    when(jobBuilder.submitAndWait(SnapshotLinkDuosDatasetResponse.class)).thenReturn(jobResponse);

    when(jobService.newJob(
            anyString(), eq(SnapshotUpdateDuosDatasetFlight.class), eq(null), eq(TEST_USER)))
        .thenReturn(jobBuilder);

    SnapshotLinkDuosDatasetResponse result =
        service.updateSnapshotDuosDataset(snapshotId, TEST_USER, null);
    assertThat("Job is submitted and response returned", result, equalTo(jobResponse));

    // Verify that we do not try to check for DUOS dataset existence given an unspecified DUOS ID
    verify(duosClient, never()).getDataset(any(), eq(TEST_USER));
    // Verify critical parameters passed to job
    verify(jobBuilder).addParameter(JobMapKeys.SNAPSHOT_ID.getKeyName(), snapshotId);
    verify(jobBuilder).addParameter(SnapshotDuosMapKeys.DUOS_ID, null);
    verify(jobBuilder).addParameter(SnapshotDuosMapKeys.FIRECLOUD_GROUP_PREV, duosFirecloudGroup);
  }

  @Test
  void testUpdateSnapshotDuosDatasetThrowsWhenSnapshotDoesNotExist() {
    SnapshotNotFoundException expectedEx = new SnapshotNotFoundException("Snapshot not found");
    when(snapshotDao.retrieveSnapshot(snapshotId)).thenThrow(expectedEx);

    SnapshotNotFoundException thrown =
        assertThrows(
            SnapshotNotFoundException.class,
            () -> service.updateSnapshotDuosDataset(snapshotId, TEST_USER, DUOS_ID));
    assertThat("Snapshot retrieval exception thrown", thrown, equalTo(expectedEx));

    // Verify that we do not try to check for DUOS dataset existence if snapshot does not exist
    verify(duosClient, never()).getDataset(DUOS_ID, TEST_USER);
    // Job is not created or submitted if snapshot does not exist
    verifyNoInteractions(jobService);
  }

  @Test
  void testUpdateSnapshotDuosDatasetThrowsWhenDuosClientThrows() {
    mockSnapshotWithDuosDataset();

    HttpClientErrorException expectedEx = new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT);
    when(duosClient.getDataset(DUOS_ID, TEST_USER)).thenThrow(expectedEx);

    HttpClientErrorException thrown =
        assertThrows(
            HttpClientErrorException.class,
            () -> service.updateSnapshotDuosDataset(snapshotId, TEST_USER, DUOS_ID));
    assertThat("DUOS client exception thrown", thrown, equalTo(expectedEx));
  }

  @Test
  void getSnapshotIdsAndRoles() throws ParseException {
    // Arranging Sam-accessible snapshots (could contain snapshots registered in a different TDR)
    UUID accessibleNonTdrSnapshotId = UUID.randomUUID();
    IamRole role = IamRole.DISCOVERER;
    Map<UUID, Set<IamRole>> resourcesAndRoles =
        Map.of(snapshotId, Set.of(role), accessibleNonTdrSnapshotId, Set.of(role));
    when(iamService.listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT))
        .thenReturn(resourcesAndRoles);

    // Arranging RAS-accessible snapshots (ECM could throw)
    HttpClientErrorException ecmException = new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT);
    when(ecmService.getRasDbgapPermissions(TEST_USER))
        .thenThrow(ecmException)
        .thenReturn(List.of());

    // Arranging TDR snapshots (could contain inaccessible snapshots)
    UUID inaccessibleTdrSnapshotId = UUID.randomUUID();
    when(snapshotDao.getSnapshotIds()).thenReturn(List.of(snapshotId, inaccessibleTdrSnapshotId));

    // First invocation: error which may yield a partial role map
    SnapshotIdsAndRolesModel result = service.getSnapshotIdsAndRoles(TEST_USER);
    verify(iamService).listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT);
    verify(ecmService).getRasDbgapPermissions(TEST_USER);
    verify(snapshotDao).getSnapshotIds();

    List<ErrorModel> errors = result.getErrors();
    Map<String, List<String>> roleMap = result.getRoleMap();
    assertThat("ECM error added to error list", errors, hasSize(1));
    assertThat(
        "ECM error contents match expectations",
        errors.get(0).getMessage(),
        containsString("Error listing RAS-authorized snapshots"));
    assertThat(
        "Role map only contains accessible snapshot present in TDR",
        roleMap.containsKey(snapshotId.toString()));
    assertThat(
        "Snapshot ID maps to its roles",
        roleMap.get(snapshotId.toString()),
        contains(role.toString()));

    // Second invocation: no errors encountered when constructing role map
    result = service.getSnapshotIdsAndRoles(TEST_USER);
    verify(iamService, times(2)).listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT);
    verify(ecmService, times(2)).getRasDbgapPermissions(TEST_USER);
    verify(snapshotDao, times(2)).getSnapshotIds();

    errors = result.getErrors();
    roleMap = result.getRoleMap();
    assertThat("No errors encountered when constructing role map", errors, empty());
    assertThat(
        "Role map only contains accessible snapshot present in TDR",
        roleMap.containsKey(snapshotId.toString()));
    assertThat(
        "Snapshot ID maps to its roles",
        roleMap.get(snapshotId.toString()),
        contains(role.toString()));
  }

  @Test
  void testRetrievePreviewGCPNoRows() {
    mockSnapshotForPreview(CloudPlatform.GCP, 0);
    testSnapshotPreviewRowCountsGCP(0, 0);
  }

  @Test
  void testRetrievePreviewGCPNoFilteredRows() {
    mockSnapshotForPreview(CloudPlatform.GCP, 10);
    testSnapshotPreviewRowCountsGCP(10, 0);
  }

  @Test
  void testRetrievePreviewGCP() {
    mockSnapshotForPreview(CloudPlatform.GCP, 10);
    testSnapshotPreviewRowCountsGCP(10, 4);
  }

  @Test
  void testRetrievePreviewAzurePNoRows() throws SQLException {
    mockSnapshotForPreview(CloudPlatform.AZURE, 0);
    testSnapshotPreviewRowCountsAzure(0, 0);
  }

  @Test
  void testRetrievePreviewAzureNoFilteredRows() throws SQLException {
    mockSnapshotForPreview(CloudPlatform.AZURE, 10);
    testSnapshotPreviewRowCountsAzure(10, 0);
  }

  @Test
  void testRetrievePreviewAzure() throws SQLException {
    mockSnapshotForPreview(CloudPlatform.AZURE, 10);
    testSnapshotPreviewRowCountsAzure(10, 4);
  }

  @Test
  void testPatchSnapshotAuthDomain() {
    List<String> userGroups = List.of("testGroup");
    var flightClass = SnapshotAddDataAccessControlsFlight.class;
    JobBuilder jobBuilder = new JobBuilder(null, flightClass, null, TEST_USER, jobService);
    AddAuthDomainResponseModel jobResponse =
        new AddAuthDomainResponseModel().authDomain(userGroups);
    when(jobService.newJob(anyString(), eq(flightClass), eq(userGroups), eq(TEST_USER)))
        .thenReturn(jobBuilder);
    ArgumentCaptor<FlightMap> flightMapCaptor = ArgumentCaptor.forClass(FlightMap.class);
    when(jobService.submitAndWait(
            eq(flightClass), flightMapCaptor.capture(), eq(AddAuthDomainResponseModel.class)))
        .thenReturn(jobResponse);

    AddAuthDomainResponseModel result =
        service.addSnapshotDataAccessControls(TEST_USER, snapshotId, userGroups);
    assertThat("Job is submitted and response returned", result, equalTo(jobResponse));
    assertThat(
        flightMapCaptor.getValue().get(JobMapKeys.SNAPSHOT_ID.getKeyName(), String.class),
        equalTo(snapshotId.toString()));
  }

  private void testPreview(int totalRowCount, int filteredRowCount) {
    SnapshotPreviewModel snapshotPreviewModel =
        service.retrievePreview(
            TEST_USER,
            snapshotId,
            SNAPSHOT_TABLE_NAME,
            10,
            0,
            PDAO_ROW_ID_COLUMN,
            SqlSortDirection.ASC,
            "");
    assertThat(
        "Correct total row count", snapshotPreviewModel.getTotalRowCount(), equalTo(totalRowCount));
    assertThat(
        "Correct filtered row count",
        snapshotPreviewModel.getFilteredRowCount(),
        equalTo(filteredRowCount));
  }

  private void testSnapshotPreviewRowCountsAzure(int totalRowCount, int filteredRowCount)
      throws SQLException {
    List<SynapseDataResultModel> values = new ArrayList<>();
    if (filteredRowCount > 0) {
      values.add(
          new SynapseDataResultModel()
              .filteredCount(filteredRowCount)
              .totalCount(totalRowCount)
              .rowResult(new HashMap<>()));
    }
    when(metadataDataAccessUtils.accessInfoFromSnapshot(any(), any(), any()))
        .thenReturn(
            new AccessInfoModel()
                .parquet(
                    new AccessInfoParquetModel()
                        .url("test.parquet.url")
                        .sasToken("test.sas.token")));
    when(azureSynapsePdao.getOrCreateExternalDataSourceForResource(any(), any(), any()))
        .thenReturn("");
    when(azureSynapsePdao.getTableData(
            any(), any(), any(), any(), anyInt(), anyInt(), any(), any(), any(), any()))
        .thenReturn(values);
    testPreview(totalRowCount, filteredRowCount);
  }

  private void testSnapshotPreviewRowCountsGCP(int totalRowCount, int filteredRowCount) {
    List<BigQueryDataResultModel> values = new ArrayList<>();
    if (filteredRowCount > 0) {
      values.add(
          new BigQueryDataResultModel()
              .filteredCount(filteredRowCount)
              .totalCount(totalRowCount)
              .rowResult(new HashMap<>()));
    }
    try (MockedStatic<BigQueryPdao> utilities = Mockito.mockStatic(BigQueryPdao.class)) {
      utilities
          .when(
              () ->
                  BigQueryPdao.getTable(
                      any(), any(), any(), anyInt(), anyInt(), any(), any(), any()))
          .thenReturn(values);
      testPreview(totalRowCount, filteredRowCount);
    }
  }

  private void mockSnapshotForPreview(CloudPlatform cloudPlatform, int totalRowCount) {
    List<Column> columns =
        List.of(
            new Column()
                .name(SNAPSHOT_COLUMN_NAME)
                .type(TableDataType.STRING)
                .arrayOf(true)
                .required(true));
    when(snapshotDao.retrieveSnapshot(any()))
        .thenReturn(
            new Snapshot()
                .name(SNAPSHOT_NAME)
                .snapshotTables(
                    List.of(
                        new SnapshotTable()
                            .name(SNAPSHOT_TABLE_NAME)
                            .id(snapshotTableId)
                            .columns(columns)
                            .rowCount(totalRowCount)))
                .snapshotSources(
                    List.of(
                        new SnapshotSource()
                            .dataset(
                                new Dataset(new DatasetSummary().cloudPlatform(cloudPlatform))))));
    if (cloudPlatform == CloudPlatform.GCP) {
      when(snapshotTableDao.retrieveColumns(any())).thenReturn(columns);
    }
  }
}
