package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoBigQueryModel;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotPatchRequestModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.ras.EcmService;
import bio.terra.service.auth.ras.RasDbgapPermissions;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {SnapshotService.class, MetadataDataAccessUtils.class})
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SnapshotServiceTest {
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

  @MockBean private JobService jobService;
  @MockBean private DatasetService datasetService;
  @MockBean private FireStoreDependencyDao dependencyDao;
  @MockBean private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Autowired private MetadataDataAccessUtils metadataDataAccessUtils;
  @MockBean private ResourceService resourceService;
  @MockBean private AzureBlobStorePdao azureBlobStorePdao;
  @MockBean private ProfileService profileService;
  @MockBean private SnapshotDao snapshotDao;
  @MockBean private SnapshotTableDao snapshotTableDao;
  @MockBean private IamService iamService;
  @MockBean private AzureSynapsePdao synapsePdao;
  @MockBean private EcmService ecmService;

  private final UUID snapshotId = UUID.randomUUID();
  private final UUID datasetId = UUID.randomUUID();
  private final UUID snapshotTableId = UUID.randomUUID();
  private final UUID profileId = UUID.randomUUID();
  private final Instant createdDate = Instant.now();

  @Autowired private SnapshotService service;

  @Test
  public void testRetrieveSnapshot() {
    mockSnapshot();
    assertThat(
        service.retrieveAvailableSnapshotModel(snapshotId, TEST_USER),
        equalTo(
            new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())
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
                .dataProject(SNAPSHOT_DATA_PROJECT)));
  }

  @Test
  public void testRetrieveSnapshotNoFields() {
    mockSnapshot();
    assertThat(
        service.retrieveAvailableSnapshotModel(
            snapshotId, List.of(SnapshotRetrieveIncludeModel.NONE), TEST_USER),
        equalTo(
            new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())));
  }

  @Test
  public void testRetrieveSnapshotDefaultFields() {
    mockSnapshot();
    assertThat(
        service.retrieveAvailableSnapshotModel(
            snapshotId,
            List.of(
                SnapshotRetrieveIncludeModel.SOURCES,
                SnapshotRetrieveIncludeModel.TABLES,
                SnapshotRetrieveIncludeModel.RELATIONSHIPS,
                SnapshotRetrieveIncludeModel.PROFILE,
                SnapshotRetrieveIncludeModel.DATA_PROJECT),
            TEST_USER),
        equalTo(service.retrieveAvailableSnapshotModel(snapshotId, TEST_USER)));
  }

  @Test
  public void testRetrieveSnapshotOnlyCreationInfo() {
    mockSnapshot();
    assertThat(
        service.retrieveAvailableSnapshotModel(
            snapshotId, List.of(SnapshotRetrieveIncludeModel.CREATION_INFORMATION), TEST_USER),
        equalTo(
            new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())
                .creationInformation(
                    new SnapshotRequestContentsModel()
                        .mode(SnapshotRequestContentsModel.ModeEnum.BYFULLVIEW)
                        .datasetName(DATASET_NAME))));
  }

  @Test
  public void testRetrieveSnapshotOnlyAccessInfo() {
    mockSnapshot();
    assertThat(
        service.retrieveAvailableSnapshotModel(
            snapshotId, List.of(SnapshotRetrieveIncludeModel.ACCESS_INFORMATION), TEST_USER),
        equalTo(
            new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())
                .accessInformation(
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
                                                    + "`")))))));
  }

  @Test
  public void testRetrieveSnapshotMultiInfo() {
    mockSnapshot();
    assertThat(
        service.retrieveAvailableSnapshotModel(
            snapshotId,
            List.of(
                SnapshotRetrieveIncludeModel.PROFILE, SnapshotRetrieveIncludeModel.DATA_PROJECT),
            TEST_USER),
        equalTo(
            new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())
                .profileId(profileId)
                .dataProject(SNAPSHOT_DATA_PROJECT)));
  }

  private void mockSnapshot() {
    when(snapshotDao.retrieveAvailableSnapshot(snapshotId))
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
                        .datasetName(DATASET_NAME)));
  }

  @Test
  public void enumerateSnapshots() {
    IamRole role = IamRole.DISCOVERER;
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(snapshotId, Set.of(role));
    SnapshotSummary summary =
        new SnapshotSummary().id(snapshotId).createdDate(Instant.now()).storage(List.of());
    MetadataEnumeration<SnapshotSummary> metadataEnumeration = new MetadataEnumeration<>();
    metadataEnumeration.items(List.of(summary));
    when(snapshotDao.retrieveSnapshots(
            anyInt(), anyInt(), any(), any(), any(), any(), any(), eq(resourcesAndRoles.keySet())))
        .thenReturn(metadataEnumeration);
    var snapshots =
        service.enumerateSnapshots(0, 10, null, null, null, null, List.of(), resourcesAndRoles);
    assertThat(snapshots.getItems().get(0).getId(), equalTo(snapshotId));
    assertThat(snapshots.getRoleMap(), hasEntry(snapshotId.toString(), List.of(role.toString())));
  }

  @Test
  public void patchSnapshotIamActions() {
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
  }

  @Test
  public void listAuthorizedSnapshots() throws Exception {
    Map<UUID, Set<IamRole>> samAuthorizedSnapshots =
        Map.of(UUID.randomUUID(), Set.of(IamRole.STEWARD));
    when(ecmService.getRasDbgapPermissions(TEST_USER))
        .thenThrow(new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT));
    when(iamService.listAuthorizedResources(TEST_USER, IamResourceType.DATASNAPSHOT))
        .thenReturn(samAuthorizedSnapshots);

    assertThat(
        "ECM exception should not block snapshot enumeration",
        service.listAuthorizedSnapshots(TEST_USER),
        equalTo(samAuthorizedSnapshots));
    verify(ecmService, times(1)).getRasDbgapPermissions(TEST_USER);
  }

  @Test
  public void listRasAuthorizedSnapshots() throws Exception {
    String consentCode = "c01";
    String phsId = "phs123456";
    List<RasDbgapPermissions> perms = List.of(new RasDbgapPermissions(consentCode, phsId));
    List<UUID> uuids = List.of(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());

    // First attempt: No linked passport.
    when(ecmService.getRasDbgapPermissions(TEST_USER)).thenReturn(List.of());
    when(snapshotDao.getAccessibleSnapshots(perms)).thenReturn(List.of());
    assertThat(
        "No linked passport yields empty result",
        service.listRasAuthorizedSnapshots(TEST_USER).entrySet(),
        empty());

    // Second attempt: Linked passport, no matching snapshots.
    when(ecmService.getRasDbgapPermissions(TEST_USER)).thenReturn(perms);
    assertThat(
        "Linked passport but no matching snapshots yields empty result",
        service.listRasAuthorizedSnapshots(TEST_USER).entrySet(),
        empty());

    // Third attempt: Linked passport, matching snapshots.
    when(snapshotDao.getAccessibleSnapshots(perms)).thenReturn(uuids);
    Map<UUID, Set<IamRole>> idsAndRolesActual = service.listRasAuthorizedSnapshots(TEST_USER);
    assertThat(
        "Linked passport matching snapshots returns correct number of snapshots",
        idsAndRolesActual.size(),
        equalTo(uuids.size()));
    assertThat(
        "All matching snapshot UUIDs are found in map keys",
        idsAndRolesActual.keySet(),
        containsInAnyOrder(uuids.toArray()));
    assertThat(
        "Snapshot accessibility by passport attributed to reader role",
        idsAndRolesActual.values(),
        everyItem(equalTo(Set.of(IamRole.READER))));
  }

  @Test
  public void combineIdsAndRoles() {
    UUID[] uuids = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

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
}
