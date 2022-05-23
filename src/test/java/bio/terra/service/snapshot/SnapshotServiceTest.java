package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoBigQueryModel;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.model.SnapshotSourceModel;
import bio.terra.model.StorageResourceModel;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.GoogleStorageResource;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

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

  @MockBean private JobService jobService;
  @MockBean private DatasetService datasetService;
  @MockBean private FireStoreDependencyDao dependencyDao;
  @MockBean private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Autowired private MetadataDataAccessUtils metadataDataAccessUtils;
  @MockBean private ResourceService resourceService;
  @MockBean private AzureBlobStorePdao azureBlobStorePdao;
  @MockBean private ProfileService profileService;
  @MockBean private SnapshotDao snapshotDao;

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
                .tables(List.of(new TableModel().name(SNAPSHOT_TABLE_NAME).primaryKey(List.of())))
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
                    List.of(new SnapshotTable().name(SNAPSHOT_TABLE_NAME).id(snapshotTableId)))
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
}
