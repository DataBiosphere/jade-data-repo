package bio.terra.service.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
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
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ActiveProfiles;

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

  @Mock private JobService jobService;
  @Mock private DatasetService datasetService;
  @Mock private FireStoreDependencyDao dependencyDao;
  @Mock private BigQueryPdao bigQueryPdao;
  @Mock private SnapshotDao snapshotDao;

  private MetadataDataAccessUtils metadataDataAccessUtils;

  private UUID snapshotId;
  private UUID datasetId;
  private UUID snapshotTableId;
  private UUID profileId;
  private Instant createdDate;

  private SnapshotService service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    metadataDataAccessUtils = new MetadataDataAccessUtils(null, null, null);
    service =
        new SnapshotService(
            jobService,
            datasetService,
            dependencyDao,
            bigQueryPdao,
            snapshotDao,
            metadataDataAccessUtils);

    snapshotId = UUID.randomUUID();
    datasetId = UUID.randomUUID();
    snapshotTableId = UUID.randomUUID();
    profileId = UUID.randomUUID();
    createdDate = Instant.now();
  }

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
                .tables(List.of(new TableModel().name(SNAPSHOT_TABLE_NAME)))
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
                                                    + "` LIMIT 1000")))))));
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
}
