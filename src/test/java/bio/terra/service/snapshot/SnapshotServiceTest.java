package bio.terra.service.snapshot;

import bio.terra.common.category.Unit;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoBigQueryModel;
import bio.terra.model.AccessInfoBigQueryModelTable;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRequestAccessIncludeModel;
import bio.terra.model.TableModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@Category(Unit.class)
public class SnapshotServiceTest {

    private static final String SNAPSHOT_NAME = "snapshotName";
    private static final String SNAPSHOT_DESCRIPTION = "snapshotDescription";
    private static final String SNAPSHOT_DATA_PROJECT = "tdrdataproject";
    private static final String SNAPSHOT_TABLE_NAME = "tableA";


    @Mock
    private JobService jobService;
    @Mock
    private DatasetService datasetService;
    @Mock
    private FireStoreDependencyDao dependencyDao;
    @Mock
    private BigQueryPdao bigQueryPdao;
    @Mock
    private SnapshotDao snapshotDao;

    private UUID snapshotId;
    private UUID snapshotTableId;
    private UUID profileId;
    private Instant createdDate;

    private SnapshotService service;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        service = new SnapshotService(jobService, datasetService, dependencyDao, bigQueryPdao, snapshotDao);

        snapshotId = UUID.randomUUID();
        snapshotTableId = UUID.randomUUID();
        profileId = UUID.randomUUID();
        createdDate = Instant.now();
    }

    @Test
    public void testRetrieveSnapshot() {
        mockSnapshot();
        assertThat(service.retrieveAvailableSnapshotModel(snapshotId), equalTo(
            new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())
                .source(Collections.emptyList())
                .tables(List.of(new TableModel()
                    .name(SNAPSHOT_TABLE_NAME)))
                .relationships(Collections.emptyList())
                .profileId(profileId)
                .dataProject(SNAPSHOT_DATA_PROJECT)
        ));
    }

    @Test
    public void testRetrieveSnapshotNoFields() {
        mockSnapshot();
        assertThat(service.retrieveAvailableSnapshotModel(snapshotId, List.of(SnapshotRequestAccessIncludeModel.NONE)),
            equalTo(new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())));
    }

    @Test
    public void testRetrieveSnapshotDefaultFields() {
        mockSnapshot();
        assertThat(service.retrieveAvailableSnapshotModel(snapshotId, List.of(
            SnapshotRequestAccessIncludeModel.SOURCES,
            SnapshotRequestAccessIncludeModel.TABLES,
            SnapshotRequestAccessIncludeModel.RELATIONSHIPS,
            SnapshotRequestAccessIncludeModel.PROFILE,
            SnapshotRequestAccessIncludeModel.DATA_PROJECT)),
            equalTo(service.retrieveAvailableSnapshotModel(snapshotId)));
    }

    @Test
    public void testRetrieveSnapshotOnlyAccessInfo() {
        mockSnapshot();
        assertThat(service.retrieveAvailableSnapshotModel(snapshotId, List.of(
            SnapshotRequestAccessIncludeModel.ACCESS_INFORMATION)),
            equalTo(new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())
                .accessInformation(new AccessInfoModel()
                    .bigQuery(new AccessInfoBigQueryModel()
                        .datasetName(SNAPSHOT_NAME)
                        .datasetId(SNAPSHOT_DATA_PROJECT + ":" + SNAPSHOT_NAME)
                        .projectId(SNAPSHOT_DATA_PROJECT)
                        .link("https://console.cloud.google.com/bigquery?project=" + SNAPSHOT_DATA_PROJECT +
                            "&ws=!" + SNAPSHOT_NAME + "&d=" + SNAPSHOT_NAME + "&p=" + SNAPSHOT_DATA_PROJECT +
                            "&page=dataset")
                        .tables(List.of(new AccessInfoBigQueryModelTable()
                            .name(SNAPSHOT_TABLE_NAME)
                            .qualifiedName(SNAPSHOT_DATA_PROJECT + "." + SNAPSHOT_NAME + "." + SNAPSHOT_TABLE_NAME)
                            .link("https://console.cloud.google.com/bigquery?project=" + SNAPSHOT_DATA_PROJECT +
                                "&ws=!" + SNAPSHOT_NAME + "&d=" + SNAPSHOT_NAME + "&p=" + SNAPSHOT_DATA_PROJECT +
                                "&page=table&t=" + SNAPSHOT_TABLE_NAME)
                            .id(SNAPSHOT_DATA_PROJECT + ":" + SNAPSHOT_NAME + "." + SNAPSHOT_TABLE_NAME)
                            .sampleQuery("SELECT * FROM `" + SNAPSHOT_DATA_PROJECT + "." + SNAPSHOT_NAME + "." +
                            SNAPSHOT_TABLE_NAME + "` LIMIT 1000")
                        ))
                    )
                )));
    }

    @Test
    public void testRetrieveSnapshotMultiInfo() {
        mockSnapshot();
        assertThat(service.retrieveAvailableSnapshotModel(snapshotId, List.of(
            SnapshotRequestAccessIncludeModel.PROFILE,
            SnapshotRequestAccessIncludeModel.SOURCES
            )),
            equalTo(new SnapshotModel()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate.toString())
                .source(Collections.emptyList())
                .profileId(profileId)));
    }

    private void mockSnapshot() {
        when(snapshotDao.retrieveAvailableSnapshot(snapshotId))
            .thenReturn(new Snapshot()
                .id(snapshotId)
                .name(SNAPSHOT_NAME)
                .description(SNAPSHOT_DESCRIPTION)
                .createdDate(createdDate)
                .profileId(profileId)
                .projectResource(new GoogleProjectResource()
                    .profileId(profileId)
                    .googleProjectId(SNAPSHOT_DATA_PROJECT))
                .snapshotTables(List.of(new SnapshotTable()
                    .name(SNAPSHOT_TABLE_NAME)
                    .id(snapshotTableId)))
            );
    }
}
