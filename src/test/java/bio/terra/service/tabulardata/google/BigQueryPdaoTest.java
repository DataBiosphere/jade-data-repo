package bio.terra.service.tabulardata.google;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.PdaoConstant;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.BulkLoadHistoryModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.exception.BadExternalFileException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.stringtemplate.v4.ST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX;
import static bio.terra.common.PdaoConstant.PDAO_LOAD_HISTORY_TABLE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class BigQueryPdaoTest {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryPdaoTest.class);

    @Autowired
    private JsonLoader jsonLoader;
    @Autowired
    private ConnectedTestConfiguration testConfig;
    @Autowired
    private BigQueryPdao bigQueryPdao;
    @Autowired
    private DatasetDao datasetDao;
    @Autowired
    private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired
    private ConnectedOperations connectedOperations;
    @Autowired
    private ResourceService resourceService;

    @MockBean
    private IamProviderInterface samService;

    private BillingProfileModel profileModel;

    private final Storage storage = StorageOptions.getDefaultInstance().getService();

    @Rule
    public ExpectedException exceptionGrabber = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        connectedOperations.stubOutSamCalls(samService);

        String coreBillingAccount = testConfig.getGoogleBillingAccountId();
        profileModel = connectedOperations.createProfileForAccount(coreBillingAccount);
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    private String insertExample = "INSERT INTO `broad-jade-dev.datarepo_hca_ebi.datarepo_load_history_staging_x` " +
        "(load_tag, load_time, source_name, target_path, state, file_id, checksum_crc32c, checksum_md5, error) " +
        "VALUES ('ebi_2020_08_15-0', '2020-08-16T01:27:54.733370Z', 'gs://broad-dsp-storage/blahblah.fastq.gz', " +
        "'/target/path', 'failed', '', '', '', \"\"\"bio.terra.common.exception.PdaoSourceFileNotFoundException: " +
        "Source file not found: 'gs://broad-dsp-storage/blahblah.fastq.gz'\"\"\")";

    @Test
    public void sanitizeErrorMsgTest() {
        List<BulkLoadHistoryModel> loadHistoryArray = new ArrayList<>();
        BulkLoadHistoryModel loadHistoryModel = new BulkLoadHistoryModel();
        loadHistoryModel.setSourcePath("gs://broad-dsp-storage/blahblah.fastq.gz");
        loadHistoryModel.setTargetPath("/target/path");
        loadHistoryModel.setState(BulkLoadFileState.FAILED);
        loadHistoryModel.setError("bio.terra.common.exception.PdaoSourceFileNotFoundException: " +
            "Source file not found: 'gs://broad-dsp-storage/blahblah.fastq.gz'");
        loadHistoryArray.add(loadHistoryModel);

        ST sqlTemplate = new ST(bigQueryPdao.insertLoadHistoryToStagingTableTemplate);
        sqlTemplate.add("project", "broad-jade-dev");
        sqlTemplate.add("dataset", "datarepo_hca_ebi");
        sqlTemplate.add("stagingTable", PDAO_LOAD_HISTORY_STAGING_TABLE_PREFIX + "x");

        sqlTemplate.add("load_history_array", loadHistoryArray);
        sqlTemplate.add("load_tag", "ebi_2020_08_15-0");
        sqlTemplate.add("load_time", "2020-08-16T01:27:54.733370Z");

        assertEquals(insertExample, sqlTemplate.render());
    }

    @Test
    public void basicTest() throws Exception {
        Dataset dataset = readDataset("ingest-test-dataset.json");
        try {
            assertThatDatasetAndTablesShouldExist(dataset, false);

            bigQueryPdao.createDataset(dataset);
            assertThatDatasetAndTablesShouldExist(dataset, true);

            // Perform the redo, which should delete and re-create
            bigQueryPdao.createDataset(dataset);
            assertThatDatasetAndTablesShouldExist(dataset, true);

            // Now delete it and test that it is gone
            bigQueryPdao.deleteDataset(dataset);
            assertThatDatasetAndTablesShouldExist(dataset, false);
        } finally {
            datasetDao.delete(dataset.getId());
        }
    }

    @Test
    public void datasetTest() throws Exception {
        Dataset defaultDataset = readDataset("ingest-test-dataset.json");
        Dataset eastDataset = readDataset("ingest-test-dataset-east.json");
        List<Object[]> testCases = Arrays.asList(
            new Object[]{defaultDataset, testConfig.getIngestbucket(),
                "BiqQuery datasets are instantiated in us-central1 by default."},
            new Object[]{eastDataset, testConfig.getNonDefaultRegionIngestBucket(),
                "BiqQuery datasets can be set to the non-default region."});

        for (Object[] tuple : testCases) {
            String targetPath = "scratch/file" + UUID.randomUUID().toString() + "/";
            Dataset dataset = (Dataset) tuple[0];
            String bucket = (String) tuple[1];
            String regionMessage = (String) tuple[2];

            String region = dataset.getDatasetSummary()
                .getStorageResourceRegion(GoogleCloudResource.BIGQUERY)
                .toString();

            connectedOperations.addDataset(dataset.getId().toString());

            // Stage tabular data for ingest.
            BlobInfo participantBlob = BlobInfo
                .newBuilder(bucket, targetPath + "ingest-test-participant.json")
                .build();
            BlobInfo sampleBlob = BlobInfo
                .newBuilder(bucket, targetPath + "ingest-test-sample.json")
                .build();
            BlobInfo fileBlob = BlobInfo
                .newBuilder(bucket, targetPath + "ingest-test-file.json")
                .build();

            BlobInfo missingPkBlob = BlobInfo
                .newBuilder(bucket, targetPath + "ingest-test-sample-no-id.json")
                .build();
            BlobInfo nullPkBlob = BlobInfo
                .newBuilder(bucket, targetPath + "ingest-test-sample-null-id.json")
                .build();

            try {
                bigQueryPdao.createDataset(dataset);

                com.google.cloud.bigquery.Dataset bqDataset = bigQueryDataset(dataset);
                assertThat(regionMessage, bqDataset.getLocation(), equalTo(region));

                storage.create(participantBlob, readFile("ingest-test-participant.json"));
                storage.create(sampleBlob, readFile("ingest-test-sample.json"));
                storage.create(fileBlob, readFile("ingest-test-file.json"));
                storage.create(missingPkBlob, readFile("ingest-test-sample-no-id.json"));
                storage.create(nullPkBlob, readFile("ingest-test-sample-null-id.json"));

                // Ingest staged data into the new dataset.
                IngestRequestModel ingestRequest = new IngestRequestModel()
                    .format(IngestRequestModel.FormatEnum.JSON);

                String datasetId = dataset.getId().toString();
                connectedOperations.ingestTableSuccess(datasetId,
                    ingestRequest.table("participant").path(gsPath(participantBlob)));
                connectedOperations.ingestTableSuccess(datasetId,
                    ingestRequest.table("sample").path(gsPath(sampleBlob)));
                connectedOperations.ingestTableSuccess(datasetId,
                    ingestRequest.table("file").path(gsPath(fileBlob)));

                // Check primary key non-nullability is enforced.
                connectedOperations.ingestTableFailure(datasetId,
                    ingestRequest.table("sample").path(gsPath(missingPkBlob)));
                connectedOperations.ingestTableFailure(datasetId,
                    ingestRequest.table("sample").path(gsPath(nullPkBlob)));

                // Create a snapshot!
                DatasetSummaryModel datasetSummaryModel =
                    DatasetJsonConversion.datasetSummaryModelFromDatasetSummary(dataset.getDatasetSummary());
                SnapshotSummaryModel snapshotSummary =
                    connectedOperations.createSnapshot(datasetSummaryModel,
                        "ingest-test-snapshot.json", "");
                SnapshotModel snapshot = connectedOperations.getSnapshot(snapshotSummary.getId());

                com.google.cloud.bigquery.Dataset bqSnapshotDataset = bigQuerySnapshot(dataset, snapshot.getName());

                assertThat(
                    String.format("Snapshot for dataset in region %s should also also be in region %s",
                        region, region),
                    region, equalTo(bqSnapshotDataset.getLocation()));

                BigQueryProject bigQueryProject = TestUtils.bigQueryProjectForDatasetName(
                    datasetDao, dataset.getName());
                Assert.assertThat(snapshot.getTables().size(), is(equalTo(3)));
                List<String> participantIds = queryForIds(snapshot.getName(), "participant", bigQueryProject);
                List<String> sampleIds = queryForIds(snapshot.getName(), "sample", bigQueryProject);
                List<String> fileIds = queryForIds(snapshot.getName(), "file", bigQueryProject);

                Assert.assertThat(participantIds, containsInAnyOrder(
                    "participant_1", "participant_2", "participant_3", "participant_4", "participant_5"));
                Assert.assertThat(sampleIds, containsInAnyOrder("sample1", "sample2", "sample5"));
                Assert.assertThat(fileIds, is(equalTo(Collections.singletonList("file1"))));

                // Simulate soft-deleting some rows.
                // TODO: Replace this with a call to the soft-delete API once it exists?
                softDeleteRows(bigQueryProject, bigQueryPdao.prefixName(dataset.getName()),
                    getTable(dataset, "participant"), Arrays.asList("participant_3", "participant_4"));
                softDeleteRows(
                    bigQueryProject, bigQueryPdao.prefixName(dataset.getName()), getTable(dataset, "sample"),
                    Collections.singletonList("sample5"));
                softDeleteRows(
                    bigQueryProject, bigQueryPdao.prefixName(dataset.getName()), getTable(dataset, "file"),
                    Collections.singletonList("file1"));

                // Create another snapshot.
                snapshotSummary = connectedOperations.createSnapshot(
                    datasetSummaryModel, "ingest-test-snapshot.json", "");
                SnapshotModel snapshot2 = connectedOperations.getSnapshot(snapshotSummary.getId());
                Assert.assertThat(snapshot2.getTables().size(), is(equalTo(3)));

                participantIds = queryForIds(snapshot2.getName(), "participant", bigQueryProject);
                sampleIds = queryForIds(snapshot2.getName(), "sample", bigQueryProject);
                fileIds = queryForIds(snapshot2.getName(), "file", bigQueryProject);
                Assert.assertThat(participantIds, containsInAnyOrder(
                    "participant_1", "participant_2", "participant_5"));
                Assert.assertThat(sampleIds, containsInAnyOrder("sample1", "sample2"));
                Assert.assertThat(fileIds, is(empty()));

                // Make sure the old snapshot wasn't changed.
                participantIds = queryForIds(snapshot.getName(), "participant", bigQueryProject);
                sampleIds = queryForIds(snapshot.getName(), "sample", bigQueryProject);
                fileIds = queryForIds(snapshot.getName(), "file", bigQueryProject);
                Assert.assertThat(participantIds, containsInAnyOrder(
                    "participant_1", "participant_2", "participant_3", "participant_4", "participant_5"));
                Assert.assertThat(sampleIds, containsInAnyOrder("sample1", "sample2", "sample5"));
                Assert.assertThat(fileIds, is(equalTo(Collections.singletonList("file1"))));
            } finally {
                storage.delete(participantBlob.getBlobId(), sampleBlob.getBlobId(),
                    fileBlob.getBlobId(), missingPkBlob.getBlobId(), nullPkBlob.getBlobId());
            }
        }
    }

    @Test
    public void nonStringAssetRootTest() throws Exception {
        Dataset dataset = readDataset("ingest-test-dataset.json");
        connectedOperations.addDataset(dataset.getId().toString());

        // Stage tabular data for ingest.
        String targetPath = "scratch/file" + UUID.randomUUID().toString() + "/";

        String bucket = testConfig.getIngestbucket();

        BlobInfo participantBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-participant.json")
            .build();
        BlobInfo sampleBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-sample.json")
            .build();
        BlobInfo fileBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-file.json")
            .build();

        try {
            bigQueryPdao.createDataset(dataset);

            storage.create(participantBlob, readFile("ingest-test-participant.json"));
            storage.create(sampleBlob, readFile("ingest-test-sample.json"));
            storage.create(fileBlob, readFile("ingest-test-file.json"));

            // Ingest staged data into the new dataset.
            IngestRequestModel ingestRequest = new IngestRequestModel()
                .format(IngestRequestModel.FormatEnum.JSON);

            String datasetId = dataset.getId().toString();
            connectedOperations.ingestTableSuccess(datasetId,
                ingestRequest.table("participant").path(gsPath(participantBlob)));
            connectedOperations.ingestTableSuccess(datasetId,
                ingestRequest.table("sample").path(gsPath(sampleBlob)));
            connectedOperations.ingestTableSuccess(datasetId,
                ingestRequest.table("file").path(gsPath(fileBlob)));

            // Create a snapshot!
            DatasetSummaryModel datasetSummaryModel =
                DatasetJsonConversion.datasetSummaryModelFromDatasetSummary(dataset.getDatasetSummary());
            SnapshotSummaryModel snapshotSummary =
                connectedOperations.createSnapshot(datasetSummaryModel,
                    "ingest-test-snapshot-by-date.json", "");
            SnapshotModel snapshot = connectedOperations.getSnapshot(snapshotSummary.getId());

            BigQueryProject bigQueryProject =
                TestUtils.bigQueryProjectForDatasetName(datasetDao, dataset.getName());
            Assert.assertThat(snapshot.getTables().size(), is(equalTo(3)));
            List<String> participantIds = queryForIds(snapshot.getName(), "participant", bigQueryProject);
            List<String> sampleIds = queryForIds(snapshot.getName(), "sample", bigQueryProject);
            List<String> fileIds = queryForIds(snapshot.getName(), "file", bigQueryProject);

            Assert.assertThat(participantIds, containsInAnyOrder(
                "participant_1", "participant_2", "participant_3", "participant_4", "participant_5"));
            Assert.assertThat(sampleIds, containsInAnyOrder("sample1", "sample2", "sample5"));
            Assert.assertThat(fileIds, is(equalTo(Collections.singletonList("file1"))));
        } finally {
            storage.delete(participantBlob.getBlobId(), sampleBlob.getBlobId(), fileBlob.getBlobId());
        }
    }

    @Test
    public void partitionTest() throws Exception {
        Dataset dataset = readDataset("ingest-test-partitioned-dataset.json");
        String bqDatasetName = bigQueryPdao.prefixName(dataset.getName());

        try {
            bigQueryPdao.createDataset(dataset);
            BigQueryProject bigQueryProject =
                TestUtils.bigQueryProjectForDatasetName(datasetDao, dataset.getName());

            for (String tableName : Arrays.asList("participant", "sample", "file")) {
                DatasetTable table = getTable(dataset, tableName);

                ST queryTemplate = new ST(queryPartitionsSummaryTemplate);
                queryTemplate.add("project", bigQueryProject.getProjectId());
                queryTemplate.add("dataset", bqDatasetName);
                queryTemplate.add("table", table.getRawTableName());

                QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(queryTemplate.render())
                    // Need legacy SQL to access the partitions summary meta-table.
                    .setUseLegacySql(true)
                    .build();

                // If the table isn't partitioned, this will go boom.
                bigQueryProject.getBigQuery().query(queryConfig);
            }

            // Make sure ingest date is exposed in live views, when it exists.
            ST queryTemplate = new ST(queryIngestDateTemplate);
            queryTemplate.add("project", bigQueryProject.getProjectId());
            queryTemplate.add("dataset", bqDatasetName);
            queryTemplate.add("table", "file");

            QueryJobConfiguration queryConfig = QueryJobConfiguration.of(queryTemplate.render());
            // If the column isn't exposed, this will go boom.
            bigQueryProject.getBigQuery().query(queryConfig);
        } finally {
            bigQueryPdao.deleteDataset(dataset);
            // Need to manually clean up the DAO because `readDataset` bypasses the
            // `connectedOperations` object, so we can't rely on its auto-teardown logic.
            datasetDao.delete(dataset.getId());
        }
    }

    @Test
    public void testGetFullViews() throws Exception {
        Dataset dataset = readDataset("ingest-test-dataset.json");
        connectedOperations.addDataset(dataset.getId().toString());

        // Stage tabular data for ingest.
        String targetPath = "scratch/file" + UUID.randomUUID().toString() + "/";

        String bucket = testConfig.getIngestbucket();

        BlobInfo participantBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-participant.json")
            .build();
        BlobInfo sampleBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-sample.json")
            .build();
        BlobInfo fileBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-file.json")
            .build();

        try {
            bigQueryPdao.createDataset(dataset);

            storage.create(participantBlob, readFile("ingest-test-participant.json"));
            storage.create(sampleBlob, readFile("ingest-test-sample.json"));
            storage.create(fileBlob, readFile("ingest-test-file.json"));

            // Ingest staged data into the new dataset.
            IngestRequestModel ingestRequest = new IngestRequestModel()
                .format(IngestRequestModel.FormatEnum.JSON);

            String datasetId = dataset.getId().toString();
            connectedOperations.ingestTableSuccess(datasetId,
                ingestRequest.table("participant").path(gsPath(participantBlob)));
            connectedOperations.ingestTableSuccess(datasetId,
                ingestRequest.table("sample").path(gsPath(sampleBlob)));
            connectedOperations.ingestTableSuccess(datasetId,
                ingestRequest.table("file").path(gsPath(fileBlob)));

            // Create a full-view snapshot!
            DatasetSummaryModel datasetSummary = DatasetJsonConversion.datasetSummaryModelFromDatasetSummary(
                dataset.getDatasetSummary());
            SnapshotSummaryModel snapshotSummary = connectedOperations.createSnapshot(datasetSummary,
                "snapshot-fullviews-test-snapshot.json", "");
            SnapshotModel snapshot = connectedOperations.getSnapshot(snapshotSummary.getId());

            BigQueryProject bigQueryProject =
                TestUtils.bigQueryProjectForDatasetName(datasetDao, dataset.getName());
            Assert.assertThat(snapshot.getTables().size(), is(equalTo(3)));
            List<String> participantIds = queryForIds(snapshot.getName(), "participant", bigQueryProject);
            List<String> sampleIds = queryForIds(snapshot.getName(), "sample", bigQueryProject);
            List<String> fileIds = queryForIds(snapshot.getName(), "file", bigQueryProject);

            Assert.assertThat(participantIds, containsInAnyOrder(
                "participant_1", "participant_2", "participant_3", "participant_4", "participant_5"));
            Assert.assertThat(sampleIds, containsInAnyOrder(
                "sample1", "sample2", "sample3", "sample4", "sample5", "sample6", "sample7"));
            Assert.assertThat(fileIds, is(equalTo(Collections.singletonList("file1"))));
        } finally {
            storage.delete(participantBlob.getBlobId(), sampleBlob.getBlobId(), fileBlob.getBlobId());
        }
    }

    @Test
    public void testBadSoftDeletePath() throws Exception {
        Dataset dataset = readDataset("ingest-test-dataset.json");
        String suffix = UUID.randomUUID().toString().replaceAll("-", "");
        String badGsUri = String.format("gs://%s/not/a/real/path/to/*files", testConfig.getIngestbucket());

        try {
            bigQueryPdao.createDataset(dataset);
            exceptionGrabber.expect(BadExternalFileException.class);
            bigQueryPdao.createSoftDeleteExternalTable(dataset, badGsUri, "participant", suffix);
        } finally {
            bigQueryPdao.deleteDataset(dataset);
            // Need to manually clean up the DAO because `readDataset` bypasses the
            // `connectedOperations` object, so we can't rely on its auto-teardown logic.
            datasetDao.delete(dataset.getId());
        }
    }

    private static final String snapshotTableDataSqlExample = "SELECT id, 'hello' as text" +
        " FROM UNNEST(GENERATE_ARRAY(1, 3)) AS id ORDER BY id;";

    @Test
    public void testGetSnapshotTableData() throws Exception {
        UUID profileId = UUID.fromString(profileModel.getId());
        String dataProjectId = googleResourceConfiguration.getSingleDataProjectId();
        Snapshot snapshot = new Snapshot().projectResource(new GoogleProjectResource()
            .profileId(profileId)
            .googleProjectId(dataProjectId)
        );
        List<Map<String, Object>> expected = getExampleSnapshotTableData();
        List<Map<String, Object>> actual = bigQueryPdao.getSnapshotTableData(snapshot, snapshotTableDataSqlExample);
        assertEquals(expected, actual);
    }

    private List<Map<String, Object>> getExampleSnapshotTableData() {
        List<Map<String, Object>> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
<<<<<<< HEAD
            values.add(Map.of("id", String.valueOf(i + 1), "text", "hello"));
=======
            values.add(Map.of( "text", "hello", "id", String.valueOf(i+1)));
>>>>>>> fd920807 (Add unit test for getSnapshotTableData)
        }

        return values;
    }

    public com.google.cloud.bigquery.Dataset bigQueryDataset(Dataset dataset) {
        return bigQueryPdao.bigQueryProjectForDataset(dataset)
            .getBigQuery().getDataset(bigQueryPdao.prefixName(dataset.getName()));
    }

    public com.google.cloud.bigquery.Dataset bigQuerySnapshot(Dataset dataset, String bigQueryDatasetName) {
        return bigQueryPdao.bigQueryProjectForDataset(dataset).getBigQuery().getDataset(bigQueryDatasetName);
    }

    private byte[] readFile(String fileName) throws IOException {
        return IOUtils.toByteArray(getClass().getClassLoader().getResource(fileName));
    }

    private String gsPath(BlobInfo blob) {
        return "gs://" + blob.getBucket() + "/" + blob.getName();
    }

    private static final String queryAllRowIdsTemplate =
        "SELECT " + PdaoConstant.PDAO_ROW_ID_COLUMN + " FROM `<project>.<dataset>.<table>` " +
            "WHERE id IN UNNEST([<ids:{id|'<id>'}; separator=\",\">])";

    private void softDeleteRows(BigQueryProject bq,
                                String datasetName,
                                DatasetTable table,
                                List<String> ids) throws Exception {

        ST sqlTemplate = new ST(queryAllRowIdsTemplate);
        sqlTemplate.add("project", bq.getProjectId());
        sqlTemplate.add("dataset", datasetName);
        sqlTemplate.add("table", table.getRawTableName());
        sqlTemplate.add("ids", ids);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlTemplate.render())
            .setDestinationTable(TableId.of(datasetName, table.getSoftDeleteTableName()))
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND)
            .build();

        bq.getBigQuery().query(queryConfig);
    }

    private static final String queryForIdsTemplate =
        "SELECT id FROM `<project>.<snapshot>.<table>` ORDER BY id";

    // Get the count of rows in a table or view
    private List<String> queryForIds(
        String snapshotName,
        String tableName,
        BigQueryProject bigQueryProject) throws Exception {
        String bigQueryProjectId = bigQueryProject.getProjectId();
        BigQuery bigQuery = bigQueryProject.getBigQuery();

        ST sqlTemplate = new ST(queryForIdsTemplate);
        sqlTemplate.add("project", bigQueryProjectId);
        sqlTemplate.add("snapshot", snapshotName);
        sqlTemplate.add("table", tableName);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlTemplate.render()).build();
        TableResult result = bigQuery.query(queryConfig);

        ArrayList<String> ids = new ArrayList<>();
        result.iterateAll().forEach(r -> ids.add(r.get("id").getStringValue()));

        return ids;
    }

    /* BigQuery Legacy SQL supports querying a "meta-table" about partitions
     * for any partitioned table.
     *
     * The table won't exist if the real table is unpartitioned, so we can
     * query it for a quick check to see if we enabled the expected options
     * on table creation.
     *
     * https://cloud.google.com/bigquery/docs/
     *   creating-partitioned-tables#listing_partitions_in_ingestion-time_partitioned_tables
     */
    private static final String queryPartitionsSummaryTemplate =
        "SELECT * FROM [<project>.<dataset>.<table>$__PARTITIONS_SUMMARY__]";

    private static final String queryIngestDateTemplate =
        "SELECT " + PdaoConstant.PDAO_INGEST_DATE_COLUMN_ALIAS + " FROM `<project>.<dataset>.<table>`";

    private String makeDatasetName() {
        return "pdaotest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
    }

    // NOTE: This method bypasses the `connectedOperations` object, and creates a dataset
    // using lower-level method calls. This means that the dataset entry isn't auto-cleaned
    // as part of `connectedOperations.teardown()`. If you forget to manually delete any
    // datasets from the DAO at the end of a test, you'll see a FK violation when `connectedOperations`
    // tries to delete the resource profile generated in `setup()`.
    private Dataset readDataset(String requestFile) throws Exception {
        String datasetName = makeDatasetName();
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(requestFile, DatasetRequestModel.class);
        datasetRequest
            .defaultProfileId(profileModel.getId())
            .name(datasetName);
        GoogleRegion region = DatasetJsonConversion.getRegionFromDatasetRequestModel(datasetRequest);
        UUID projectId = resourceService.getOrCreateDatasetProject(datasetName, profileModel, region);
        Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest)
            .projectResourceId(projectId)
            .projectResource(resourceService.getProjectResource(projectId));

        String createFlightId = UUID.randomUUID().toString();
        UUID datasetId = UUID.randomUUID();
        dataset
            .id(datasetId);
        datasetDao.createAndLock(dataset, createFlightId);
        datasetDao.unlockExclusive(dataset.getId(), createFlightId);
        return dataset;
    }

    private DatasetTable getTable(Dataset dataset, String name) {
        return dataset.getTableByName(name)
            .orElseThrow(() -> new IllegalStateException("Expected table " + name + " not found!"));
    }

    private void assertThatDatasetAndTablesShouldExist(Dataset dataset, boolean shouldExist)
        throws InterruptedException {

        boolean datasetExists = bigQueryPdao.tableExists(dataset, "participant");
        assertThat(
            String.format("Dataset: %s, exists", dataset.getName()),
            datasetExists,
            equalTo(shouldExist));

        boolean loadTableExists = bigQueryPdao.tableExists(dataset, PDAO_LOAD_HISTORY_TABLE);
        assertThat(
            String.format("Load Table: %s, exists", PDAO_LOAD_HISTORY_TABLE),
            loadTableExists,
            equalTo(shouldExist));

        for (String name : Arrays.asList("participant", "sample", "file")) {
            DatasetTable table = getTable(dataset, name);
            for (String t : Arrays.asList(table.getName(), table.getRawTableName(), table.getSoftDeleteTableName())) {
                assertThat(
                    "Table: " + dataset.getName() + "." + t + ", exists",
                    bigQueryPdao.tableExists(dataset, t),
                    equalTo(shouldExist));
            }
        }
    }
}
