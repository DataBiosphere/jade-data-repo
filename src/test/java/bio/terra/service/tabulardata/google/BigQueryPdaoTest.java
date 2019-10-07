package bio.terra.service.tabulardata.google;

import bio.terra.category.Connected;
import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.service.dataset.Dataset;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.SamClientService;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class BigQueryPdaoTest {
    private static final Logger logger = LoggerFactory.getLogger(BigQueryPdaoTest.class);

    @Autowired private JsonLoader jsonLoader;
    @Autowired private ConnectedTestConfiguration testConfig;
    @Autowired private BigQueryPdao bigQueryPdao;
    @Autowired private DatasetDao datasetDao;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private DatasetService datasetService;

    @MockBean
    private SamClientService samService;

    private Dataset dataset;
    private BillingProfileModel profileModel;
    private Storage storage = StorageOptions.getDefaultInstance().getService();

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        connectedOperations.stubOutSamCalls(samService);

        String coreBillingAccount = googleResourceConfiguration.getCoreBillingAccount();
        profileModel = connectedOperations.createProfileForAccount(coreBillingAccount);
        // TODO: this next bit should be in connected operations, need to make it a component and autowire a datasetdao
        DatasetRequestModel datasetRequest = jsonLoader.loadObject("ingest-test-dataset.json",
            DatasetRequestModel.class);
        datasetRequest
            .defaultProfileId(profileModel.getId())
            .name(datasetName());
        dataset = DatasetJsonConversion.datasetRequestToDataset(datasetRequest);
        UUID datasetId = datasetDao.create(dataset);
        dataset = datasetService.retrieve(datasetId);
        logger.info("Created dataset in setup: {}", datasetId);
    }

    @After
    public void teardown() throws Exception {
        datasetDao.delete(dataset.getId());
        connectedOperations.teardown();
    }

    private String datasetName() {
        return "pdaotest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
    }

    private void assertThatDatasetAndTablesShouldExist(boolean shouldExist) {
        boolean datasetExists = bigQueryPdao.tableExists(dataset, "participant");
        assertThat(
            String.format("Dataset: %s, exists", dataset.getName()),
            datasetExists,
            equalTo(shouldExist));

        boolean participantTableExists = bigQueryPdao.tableExists(dataset, "participant");
        assertThat(
            String.format("Table: %s.participant, exists", dataset.getName()),
            participantTableExists,
            equalTo(shouldExist));
        String participantSoftDeleteTableName =  bigQueryPdao.prefixSoftDeleteTableName("participant");
        boolean participantSoftDeleteTableExists =  bigQueryPdao.tableExists(dataset, participantSoftDeleteTableName);
        assertThat(
            String.format("Table: %s.%s, exists", dataset.getName(), participantSoftDeleteTableName),
            participantSoftDeleteTableExists,
            equalTo(shouldExist));

        boolean sampleTableExists =  bigQueryPdao.tableExists(dataset, "sample");
        assertThat(
            String.format("Table: %s.sample, exists", dataset.getName()),
            sampleTableExists,
            equalTo(shouldExist));
        String sampleSoftDeleteTableName =  bigQueryPdao.prefixSoftDeleteTableName("sample");
        boolean sampleSoftDeleteTableExists =  bigQueryPdao.tableExists(dataset, sampleSoftDeleteTableName);
        assertThat(
            String.format("Table: %s.%s, exists", dataset.getName(), sampleSoftDeleteTableName),
            sampleSoftDeleteTableExists,
            equalTo(shouldExist));

        boolean fileTableExists =  bigQueryPdao.tableExists(dataset, "file");
        assertThat(
            String.format("Table: %s.file, exists", dataset.getName()),
            fileTableExists,
            equalTo(shouldExist));
        String fileSoftDeleteTableName =  bigQueryPdao.prefixSoftDeleteTableName("file");
        boolean fileSoftDeleteTableExists =  bigQueryPdao.tableExists(dataset, fileSoftDeleteTableName);
        assertThat(
            String.format("Table: %s.%s, exists", dataset.getName(), fileSoftDeleteTableName),
            fileSoftDeleteTableExists,
            equalTo(shouldExist));
    }

    private List<String> getSoftDeletedRowIds(Dataset dataset,
                                           String tableName,
                                           String projectId,
                                           BigQueryProject bigQueryProject) {

        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ")
            .append(PDAO_ROW_ID_COLUMN)
            .append(" FROM `")
            .append(projectId)
            .append(".")
            .append(bigQueryPdao.prefixName(dataset.getName()))
            .append(".")
            .append(bigQueryPdao.prefixSoftDeleteTableName(tableName))
            .append("`");

        String sql = builder.toString();
        TableResult result = bigQueryProject.query(sql);

        List<String> rowIds = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            String rowId = row.get(0).getStringValue();
            rowIds.add(rowId);
        }

        return rowIds;
    }

    private List<String> getRowIds(Dataset dataset,
                                   String tableName,
                                   String projectId,
                                   BigQueryProject bigQueryProject) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ")
            .append(PDAO_ROW_ID_COLUMN)
            .append(" FROM `")
            .append(projectId)
            .append(".")
            .append(bigQueryPdao.prefixName(dataset.getName()))
            .append(".")
            .append(tableName)
            .append("` EXCEPT DISTINCT (SELECT ")
            .append(PDAO_ROW_ID_COLUMN)
            .append(" FROM `")
            .append(projectId)
            .append(".")
            .append(bigQueryPdao.prefixName(dataset.getName()))
            .append(".")
            .append(bigQueryPdao.prefixSoftDeleteTableName(tableName))
            .append("`)");

        String sql = builder.toString();
        TableResult result = bigQueryProject.query(sql);

        List<String> rowIds = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            String rowId = row.get(0).getStringValue();
            rowIds.add(rowId);
        }

        return rowIds;
    }

    @Test
    public void basicTest() throws Exception {
        assertThatDatasetAndTablesShouldExist(false);

        bigQueryPdao.createDataset(dataset);
        assertThatDatasetAndTablesShouldExist(true);

        // Perform the redo, which should delete and re-create
        bigQueryPdao.createDataset(dataset);
        assertThatDatasetAndTablesShouldExist(true);

        // Now delete it and test that it is gone
        bigQueryPdao.deleteDataset(dataset);
        assertThatDatasetAndTablesShouldExist(false);
    }

    @Test
    public void datasetTest() throws Exception {
        bigQueryPdao.createDataset(dataset);

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

        BlobInfo missingPkBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-sample-no-id.json")
            .build();
        BlobInfo nullPkBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-sample-null-id.json")
            .build();

        BlobInfo updatedParticipantBlob = BlobInfo
            .newBuilder(bucket, targetPath + "ingest-test-updated-participant.json")
            .build();

        try {
            storage.create(participantBlob, readFile("ingest-test-participant.json"));
            storage.create(sampleBlob, readFile("ingest-test-sample.json"));
            storage.create(fileBlob, readFile("ingest-test-file.json"));
            storage.create(missingPkBlob, readFile("ingest-test-sample-no-id.json"));
            storage.create(nullPkBlob, readFile("ingest-test-sample-null-id.json"));

            // Ingest staged data into the new dataset.
            IngestRequestModel ingestRequest = new IngestRequestModel()
                .format(IngestRequestModel.FormatEnum.JSON);
            ingestRequest.setStrategy(IngestRequestModel.StrategyEnum.APPEND);

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
            MockHttpServletResponse snapshotResponse =
                connectedOperations.launchCreateSnapshot(datasetSummaryModel,
                    "ingest-test-snapshot.json", "");
            SnapshotSummaryModel snapshotSummary =
                connectedOperations.handleCreateSnapshotSuccessCase(snapshotResponse);
            SnapshotModel snapshot = connectedOperations.getSnapshot(snapshotSummary.getId());

            // TODO: Assert that the snapshot contains the rows we expect.
            // Skipping that for now because there's no REST API to query table contents.
            Assert.assertThat(snapshot.getTables().size(), is(equalTo(3)));

            BigQueryProject bigQueryProject = bigQueryPdao.bigQueryProjectForDataset(dataset);
            String tableName = "participant";
            List<String> rowIds = getRowIds(dataset,
                tableName,
                dataset.getDataProjectId(),
                bigQueryProject);
            int originalNumOfRows = rowIds.size();

            // Ingest updated data into the new dataset.
            storage.create(updatedParticipantBlob, readFile("ingest-test-updated-participant.json"));
            ingestRequest.setStrategy(IngestRequestModel.StrategyEnum.UPSERT);
            connectedOperations.ingestTableSuccess(datasetId,
                ingestRequest.table(tableName).path(gsPath(updatedParticipantBlob)));
            int numOfRowsSoftDeleted = 1;

            // assert the changed row is soft deleted
            List<String> softDeletedRowIds = getSoftDeletedRowIds(dataset,
                tableName,
                dataset.getDataProjectId(),
                bigQueryProject);
            Assert.assertThat("On upsert the changed row is soft deleted",
                softDeletedRowIds.size(),
                is(equalTo(numOfRowsSoftDeleted)));

            // assert only the new row is added
            rowIds = getRowIds(dataset,
                tableName,
                dataset.getDataProjectId(),
                bigQueryProject);
            // originalNumOfRows + 2 for the new row being added and the chnaged row being added
            Assert.assertThat("On upsert that # of rows being added accounts for the soft delete",
                rowIds.size(),
                is(equalTo(originalNumOfRows + 2 - numOfRowsSoftDeleted)));
        } finally {
            storage.delete(participantBlob.getBlobId(), sampleBlob.getBlobId(),
                fileBlob.getBlobId(), missingPkBlob.getBlobId(), nullPkBlob.getBlobId());
        }
    }

    private byte[] readFile(String fileName) throws IOException {
        return IOUtils.toByteArray(getClass().getClassLoader().getResource(fileName));
    }

    private String gsPath(BlobInfo blob) {
        return "gs://" + blob.getBucket() + "/" + blob.getName();
    }
}
