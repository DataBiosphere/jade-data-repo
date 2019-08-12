package bio.terra.pdao.bigquery;

import bio.terra.category.Connected;
import bio.terra.configuration.ConnectedTestConfiguration;
import bio.terra.dao.DatasetDao;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.metadata.Dataset;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.DatasetJsonConversion;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import bio.terra.service.SamClientService;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
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

import java.io.IOException;
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
    @Autowired private Storage storage;
    @Autowired private BigQueryPdao bigQueryPdao;
    @Autowired private DatasetDao datasetDao;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;
    @Autowired private ConnectedOperations connectedOperations;

    @MockBean
    private SamClientService samService;

    private Dataset dataset;
    private BillingProfileModel profileModel;

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        connectedOperations.stubOutSamCalls(samService);

        String coreBillingAccount = googleResourceConfiguration.getCoreBillingAccount();
        profileModel = connectedOperations.getOrCreateProfileForAccount(coreBillingAccount);
        // TODO: this next bit should be in connected operations, need to make it a component and autowire a datasetdao
        DatasetRequestModel datasetRequest = jsonLoader.loadObject("ingest-test-dataset.json",
            DatasetRequestModel.class);
        datasetRequest
            .defaultProfileId(profileModel.getId())
            .name(datasetName());
        dataset = DatasetJsonConversion.datasetRequestToDataset(datasetRequest);
        UUID datasetId = datasetDao.create(dataset);
        dataset.id(datasetId);
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

    @Test
    public void basicTest() throws Exception {
        boolean exists = bigQueryPdao.datasetExists(dataset);
        Assert.assertThat(exists, is(equalTo(false)));

        bigQueryPdao.createDataset(dataset);

        exists = bigQueryPdao.datasetExists(dataset);
        Assert.assertThat(exists, is(equalTo(true)));

        // Perform the redo, which should delete and re-create
        bigQueryPdao.createDataset(dataset);
        exists = bigQueryPdao.datasetExists(dataset);
        Assert.assertThat(exists, is(equalTo(true)));


        // Now delete it and test that it is gone
        bigQueryPdao.deleteDataset(dataset);
        exists = bigQueryPdao.datasetExists(dataset);
        Assert.assertThat(exists, is(equalTo(false)));
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

        try {
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
            MockHttpServletResponse snapshotResponse =
                connectedOperations.launchCreateSnapshot(datasetSummaryModel, "ingest-test-snapshot.json", "");
            SnapshotSummaryModel snapshotSummary =
                connectedOperations.handleCreateSnapshotSuccessCase(snapshotResponse);
            SnapshotModel snapshot = connectedOperations.getSnapshot(snapshotSummary.getId());

            // TODO: Assert that the snapshot contains the rows we expect.
            // Skipping that for now because there's no REST API to query table contents.
            Assert.assertThat(snapshot.getTables().size(), is(equalTo(3)));
        } finally {
            storage.delete(participantBlob.getBlobId(), sampleBlob.getBlobId(),
                fileBlob.getBlobId(), missingPkBlob.getBlobId());
        }
    }

    private byte[] readFile(String fileName) throws IOException {
        return IOUtils.toByteArray(getClass().getClassLoader().getResource(fileName));
    }

    private String gsPath(BlobInfo blob) {
        return "gs://" + blob.getBucket() + "/" + blob.getName();
    }
}
