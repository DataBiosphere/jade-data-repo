package bio.terra.pdao.bigquery;

import bio.terra.category.Connected;
import bio.terra.configuration.ConnectedTestConfiguration;
import bio.terra.dao.StudyDao;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.metadata.Study;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.StudyJsonConversion;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

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

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private ConnectedTestConfiguration testConfig;
    @Autowired private Storage storage;
    @Autowired private BigQueryPdao bigQueryPdao;
    @Autowired private StudyDao studyDao;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;

    @MockBean
    private SamClientService samService;

    private ConnectedOperations connectedOperations;
    private Study study;
    private BillingProfileModel profileModel;

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        ConnectedOperations.stubOutSamCalls(samService);
        connectedOperations = new ConnectedOperations(mvc, objectMapper, jsonLoader);

        String coreBillingAccount = googleResourceConfiguration.getCoreBillingAccount();
        profileModel = connectedOperations.getOrCreateProfileForAccount(coreBillingAccount);
        // TODO: this next bit should be in connected operations, need to make it a component and autowire a studydao
        StudyRequestModel studyRequest = jsonLoader.loadObject("ingest-test-study.json",
            StudyRequestModel.class);
        studyRequest
            .defaultProfileId(profileModel.getId())
            .name(studyName());
        study = StudyJsonConversion.studyRequestToStudy(studyRequest);
        UUID studyId = studyDao.create(study);
        study.id(studyId);
        connectedOperations.addStudy(studyId.toString());
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    private String studyName() {
        return "pdaotest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
    }

    @Test
    public void basicTest() throws Exception {
        boolean exists = bigQueryPdao.studyExists(study);
        Assert.assertThat(exists, is(equalTo(false)));

        bigQueryPdao.createStudy(study);

        exists = bigQueryPdao.studyExists(study);
        Assert.assertThat(exists, is(equalTo(true)));

        // Perform the redo, which should delete and re-create
        bigQueryPdao.createStudy(study);
        exists = bigQueryPdao.studyExists(study);
        Assert.assertThat(exists, is(equalTo(true)));


        // Now delete it and test that it is gone
        bigQueryPdao.deleteStudy(study);
        exists = bigQueryPdao.studyExists(study);
        Assert.assertThat(exists, is(equalTo(false)));
    }

    @Test
    public void datasetTest() throws Exception {
        bigQueryPdao.createStudy(study);

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
            storage.create(participantBlob, readFile("ingest-test-participant.json"));
            storage.create(sampleBlob, readFile("ingest-test-sample.json"));
            storage.create(fileBlob, readFile("ingest-test-file.json"));

            // Ingest staged data into the new study.
            IngestRequestModel ingestRequest = new IngestRequestModel()
                .format(IngestRequestModel.FormatEnum.JSON);

            String studyId = study.getId().toString();
            connectedOperations.ingestTableSuccess(studyId,
                ingestRequest.table("participant").path(gsPath(participantBlob)));
            connectedOperations.ingestTableSuccess(studyId,
                ingestRequest.table("sample").path(gsPath(sampleBlob)));
            connectedOperations.ingestTableSuccess(studyId,
                ingestRequest.table("file").path(gsPath(fileBlob)));

            // Create a dataset!
            StudySummaryModel studySummary =
                StudyJsonConversion.studySummaryModelFromStudySummary(study.getStudySummary());
            MockHttpServletResponse datasetResponse =
                connectedOperations.launchCreateDataset(studySummary, "ingest-test-dataset.json", "");
            DatasetSummaryModel datasetSummary = connectedOperations.handleCreateDatasetSuccessCase(datasetResponse);
            DatasetModel dataset = connectedOperations.getDataset(datasetSummary.getId());

            // TODO: Assert that the dataset contains the rows we expect.
            // Skipping that for now because there's no REST API to query table contents.
            Assert.assertThat(dataset.getTables().size(), is(equalTo(3)));
        } finally {
            storage.delete(participantBlob.getBlobId(), sampleBlob.getBlobId(), fileBlob.getBlobId());
        }
    }

    private byte[] readFile(String fileName) throws IOException {
        return IOUtils.toByteArray(getClass().getClassLoader().getResource(fileName));
    }

    private String gsPath(BlobInfo blob) {
        return "gs://" + blob.getBucket() + "/" + blob.getName();
    }
}
