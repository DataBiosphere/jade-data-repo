package bio.terra.pdao.bigquery;

import bio.terra.category.Connected;
import bio.terra.configuration.ConnectedTestConfiguration;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.metadata.Column;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyDataProject;
import bio.terra.metadata.Table;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.StudySummaryModel;
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

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private ConnectedTestConfiguration testConfig;
    @Autowired private Storage storage;
    @Autowired private BigQueryPdao bigQueryPdao;

    @MockBean
    private SamClientService samService;

    private ConnectedOperations connectedOperations;

    @Before
    public void setup() throws Exception {
        // Setup mock sam service
        ConnectedOperations.stubOutSamCalls(samService);
        connectedOperations = new ConnectedOperations(mvc, objectMapper, jsonLoader);
        StudyDataProject studyDataProject = dataProjectService.getProjectForStudy(study);
        BigQueryProject bigQueryProject = new BigQueryProject(studyDataProject.getGoogleProjectId());
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    public void basicTest() throws Exception {
        // Contrive a study object with a unique name
        String studyName = "pdaotest" + StringUtils.remove(UUID.randomUUID().toString(), '-');
        Study study = makeStudy(studyName);

        boolean exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(false)));

        bigQueryPdao.createStudy(study);

        exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(true)));

        // Perform the redo, which should delete and re-create
        bigQueryPdao.createStudy(study);
        exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(true)));


        // Now delete it and test that it is gone
        bigQueryPdao.deleteStudy(study);
        exists = bigQueryPdao.studyExists(studyName);
        Assert.assertThat(exists, is(equalTo(false)));
    }

    @Test
    public void datasetTest() throws Exception {
        // Create a random study.
        StudySummaryModel studySummary = connectedOperations.createTestStudy("ingest-test-study.json");

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

            connectedOperations.ingestTableSuccess(studySummary.getId(),
                ingestRequest.table("participant").path(gsPath(participantBlob)));
            connectedOperations.ingestTableSuccess(studySummary.getId(),
                ingestRequest.table("sample").path(gsPath(sampleBlob)));
            connectedOperations.ingestTableSuccess(studySummary.getId(),
                ingestRequest.table("file").path(gsPath(fileBlob)));

            // Create a dataset!
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

    private Study makeStudy(String studyName) {
        Column col1 = new Column().name("col1").type("string");
        Column col2 = new Column().name("col2").type("string");
        Column col3 = new Column().name("col3").type("string");
        Column col4 = new Column().name("col4").type("string");

        List<Column> table1Columns = new ArrayList<>();
        table1Columns.add(col1);
        table1Columns.add(col2);
        Table table1 = new Table().name("table1").columns(table1Columns);

        List<Column> table2Columns = new ArrayList<>();
        table2Columns.add(col4);
        table2Columns.add(col3);
        table2Columns.add(col2);
        table2Columns.add(col1);
        Table table2 = new Table().name("table2").columns(table2Columns);

        List<Table> tables = new ArrayList<>();
        tables.add(table1);
        tables.add(table2);

        Study study = new Study();
        study.name(studyName)
                .description("this is a test study");
        study.tables(tables);
        return study;
    }

}
