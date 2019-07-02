package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.auth.Users;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.*;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.pdao.bigquery.BigQueryProject;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.service.SamClientService;
import com.google.j2objc.annotations.AutoreleasePool;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class AccessTest {
    private static final String omopStudyName = "it_study_omop";
    private static final String omopStudyDesc =
        "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki";
    private static Logger logger = LoggerFactory.getLogger(StudyTest.class);

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Autowired
    private Users users;

    @Autowired
    private AuthService authService;

    @Autowired
    private SamClientService samClientService;

    @Autowired
    private BigQueryPdao bigQueryPdao;

    private TestConfiguration.User steward;
    private TestConfiguration.User custodian;
    private TestConfiguration.User reader;
    private String stewardToken;
    private String custodianToken;
    private String readerToken;
    private StudySummaryModel studySummaryModel;
    private String studyId;



    @Before
    public void setup() throws Exception {
        steward = users.getUserForRole("steward");
        custodian = users.getUser("harry");
        reader = users.getUserForRole("reader");
        stewardToken = authService.getAuthToken(steward.getEmail());
        custodianToken = authService.getAuthToken(custodian.getEmail());
        readerToken = authService.getAuthToken(reader.getEmail());

        studySummaryModel = dataRepoFixtures.createStudy(stewardToken, "ingest-test-study.json");
        studyId = studySummaryModel.getId();
    }


    @Test
    public void checkShared() throws  Exception{
        IngestResponseModel ingestResponse =
            dataRepoFixtures.ingestJsonData(
                stewardToken, studyId, "participant", "ingest-test/ingest-test-participant.json");
        assertThat("correct participant row count", ingestResponse.getRowCount(), equalTo(2L));

        ingestResponse = dataRepoFixtures.ingestJsonData(
            stewardToken, studyId, "sample", "ingest-test/ingest-test-sample.json");
        assertThat("correct sample row count", ingestResponse.getRowCount(), equalTo(5L));

        dataRepoFixtures.addStudyPolicyMember(
            stewardToken,
            studyId,
            SamClientService.DataRepoRole.CUSTODIAN,
            custodian.getEmail());
        DataRepoResponse<EnumerateStudyModel> enumStudies = dataRepoFixtures.enumerateStudiesRaw(custodianToken);
        assertThat("Custodian is authorized to enumerate studies",
            enumStudies.getStatusCode(),
            equalTo(HttpStatus.OK));

        DatasetSummaryModel datasetSummaryModel = dataRepoFixtures.createDataset(custodianToken, studySummaryModel, "ingest-test-dataset.json");

        dataRepoFixtures.addDatasetPolicyMember(
            custodianToken,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader.getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest = new AuthenticatedUserRequest(reader.getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASET,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));

        bigQueryPdao.

    }
}
