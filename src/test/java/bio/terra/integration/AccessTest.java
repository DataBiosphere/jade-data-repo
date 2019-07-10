package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.auth.Users;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.pdao.bigquery.BigQueryConfiguration;
import bio.terra.pdao.bigquery.BigQueryProject;
import bio.terra.pdao.exception.PdaoException;
import bio.terra.service.SamClientService;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
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

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
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
    private BigQueryConfiguration bigQueryConfiguration;

    private TestConfiguration.User steward;
    private TestConfiguration.User custodian;
    private TestConfiguration.User reader;
    private String readerToken;
    private StudySummaryModel studySummaryModel;
    private String studyId;



    @Before
    public void setup() throws Exception {
        steward = users.getUserForRole("steward");
        custodian = users.getUser("harry");
        reader = users.getUserForRole("reader");
        readerToken = authService.getDirectAccessAuthToken(reader.getEmail());

        studySummaryModel = dataRepoFixtures.createStudy(steward, "ingest-test-study.json");
        studyId = studySummaryModel.getId();
    }

    private BigQueryProject getBigQueryProject(String token){
        String projectId = bigQueryConfiguration.googleProjectId();
        GoogleCredentials googleCredentials = GoogleCredentials.create(new AccessToken(token, null));
        BigQueryProject bigQueryProject = new BigQueryProject(projectId, googleCredentials);

        return bigQueryProject;
    }

    @Test
    public void checkShared() throws  Exception {
        BigQueryProject bigQueryProject = getBigQueryProject(readerToken);

        IngestResponseModel ingestResponse =
            dataRepoFixtures.ingestJsonData(
                steward, studyId, "participant", "ingest-test/ingest-test-participant.json");

        ingestResponse = dataRepoFixtures.ingestJsonData(
            steward, studyId, "sample", "ingest-test/ingest-test-sample.json");

        dataRepoFixtures.addStudyPolicyMember(
            steward,
            studyId,
            SamClientService.DataRepoRole.CUSTODIAN,
            custodian.getEmail());
        DataRepoResponse<EnumerateStudyModel> enumStudies = dataRepoFixtures.enumerateStudiesRaw(custodian);
        assertThat("Custodian is authorized to enumerate studies",
            enumStudies.getStatusCode(),
            equalTo(HttpStatus.OK));

        DatasetSummaryModel datasetSummaryModel =
            dataRepoFixtures.createDataset(custodian, studySummaryModel, "ingest-test-dataset.json");

       try {
           bigQueryProject.datasetExists(datasetSummaryModel.getName());
           assertThat("reader can access the dataset after it has been shared",
               bigQueryProject.datasetExists(datasetSummaryModel.getName()),
               not(true));
       } catch (PdaoException e) {
          assertThat(
              "checking message for pdao exception error",
              e.getMessage(),
              equalTo("existence check failed for ".concat(datasetSummaryModel.getName())));
       }

        dataRepoFixtures.addDatasetPolicyMember(
            custodian,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoRole.READER,
            reader.getEmail());

        AuthenticatedUserRequest authenticatedReaderRequest =
            new AuthenticatedUserRequest(reader.getEmail(), readerToken);
        assertThat("correctly added reader", samClientService.isAuthorized(
            authenticatedReaderRequest,
            SamClientService.ResourceType.DATASET,
            datasetSummaryModel.getId(),
            SamClientService.DataRepoAction.READ_DATA), equalTo(true));


        long startTime = System.currentTimeMillis();
        boolean hasAccess = false;
        while (hasAccess == false && (System.currentTimeMillis()-startTime)<300000){
            TimeUnit.SECONDS.sleep(5);
            try {
                boolean datasetExists = bigQueryProject.datasetExists(datasetSummaryModel.getName());
                hasAccess = true;
                assertThat("Dataset wasn't created right", datasetExists, equalTo(true));
            } catch (PdaoException e) {
                assertThat(
                    "checking message for pdao exception error",
                    e.getCause().getMessage(),
                    startsWith("Access Denied:"));
            }
        }

        assertThat("reader can access the dataset after it has been shared",
            hasAccess,
            equalTo(true));

    }


}
