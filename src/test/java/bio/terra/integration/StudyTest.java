package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.auth.Users;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.StudyModel;
import bio.terra.model.StudySummaryModel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class StudyTest {
    private static final String omopStudyName = "it_study_omop";
    private static final String omopStudyDesc =
        "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki";

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


    private TestConfiguration.User steward;
    private TestConfiguration.User custodian;
    private TestConfiguration.User reader;
    private String stewardToken;
    private String custodianToken;
    private String readerToken;


    @Before
    public void setup() throws Exception {
        steward = users.getUserForRole("steward");
        custodian = users.getUserForRole("custodian");
        reader = users.getUserForRole("reader");
        stewardToken = authService.getAuthToken(steward.getEmail());
        custodianToken = authService.getAuthToken(custodian.getEmail());
        readerToken = authService.getAuthToken(reader.getEmail());
        System.out.println("steward: " + steward.getName() + "; custodian: " + custodian.getName() +
            "; reader: " + reader.getName());
    }

    @Test
    public void studyHappyPath() throws Exception {
        StudySummaryModel summaryModel = dataRepoFixtures.createStudy(stewardToken, "it-study-omop.json");
        try {
            assertThat(summaryModel.getName(), startsWith(omopStudyName));
            assertThat(summaryModel.getDescription(), equalTo(omopStudyDesc));

            StudyModel studyModel = dataRepoFixtures.getStudy(stewardToken, summaryModel.getId());

            assertThat(studyModel.getName(), startsWith(omopStudyName));
            assertThat(studyModel.getDescription(), equalTo(omopStudyDesc));

            EnumerateStudyModel enumerateStudyModel = dataRepoFixtures.enumerateStudies(stewardToken);
            boolean found = false;
            for (StudySummaryModel oneStudy : enumerateStudyModel.getItems()) {
                if (oneStudy.getId().equals(studyModel.getId())) {
                    assertThat(oneStudy.getName(), startsWith(omopStudyName));
                    assertThat(oneStudy.getDescription(), equalTo(omopStudyDesc));
                    found = true;
                    break;
                }
            }
            assertTrue("study was found in enumeration", found);

            // test allowable permissions

            dataRepoFixtures.addStudyPolicyMember(summaryModel.getId(), custodian.getEmail());
            DataRepoResponse<EnumerateStudyModel> enumStudies = dataRepoFixtures.enumerateStudiesRaw(custodianToken);
            assertThat("Custodian is authorized to enumerate studies",
                enumStudies.getStatusCode().value(),
                equalTo(HttpStatus.OK));

        } finally {
            dataRepoFixtures.deleteStudyRaw(stewardToken, summaryModel.getId());
        }
    }

    @Rule
    public ExpectedException exceptionGrabber = ExpectedException.none();

    @Test
    public void studyUnauthorizedPermissionsTest() throws Exception {

        DataRepoResponse<StudySummaryModel> studySumRespCust = dataRepoFixtures.createStudyRaw(
            custodianToken, "study-minimal.json");
        assertThat("Custodian is not authorized to create a study",
            studySumRespCust.getStatusCode().value(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DataRepoResponse<StudySummaryModel> studySumRespReader = dataRepoFixtures.createStudyRaw(
            readerToken, "study-minimal.json");
        assertThat("Reader is not authorized to create a study",
            studySumRespReader.getStatusCode().value(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DataRepoResponse<EnumerateStudyModel> enumStudiesResp = dataRepoFixtures.enumerateStudiesRaw(readerToken);
        assertThat("Reader is not authorized to enumerate studies",
            enumStudiesResp.getStatusCode().value(),
            equalTo(HttpStatus.UNAUTHORIZED));

        StudySummaryModel summaryModel = null;
        try {
            summaryModel = dataRepoFixtures.createStudy(stewardToken, "study-minimal.json");

            DataRepoResponse<StudyModel> getStudyResp = dataRepoFixtures.getStudyRaw(readerToken, summaryModel.getId());
            assertThat("Reader is not authorized to get study",
                getStudyResp.getStatusCode().value(),
                equalTo(HttpStatus.UNAUTHORIZED));

            DataRepoResponse<DeleteResponseModel> deleteResp1 = dataRepoFixtures.deleteStudyRaw(readerToken, summaryModel.getId());
            assertThat("Reader is not authorized to delete studies",
                deleteResp1.getStatusCode().value(),
                equalTo(HttpStatus.UNAUTHORIZED));

            DataRepoResponse<DeleteResponseModel> deleteResp2 = dataRepoFixtures.deleteStudyRaw(custodianToken, summaryModel.getId());
            assertThat("Custodian is not authorized to delete studies",
                deleteResp2.getStatusCode().value(),
                equalTo(HttpStatus.UNAUTHORIZED));
        } finally {
            dataRepoFixtures.deleteStudy(stewardToken, summaryModel.getId());
        }
    }

}
