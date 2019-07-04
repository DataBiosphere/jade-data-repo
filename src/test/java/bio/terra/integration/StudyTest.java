package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.StudyModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class StudyTest extends UsersBase {
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

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void studyHappyPath() throws Exception {
        StudySummaryModel summaryModel = dataRepoFixtures.createStudy(steward(), "it-study-omop.json");
        try {
            logger.info("study id is " + summaryModel.getId());
            assertThat(summaryModel.getName(), startsWith(omopStudyName));
            assertThat(summaryModel.getDescription(), equalTo(omopStudyDesc));

            StudyModel studyModel = dataRepoFixtures.getStudy(steward(), summaryModel.getId());

            assertThat(studyModel.getName(), startsWith(omopStudyName));
            assertThat(studyModel.getDescription(), equalTo(omopStudyDesc));

            // There is a delay from when a resource is created in SAM to when it is available in an enumerate call.
            TimeUnit.SECONDS.sleep(5);

            EnumerateStudyModel enumerateStudyModel = dataRepoFixtures.enumerateStudies(steward());
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

            dataRepoFixtures.addStudyPolicyMember(
                steward(),
                summaryModel.getId(),
                SamClientService.DataRepoRole.CUSTODIAN,
                custodian().getEmail());
            DataRepoResponse<EnumerateStudyModel> enumStudies = dataRepoFixtures.enumerateStudiesRaw(custodian());
            assertThat("Custodian is authorized to enumerate studies",
                enumStudies.getStatusCode(),
                equalTo(HttpStatus.OK));

        } finally {
            logger.info("deleting study");
            dataRepoFixtures.deleteStudyRaw(steward(), summaryModel.getId());
        }
    }

    @Test
    public void studyUnauthorizedPermissionsTest() throws Exception {

        DataRepoResponse<StudySummaryModel> studySumRespCust = dataRepoFixtures.createStudyRaw(
            custodian(), "study-minimal.json");
        assertThat("Custodian is not authorized to create a study",
            studySumRespCust.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DataRepoResponse<StudySummaryModel> studySumRespReader = dataRepoFixtures.createStudyRaw(
            reader(), "study-minimal.json");
        assertThat("Reader is not authorized to create a study",
            studySumRespReader.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        EnumerateStudyModel enumStudiesResp = dataRepoFixtures.enumerateStudies(reader());
        assertThat("Reader does not have access to studies",
            enumStudiesResp.getTotal(),
            equalTo(0));

        StudySummaryModel summaryModel = null;
        try {
            summaryModel = dataRepoFixtures.createStudy(steward(), "study-minimal.json");
            logger.info("study id is " + summaryModel.getId());

            DataRepoResponse<StudyModel> getStudyResp = dataRepoFixtures.getStudyRaw(reader(), summaryModel.getId());
            assertThat("Reader is not authorized to get study",
                getStudyResp.getStatusCode(),
                equalTo(HttpStatus.UNAUTHORIZED));

            DataRepoResponse<DeleteResponseModel> deleteResp1 = dataRepoFixtures.deleteStudyRaw(
                reader(), summaryModel.getId());
            assertThat("Reader is not authorized to delete studies",
                deleteResp1.getStatusCode(),
                equalTo(HttpStatus.UNAUTHORIZED));

            DataRepoResponse<DeleteResponseModel> deleteResp2 = dataRepoFixtures.deleteStudyRaw(
                custodian(), summaryModel.getId());
            assertThat("Custodian is not authorized to delete studies",
                deleteResp2.getStatusCode(),
                equalTo(HttpStatus.UNAUTHORIZED));
        } finally {
            if (summaryModel != null)
                dataRepoFixtures.deleteStudy(steward(), summaryModel.getId());
        }
    }

}
