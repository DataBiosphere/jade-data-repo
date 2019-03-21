package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.StudyModel;
import bio.terra.model.StudySummaryModel;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(Integration.class)
public class StudyTest {
    private static final String omopStudyName = "it_study_omop";
    private static final String omopStudyDesc =
        "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki";

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Test
    public void studyHappyPath() throws Exception {
        String studyJson = jsonLoader.loadJson("it-study-omop.json");
        DataRepoResponse<StudySummaryModel> postResponse = dataRepoClient.post(
            "/api/repository/v1/studies",
            studyJson,
            StudySummaryModel.class);

        assertThat("study is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("study create response is present", postResponse.getResponseObject().isPresent());
        StudySummaryModel summaryModel = postResponse.getResponseObject().get();
        assertThat(summaryModel.getName(), equalTo(omopStudyName));
        assertThat(summaryModel.getDescription(), equalTo(omopStudyDesc));

        String studyPath = "/api/repository/v1/studies/" + summaryModel.getId();
        DataRepoResponse<StudyModel> getResponse = dataRepoClient.get(studyPath, StudyModel.class);
        postResponse = null;

        assertThat("study is successfully retrieved", getResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", getResponse.getResponseObject().isPresent());
        StudyModel studyModel = getResponse.getResponseObject().get();
        assertThat(studyModel.getName(), equalTo(omopStudyName));
        assertThat(studyModel.getDescription(), equalTo(omopStudyDesc));
        getResponse = null;

        DataRepoResponse<StudySummaryModel[]> enumResponse = dataRepoClient.get(
            "/api/repository/v1/studies?offset=0&items=1000",
            StudySummaryModel[].class);

        assertThat("study enumeration is successful", enumResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study get response is present", enumResponse.getResponseObject().isPresent());
        StudySummaryModel[] summaryArray = enumResponse.getResponseObject().get();
        boolean found = false;
        for (StudySummaryModel oneStudy : summaryArray) {
            if (oneStudy.getId().equals(studyModel.getId())) {
                assertThat(oneStudy.getName(), equalTo(omopStudyName));
                assertThat(oneStudy.getDescription(), equalTo(omopStudyDesc));
                found = true;
                break;
            }
        }
        assertTrue("study was found in enumeration", found);
        enumResponse = null;

        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoClient.delete(studyPath);
        assertThat("study delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("study delete response is present", deleteResponse.getResponseObject().isPresent());
        DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
        assertThat(deleteModel.getObjectState(), equalTo(DeleteResponseModel.ObjectStateEnum.DELETED));
    }

}
