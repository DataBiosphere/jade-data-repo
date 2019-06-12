package bio.terra.integration;

import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateStudyModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudyModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import static org.junit.Assert.assertTrue;

@Component
@Profile("integrationtest")
public class DataRepoFixtures {
    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private TestConfiguration testConfig;

    @Autowired
    private ObjectMapper objectMapper;


    // studies

    public DataRepoResponse<StudySummaryModel> createStudyRaw(String authToken, String filename) throws Exception {
        StudyRequestModel requestModel = jsonLoader.loadObject(filename, StudyRequestModel.class);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        String json = objectMapper.writeValueAsString(requestModel);

        return dataRepoClient.post(
            authToken,
            "/api/repository/v1/studies",
            json,
            StudySummaryModel.class);
    }

    public StudySummaryModel createStudy(String authToken, String filename) throws Exception {
        DataRepoResponse<StudySummaryModel> postResponse = createStudyRaw(authToken, filename);
        assertThat("study is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("study create response is present", postResponse.getResponseObject().isPresent());
        return postResponse.getResponseObject().get();
    }

    public void deleteTestStudy(String authToken, String studyId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoFixtures.deleteStudyRaw(authToken, studyId);
        assertGoodDeleteResponse(deleteResponse);
    }

    // Create a test dataset; expect successful creatio
    public DatasetSummaryModel createTestDataset(String authToken, StudySummaryModel studySummaryModel,
                                                               String filename) throws Exception {
        DataRepoResponse<DatasetSummaryModel> datasetResponse = dataRepoFixtures.createDataset(
            authToken, studySummaryModel, filename);
        assertThat("dataset create is successful", datasetResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataset create response is present", datasetResponse.getResponseObject().isPresent());
        return datasetResponse.getResponseObject().get();
    }

    public void deleteTestDataset(String authToken, String datasetId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = dataRepoFixtures.deleteDataset(authToken, datasetId);
        assertGoodDeleteResponse(deleteResponse);
    }



    public DataRepoResponse<DeleteResponseModel> deleteStudyRaw(String authToken, String studyId) throws Exception {
        return dataRepoClient.delete(
            authToken, "/api/repository/v1/studies/" + studyId, DeleteResponseModel.class);
    }

    public DataRepoResponse<EnumerateStudyModel> enumerateStudiesRaw(String authToken) throws Exception {
        return dataRepoClient.get(authToken, "api/repository/v1/studies", EnumerateStudyModel.class);
    }

    public DataRepoResponse<StudyModel> getStudyRaw(String authToken, String studyId) throws Exception {
        return dataRepoClient.get(authToken, "api/repository/v1/studies/" + studyId, StudyModel.class);
    }


    // datasets

    public DataRepoResponse<DatasetSummaryModel> createDataset(String authToken, StudySummaryModel studySummaryModel,
                                                               String filename) throws Exception {
        DatasetRequestModel requestModel = jsonLoader.loadObject(filename, DatasetRequestModel.class);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        requestModel.getContents().get(0).getSource().setStudyName(studySummaryModel.getName());
        String json = objectMapper.writeValueAsString(requestModel);

        DataRepoResponse<JobModel> jobResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/datasets",
            json,
            JobModel.class);

        assertTrue("dataset create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset create launch response is present", jobResponse.getResponseObject().isPresent());

        return dataRepoClient.waitForResponse(authToken, jobResponse, DatasetSummaryModel.class);
    }

    public DataRepoResponse<DeleteResponseModel> deleteDataset(String authToken, String datasetId) throws Exception {
        DataRepoResponse<JobModel> jobResponse =
            dataRepoClient.delete(authToken, "/api/repository/v1/datasets/" + datasetId, JobModel.class);

        assertTrue("dataset delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataset delete launch response is present", jobResponse.getResponseObject().isPresent());

        return dataRepoClient.waitForResponse(authToken, jobResponse, DeleteResponseModel.class);
    }
}
