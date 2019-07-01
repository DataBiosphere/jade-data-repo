package bio.terra.integration;

import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DataSnapshotRequestModel;
import bio.terra.model.DataSnapshotSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDrDatasetModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.DrDatasetModel;
import bio.terra.model.DrDatasetRequestModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
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


    // datasets

    public DataRepoResponse<DrDatasetSummaryModel> createDatasetRaw(
            String authToken,
            String filename) throws Exception {
        DrDatasetRequestModel requestModel = jsonLoader.loadObject(filename, DrDatasetRequestModel.class);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        String json = objectMapper.writeValueAsString(requestModel);

        return dataRepoClient.post(
            authToken,
            "/api/repository/v1/datasets",
            json,
            DrDatasetSummaryModel.class);
    }

    public DrDatasetSummaryModel createDataset(String authToken, String filename) throws Exception {
        DataRepoResponse<DrDatasetSummaryModel> postResponse = createDatasetRaw(authToken, filename);
        assertThat("dataset is successfully created", postResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataset create response is present", postResponse.getResponseObject().isPresent());
        return postResponse.getResponseObject().get();
    }

    public DataRepoResponse<DeleteResponseModel> deleteDatasetRaw(
            String authToken,
            String datasetId) throws Exception {
        return dataRepoClient.delete(
            authToken, "/api/repository/v1/datasets/" + datasetId, DeleteResponseModel.class);
    }

    public void deleteDataset(String authToken, String datasetId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDatasetRaw(authToken, datasetId);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<EnumerateDrDatasetModel> enumerateStudiesRaw(String authToken) throws Exception {
        return dataRepoClient.get(authToken,
            "/api/repository/v1/datasets?sort=created_date&direction=desc",
            EnumerateDrDatasetModel.class);
    }

    public EnumerateDrDatasetModel enumerateStudies(String authToken) throws Exception {
        DataRepoResponse<EnumerateDrDatasetModel> response = enumerateStudiesRaw(authToken);
        assertThat("dataset enumeration is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("dataset get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<DrDatasetModel> getDatasetRaw(String authToken, String datasetId) throws Exception {
        return dataRepoClient.get(authToken, "/api/repository/v1/datasets/" + datasetId, DrDatasetModel.class);
    }

    public DrDatasetModel getDataset(String authToken, String datasetId) throws Exception {
        DataRepoResponse<DrDatasetModel> response = getDatasetRaw(authToken, datasetId);
        assertThat("dataset is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("dataset get response is present", response.getResponseObject().isPresent());
        return response.getResponseObject().get();
    }

    public DataRepoResponse<Object> addDatasetPolicyMemberRaw(String authToken,
                                                              String datasetId,
                                                              SamClientService.DataRepoRole role,
                                                              String userEmail) throws Exception {
        PolicyMemberRequest req = new PolicyMemberRequest().email(userEmail);
        return dataRepoClient.post(authToken,
            "/api/repository/v1/datasets/" + datasetId + "/policies/" + role.toString() + "/members",
            objectMapper.writeValueAsString(req), null);
    }

    public void addDatasetPolicyMember(String authToken,
                                       String datasetId,
                                       SamClientService.DataRepoRole role,
                                       String userEmail) throws Exception {
        DataRepoResponse<Object> response = addDatasetPolicyMemberRaw(authToken, datasetId, role, userEmail);
        assertThat("dataset policy memeber is successfully added",
            response.getStatusCode(), equalTo(HttpStatus.OK));
    }

    // dataSnapshots

    public DataRepoResponse<DataSnapshotSummaryModel> createDataSnapshotRaw(
        String authToken, DrDatasetSummaryModel datasetSummaryModel, String filename) throws Exception {
        DataSnapshotRequestModel requestModel = jsonLoader.loadObject(filename, DataSnapshotRequestModel.class);
        requestModel.setName(Names.randomizeName(requestModel.getName()));
        requestModel.getContents().get(0).getSource().setDatasetName(datasetSummaryModel.getName());
        String json = objectMapper.writeValueAsString(requestModel);

        DataRepoResponse<JobModel> jobResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/datasnapshots",
            json,
            JobModel.class);

        assertTrue("dataSnapshot create launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataSnapshot create launch response is present",
            jobResponse.getResponseObject().isPresent());

        return dataRepoClient.waitForResponse(authToken, jobResponse, DataSnapshotSummaryModel.class);
    }

    public DataSnapshotSummaryModel createDataSnapshot(String authToken, DrDatasetSummaryModel datasetSummaryModel,
                                             String filename) throws Exception {
        DataRepoResponse<DataSnapshotSummaryModel> dataSnapshotResponse = createDataSnapshotRaw(
            authToken, datasetSummaryModel, filename);
        assertThat("dataSnapshot create is successful",
            dataSnapshotResponse.getStatusCode(), equalTo(HttpStatus.CREATED));
        assertTrue("dataSnapshot create response is present",
            dataSnapshotResponse.getResponseObject().isPresent());
        return dataSnapshotResponse.getResponseObject().get();
    }

    public void deleteDataSnapshot(String authToken, String dataSnapshotId) throws Exception {
        DataRepoResponse<DeleteResponseModel> deleteResponse = deleteDataSnapshotRaw(authToken, dataSnapshotId);
        assertGoodDeleteResponse(deleteResponse);
    }

    public DataRepoResponse<DeleteResponseModel> deleteDataSnapshotRaw(String authToken, String dataSnapshotId)
        throws Exception {
        DataRepoResponse<JobModel> jobResponse = dataRepoClient.delete(
            authToken, "/api/repository/v1/datasnapshots/" + dataSnapshotId, JobModel.class);

        assertTrue("dataSnapshot delete launch succeeded", jobResponse.getStatusCode().is2xxSuccessful());
        assertTrue("dataSnapshot delete launch response is present",
            jobResponse.getResponseObject().isPresent());

        return dataRepoClient.waitForResponse(authToken, jobResponse, DeleteResponseModel.class);
    }


    private void assertGoodDeleteResponse(DataRepoResponse<DeleteResponseModel> deleteResponse) {
        assertThat("delete is successful", deleteResponse.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("delete response is present", deleteResponse.getResponseObject().isPresent());
        DeleteResponseModel deleteModel = deleteResponse.getResponseObject().get();
        assertTrue("Valid delete response", (
            deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                deleteModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    // Create a test dataset; expect successful creation

    /**
     * Ingests JSON data taking the defaults for the ingest specification
     *
     * @param datasetId   - id of dataset to load
     * @param tableName - name of table to load data into
     * @param datafile  - file path within the bucket from property integrationtest.ingestbucket
     * @return ingest response
     * @throws Exception
     */
    public DataRepoResponse<IngestResponseModel> ingestJsonDataRaw(
        String authToken, String datasetId, String tableName, String datafile) throws Exception {
        String ingestBody = buildSimpleIngest(tableName, datafile);
        DataRepoResponse<JobModel> postResponse = dataRepoClient.post(
            authToken,
            "/api/repository/v1/datasets/" + datasetId + "/ingest",
            ingestBody,
            JobModel.class);
        assertTrue("ingest launch succeeded", postResponse.getStatusCode().is2xxSuccessful());
        assertTrue("ingest launch response is present", postResponse.getResponseObject().isPresent());

        return dataRepoClient.waitForResponse(authToken, postResponse, IngestResponseModel.class);
    }

    public IngestResponseModel ingestJsonData(String authToken, String datasetId, String tableName, String datafile)
        throws Exception {
        DataRepoResponse<IngestResponseModel> response = ingestJsonDataRaw(authToken, datasetId, tableName, datafile);
        assertThat("ingestOne is successful", response.getStatusCode(), equalTo(HttpStatus.OK));
        assertTrue("ingestOne response is present", response.getResponseObject().isPresent());

        IngestResponseModel ingestResponse = response.getResponseObject().get();
        assertThat("no bad sample rows", ingestResponse.getBadRowCount(), equalTo(0L));
        return ingestResponse;
    }

    private String buildSimpleIngest(String table, String filename) {
        // TODO: Change this to create the IngestRequestModel and convert it to JSON
        StringBuilder ingestBuilder = new StringBuilder()
            .append("{").append('"').append("table").append('"').append(':').append('"').append(table).append('"')
            .append(", ").append('"').append("format").append('"').append(':').append('"').append("json").append('"')
            .append(", ").append('"').append("path").append('"').append(':').append('"')
            .append("gs://").append(testConfig.getIngestbucket()).append("/").append(filename)
            .append('"').append('}');

        return ingestBuilder.toString();
    }

}
