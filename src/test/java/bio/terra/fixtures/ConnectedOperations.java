package bio.terra.fixtures;

import bio.terra.model.DRSChecksum;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestResponseModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.service.SamClientService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.hamcrest.CoreMatchers;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// Common code for creating and deleting studies and datasets via MockMvc
// and tracking what is created so it can be deleted.
public class ConnectedOperations {
    private MockMvc mvc;
    private ObjectMapper objectMapper;
    private JsonLoader jsonLoader;

    private boolean deleteOnTeardown;
    private List<String> createdDatasetIds;
    private List<String> createdStudyIds;
    private List<String[]> createdFileIds; // [0] is studyid, [1] is fileid


    public static void stubOutSamCalls(SamClientService samService) throws ApiException {
        when(samService.createDatasetResource(any(), any(), any())).thenReturn("hi@hi.com");
        doNothing().when(samService).createStudyResource(any(), any());
        doNothing().when(samService).deleteDatasetResource(any(), any());
        doNothing().when(samService).deleteStudyResource(any(), any());
    }

    public ConnectedOperations(MockMvc mvc,
                               ObjectMapper objectMapper,
                               JsonLoader jsonLoader) {
        this.mvc = mvc;
        this.objectMapper = objectMapper;
        this.jsonLoader = jsonLoader;

        createdDatasetIds = new ArrayList<>();
        createdStudyIds = new ArrayList<>();
        createdFileIds = new ArrayList<>();
        deleteOnTeardown = true;

    }

    public StudySummaryModel createTestStudy(String resourcePath) throws Exception {
        StudyRequestModel studyRequest = jsonLoader.loadObject(resourcePath, StudyRequestModel.class);
        studyRequest.setName(Names.randomizeName(studyRequest.getName()));

        MvcResult result = mvc.perform(post("/api/repository/v1/studies")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(studyRequest)))
            .andExpect(status().isCreated())
            .andReturn();

        MockHttpServletResponse response = result.getResponse();
        StudySummaryModel studySummaryModel =
            objectMapper.readValue(response.getContentAsString(), StudySummaryModel.class);

        addStudy(studySummaryModel.getId());

        return studySummaryModel;
    }

    public MockHttpServletResponse launchCreateDataset(StudySummaryModel studySummaryModel,
                                                        String resourcePath,
                                                        String infix) throws Exception {

        DatasetRequestModel datasetRequest = jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        datasetRequest.getContents().get(0).getSource().setStudyName(studySummaryModel.getName());

        String datasetName = Names.randomizeNameInfix(datasetRequest.getName(), infix);
        datasetRequest.setName(datasetName);

        String jsonRequest = objectMapper.writeValueAsString(datasetRequest);

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();

        return validateJobModelAndWait(result);
    }

    public DatasetModel getDataset(String datasetId) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets/" + datasetId)).andReturn();
        MockHttpServletResponse response = result.getResponse();
        return objectMapper.readValue(response.getContentAsString(), DatasetModel.class);
    }

    public DatasetSummaryModel handleCreateDatasetSuccessCase(MockHttpServletResponse response) throws Exception {
        DatasetSummaryModel summaryModel = handleAsyncSuccessCase(response, DatasetSummaryModel.class);
        addDataset(summaryModel.getId());
        return summaryModel;
    }

    public <T> T handleAsyncSuccessCase(MockHttpServletResponse response, Class<T> returnClass) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        if (!responseStatus.is2xxSuccessful()) {
            String failMessage = "Request for " + returnClass.getName() +
                " failed: status=" + responseStatus.toString();
            if (StringUtils.contains(responseBody, "message")) {
                // If the responseBody contains the word 'message', then we try to decode it as an ErrorModel
                // so we can generate good failure information.
                ErrorModel errorModel = objectMapper.readValue(responseBody, ErrorModel.class);
                failMessage += " msg=" + errorModel.getMessage();
            } else {
                failMessage += " responseBody=" + responseBody;
            }
            fail(failMessage);
        }

        return objectMapper.readValue(responseBody, returnClass);
    }

    public ErrorModel handleAsyncFailureCase(MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        assertFalse("Expect failure", responseStatus.is2xxSuccessful());

        assertTrue("Error model was returned on failure",
            StringUtils.contains(responseBody, "message"));

        return objectMapper.readValue(responseBody, ErrorModel.class);
    }

    public void deleteTestStudy(String id) throws Exception {
        // We only use this for @After, so we don't check return values
        MvcResult result = mvc.perform(delete("/api/repository/v1/studies/" + id)).andReturn();
        checkDeleteResponse(result.getResponse());
    }

    public void deleteTestDataset(String id) throws Exception {
        MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + id)).andReturn();
        MockHttpServletResponse response = validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
        checkDeleteResponse(response);
    }

    public void deleteTestFile(String studyId, String fileId) throws Exception {
        MvcResult result = mvc.perform(
            delete("/api/repository/v1/studies/" + studyId + "/files/" + fileId))
                .andReturn();
        MockHttpServletResponse response = validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
        checkDeleteResponse(response);
    }

    private void checkDeleteResponse(MockHttpServletResponse response) throws Exception {
        DeleteResponseModel responseModel =
            objectMapper.readValue(response.getContentAsString(), DeleteResponseModel.class);
        assertTrue("Valid delete response object state enumeration",
            (responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    public IngestResponseModel ingestTableSuccess(
        String studyId,
        IngestRequestModel ingestRequestModel) throws Exception {

        String jsonRequest = objectMapper.writeValueAsString(ingestRequestModel);
        String url = "/api/repository/v1/studies/" + studyId + "/ingest";

        MvcResult result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();
        MockHttpServletResponse response = validateJobModelAndWait(result);

        IngestResponseModel ingestResponse =
            handleAsyncSuccessCase(response, IngestResponseModel.class);
        assertThat("ingest response has no bad rows", ingestResponse.getBadRowCount(), equalTo(0L));

        return ingestResponse;
    }

    public FileModel ingestFileSuccess(String studyId, FileLoadModel fileLoadModel) throws Exception {
        String jsonRequest = objectMapper.writeValueAsString(fileLoadModel);
        String url = "/api/repository/v1/studies/" + studyId + "/files";
        MvcResult result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();

        MockHttpServletResponse response = validateJobModelAndWait(result);

        FileModel fileModel = handleAsyncSuccessCase(response, FileModel.class);
        assertThat("description matches", fileModel.getDescription(),
            CoreMatchers.equalTo(fileLoadModel.getDescription()));
        assertThat("mime type matches", fileModel.getMimeType(),
            CoreMatchers.equalTo(fileLoadModel.getMimeType()));

        for (DRSChecksum checksum : fileModel.getChecksums()) {
            assertTrue("valid checksum type",
                (StringUtils.equals(checksum.getType(), "crc32c") ||
                    StringUtils.equals(checksum.getType(), "md5")));
        }

        addFile(studyId, fileModel.getFileId());

        return fileModel;
    }

    public ErrorModel ingestFileFailure(String studyId, FileLoadModel fileLoadModel) throws Exception {
        String jsonRequest = objectMapper.writeValueAsString(fileLoadModel);
        String url = "/api/repository/v1/studies/" + studyId + "/files";
        MvcResult result = mvc.perform(post(url)
            .contentType(MediaType.APPLICATION_JSON)
            .content(jsonRequest))
            .andReturn();

        MockHttpServletResponse response = validateJobModelAndWait(result);

        return handleAsyncFailureCase(response);
    }

    public MockHttpServletResponse validateJobModelAndWait(MvcResult inResult) throws Exception {
        MvcResult result = inResult;
        while (true) {
            MockHttpServletResponse response = result.getResponse();
            HttpStatus status = HttpStatus.valueOf(response.getStatus());
            assertTrue("received expected jobs polling status",
                (status == HttpStatus.ACCEPTED || status == HttpStatus.OK));

            JobModel jobModel = objectMapper.readValue(response.getContentAsString(), JobModel.class);
            String jobId = jobModel.getId();
            String locationUrl = response.getHeader("Location");
            assertNotNull("location URL was specified", locationUrl);

            switch (status) {
                case ACCEPTED:
                    // Not done case: sleep and probe using the header URL
                    assertThat("location header for probe", locationUrl,
                        equalTo(String.format("/api/repository/v1/jobs/%s", jobId)));

                    TimeUnit.SECONDS.sleep(1);
                    result = mvc.perform(get(locationUrl).accept(MediaType.APPLICATION_JSON)).andReturn();
                    break;

                case OK:
                    // Done case: get the result with the header URL and return the response;
                    // let the caller interpret the response
                    assertThat("location header for result", locationUrl,
                        equalTo(String.format("/api/repository/v1/jobs/%s/result", jobId)));
                    result = mvc.perform(get(locationUrl).accept(MediaType.APPLICATION_JSON)).andReturn();
                    return result.getResponse();

                default:
                    fail("invalid response status");
            }
        }
    }

    // -- tracking methods --

    public void addStudy(String id) {
        createdStudyIds.add(id);
    }

    public void addDataset(String id) {
        createdDatasetIds.add(id);
    }

    public void addFile(String studyId, String fileId) {
        String[] createdFile = new String[]{studyId, fileId};
        createdFileIds.add(createdFile);
    }

    public void setDeleteOnTeardown(boolean deleteOnTeardown) {
        this.deleteOnTeardown = deleteOnTeardown;
    }

    public void teardown() throws Exception {
        if (deleteOnTeardown) {
            // Order is important: delete all the datasets first so we eliminate dependencies
            // Then delete the files before the studies
            for (String datasetId : createdDatasetIds) {
                deleteTestDataset(datasetId);
            }

            for (String[] fileInfo : createdFileIds) {
                deleteTestFile(fileInfo[0], fileInfo[1]);
            }

            for (String studyId : createdStudyIds) {
                deleteTestStudy(studyId);
            }
        }
    }

}
