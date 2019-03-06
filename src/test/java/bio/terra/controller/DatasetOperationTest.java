package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSourceModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static bio.terra.pdao.PdaoConstant.PDAO_PREFIX;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Connected.class)
public class DatasetOperationTest {

    // TODO: MORE TESTS to be done when we can ingest data:
    // - test more complex studies with relationships
    // - test relationship walking with valid and invalid setups
    // TODO: MORE TESTS when we separate the value translation from the create
    // - test invalid row ids

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private BigQuery bigQuery;
    @Autowired private String projectId;

    private ClassLoader classLoader;
    private List<String> createdDatasetIds;
    private String datasetOriginalName;

    // Has to match data in the dataset-test-dataset.json file
    // and not match data in the dataset-test-baddata.json file
    private final String[] data = {"Andrea", "Dan", "Rori", "Jeremy"};

    @Before
    public void setup() throws Exception {
        classLoader = getClass().getClassLoader();
        createdDatasetIds = new ArrayList<>();
    }

    @After
    public void tearDown() throws Exception {
        // TODO: add study deletes when that is available

        for (String datasetId : createdDatasetIds) {
            deleteTestDataset(datasetId);
        }
    }

    @Test
    public void testHappyPath() throws Exception {
        StudySummaryModel studySummary = createTestStudy("dataset-test-study.json");
        loadStudyData(studySummary.getName(), "thetable", "dataset-test-study-data.csv");

        DatasetRequestModel datasetRequest = makeDatasetTestRequest(studySummary, "dataset-test-dataset.json");
        MockHttpServletResponse response = performCreateDataset(datasetRequest, "_happy_");
        DatasetSummaryModel summaryModel = handleCreateDatasetSuccessCase(datasetRequest, response);

        DatasetModel datasetModel = getTestDataset(summaryModel.getId(), datasetRequest, studySummary);

        deleteTestDataset(datasetModel.getId());
        // Duplicate delete should work
        deleteTestDataset(datasetModel.getId());

        getNonexistentDataset(datasetModel.getId());
    }

    @Test
    public void testMinimal() throws Exception {
        StudySummaryModel studySummary = setupMinimalStudy();
        String studyName = PDAO_PREFIX + studySummary.getName();
        long studyParticipants = queryForCount(studyName, "participant");
        assertThat("study participants loaded properly", studyParticipants, equalTo(2L));
        long studySamples = queryForCount(studyName, "sample");
        assertThat("study samples loaded properly", studySamples, equalTo(5L));

        DatasetRequestModel datasetRequest = makeDatasetTestRequest(studySummary, "study-minimal-dataset.json");
        MockHttpServletResponse response = performCreateDataset(datasetRequest, "");
        DatasetSummaryModel summaryModel = handleCreateDatasetSuccessCase(datasetRequest, response);
        getTestDataset(summaryModel.getId(), datasetRequest, studySummary);

        long datasetParticipants = queryForCount(summaryModel.getName(), "participant");
        assertThat("study participants loaded properly", datasetParticipants, equalTo(1L));
        long datasetSamples = queryForCount(summaryModel.getName(), "sample");
        assertThat("study samples loaded properly", datasetSamples, equalTo(2L));
    }

    @Test
    public void testMinimalBadAsset() throws Exception {
        StudySummaryModel studySummary = setupMinimalStudy();
        DatasetRequestModel datasetRequest = makeDatasetTestRequest(studySummary,
                "study-minimal-dataset-bad-asset.json");
        MockHttpServletResponse response = performCreateDataset(datasetRequest, "");
        ErrorModel errorModel = handleCreateDatasetFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Asset"));
        assertThat(errorModel.getMessage(), containsString("NotARealAsset"));
    }

    @Test
    public void testEnumeration() throws Exception {
        StudySummaryModel studySummary = createTestStudy("dataset-test-study.json");
        loadStudyData(studySummary.getName(), "thetable", "dataset-test-study-data.csv");
        DatasetRequestModel datasetRequest = makeDatasetTestRequest(studySummary, "dataset-test-dataset.json");

        // Other unit tests exercise the array bounds, so here we don't fuss with that here.
        // Just make sure we get the same dataset summary that we made.
        List<DatasetSummaryModel> datasetList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MockHttpServletResponse response = performCreateDataset(datasetRequest, "_enum_");
            DatasetSummaryModel summaryModel = handleCreateDatasetSuccessCase(datasetRequest, response);
            datasetList.add(summaryModel);
        }

        DatasetSummaryModel[] enumeratedArray = enumerateTestDatasets();

        // The enumeratedArray may contain more datasets than just the set we created,
        // but ours should be in order in the enumeration. So we do a merge waiting until we match
        // by id and then comparing contents.
        int compareIndex = 0;
        for (int i = 0; i < enumeratedArray.length; i++) {
            if (enumeratedArray[i].getId().equals(datasetList.get(compareIndex).getId())) {
                assertThat("Enumeration summary matches create summary",
                        enumeratedArray[i], equalTo(datasetList.get(compareIndex)));
                compareIndex++;
            }
        }
        assertThat("we found all datasets", compareIndex, equalTo(5));

        for (int i = 0; i < 5; i++) {
            deleteTestDataset(enumeratedArray[i].getId());
        }
    }

    @Test
    public void testBadData() throws Exception {
        StudySummaryModel studySummary = createTestStudy("dataset-test-study.json");
        loadStudyData(studySummary.getName(), "thetable", "dataset-test-study-data.csv");
        DatasetRequestModel badDataRequest = makeDatasetTestRequest(studySummary,
                "dataset-test-dataset-baddata.json");

        MockHttpServletResponse response = performCreateDataset(badDataRequest, "_baddata_");
        ErrorModel errorModel = handleCreateDatasetFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Fred"));
    }

    // !!! This test is intended to be run manually when the BigQuery project gets orphans in it.
    // !!! It tries to delete all datasets from the project.
    // You have to comment out the @Ignore to run it and not forget to uncomment it when you are done.
    @Ignore
    @Test
    public void deleteAllBigQueryProjects() throws Exception {
        // Collect a list of datasets. Then delete each one.
        List<DatasetId> idList = new ArrayList<>();
        for (com.google.cloud.bigquery.Dataset dataset :  bigQuery.listDatasets().iterateAll()) {
            idList.add(dataset.getDatasetId());
        }

        for (DatasetId id : idList) {
            bigQuery.delete(id, BigQuery.DatasetDeleteOption.deleteContents());
        }
    }

    private StudySummaryModel setupMinimalStudy() throws Exception {
        StudySummaryModel studySummary = createTestStudy("study-minimal.json");
        loadStudyData(studySummary.getName(), "participant", "study-minimal-participant.csv");
        loadStudyData(studySummary.getName(), "sample", "study-minimal-sample.csv");
        return  studySummary;
    }

    // create a study to create datasets in and return its id
    private StudySummaryModel createTestStudy(String resourcePath) throws Exception {
        String studyJson = IOUtils.toString(classLoader.getResourceAsStream(resourcePath));
        StudyRequestModel studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJson);
        studyRequest.setName(randomizedName(studyRequest.getName(), ""));

        MvcResult result = mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(studyRequest)))
                .andExpect(status().isCreated())
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        StudySummaryModel studySummaryModel =
                objectMapper.readValue(response.getContentAsString(), StudySummaryModel.class);

        return studySummaryModel;
    }

    private void loadStudyData(String studyName, String tableName, String resourcePath) throws Exception {
        String datasetName = PDAO_PREFIX + studyName;
        String location = "US"; // ?? Hope this will work. It is what
        TableId tableId = TableId.of(datasetName, tableName);

        CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();
        WriteChannelConfiguration writeChannelConfiguration =
                WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(csvOptions).build();

        // The location must be specified; other fields can be auto-detected.
        JobId jobId = JobId.newBuilder().setLocation(location).build();
        TableDataWriteChannel writer = bigQuery.writer(jobId, writeChannelConfiguration);

        // Write data to writer
        try (OutputStream stream = Channels.newOutputStream(writer);
             InputStream csvStream = classLoader.getResourceAsStream(resourcePath)) {
            IOUtils.copy(csvStream, stream);
        }

        // Get load job
        Job job = writer.getJob();
        job = job.waitFor();
        JobStatus jobStatus = job.getStatus();
        List<BigQueryError> jobErrors = jobStatus.getExecutionErrors();
        if (jobErrors != null && jobErrors.size() != 0) {
            System.out.println("Errors loading study data: ");
            for (BigQueryError bqError : jobErrors) {
                System.out.println(bqError.toString());
            }
            fail("Failed to load study data");
        }
    }

    private DatasetRequestModel makeDatasetTestRequest(StudySummaryModel studySummaryModel,
                                                       String resourcePath) throws Exception {
        String datasetJson = IOUtils.toString(classLoader.getResourceAsStream(resourcePath));
        DatasetRequestModel datasetRequest = objectMapper.readerFor(DatasetRequestModel.class).readValue(datasetJson);
        datasetRequest.getContents().get(0).getSource().setStudyName(studySummaryModel.getName());
        return datasetRequest;
    }

    private MockHttpServletResponse performCreateDataset(DatasetRequestModel datasetRequest, String infix)
            throws Exception {
        datasetOriginalName = datasetRequest.getName();
        String datasetName = randomizedName(datasetOriginalName, infix);
        datasetRequest.setName(datasetName);

        String jsonRequest = objectMapper.writeValueAsString(datasetRequest);

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
// TODO: swagger field validation errors do not set content type; they log and return nothing
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();
        MockHttpServletResponse response = validateJobModelAndWait(result);
        return response;
    }

    private DatasetSummaryModel handleCreateDatasetSuccessCase(DatasetRequestModel datasetRequest,
                                                               MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        if (!responseStatus.is2xxSuccessful()) {
            String failMessage = "createTestDataset failed: status=" + responseStatus.toString();
            if (StringUtils.contains(responseBody, "message")) {
                // If the responseBody contains the word 'message', then we try to decode it as an ErrorModel
                // so we can generate good failure information.
                ErrorModel errorModel = objectMapper.readValue(responseBody, ErrorModel.class);
                failMessage += " msg=" + errorModel.getMessage();
            }
            fail(failMessage);
        }

        DatasetSummaryModel summaryModel = objectMapper.readValue(responseBody, DatasetSummaryModel.class);
        createdDatasetIds.add(summaryModel.getId());

        assertThat(summaryModel.getDescription(), equalTo(datasetRequest.getDescription()));
        assertThat(summaryModel.getName(), equalTo(datasetRequest.getName()));

        // Reset the name in the dataset request
        datasetRequest.setName(datasetOriginalName);

        return summaryModel;
    }

    private ErrorModel handleCreateDatasetFailureCase(MockHttpServletResponse response) throws Exception {
        String responseBody = response.getContentAsString();
        HttpStatus responseStatus = HttpStatus.valueOf(response.getStatus());
        assertFalse("Expect create dataset failure", responseStatus.is2xxSuccessful());

        assertTrue("Error model was returned on failure",
                StringUtils.contains(responseBody, "message"));

        return objectMapper.readValue(responseBody, ErrorModel.class);
    }

    private void deleteTestDataset(String id) throws Exception {
        MvcResult result = mvc.perform(delete("/api/repository/v1/datasets/" + id)).andReturn();
        MockHttpServletResponse response = validateJobModelAndWait(result);
        assertThat(response.getStatus(), equalTo(HttpStatus.NO_CONTENT.value()));
    }

    private DatasetSummaryModel[] enumerateTestDatasets() throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets?offset=0&limit=100"))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        DatasetSummaryModel[] summaryArray =
                objectMapper.readValue(response.getContentAsString(), DatasetSummaryModel[].class);
        return summaryArray;
    }

    private DatasetModel getTestDataset(String id,
                                        DatasetRequestModel datasetRequest,
                                        StudySummaryModel studySummary) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        DatasetModel datasetModel = objectMapper.readValue(response.getContentAsString(), DatasetModel.class);

        assertThat(datasetModel.getDescription(), equalTo(datasetRequest.getDescription()));
        assertThat(datasetModel.getName(), startsWith(datasetRequest.getName()));

        assertThat("source array has one element",
                datasetModel.getSource().size(), equalTo(1));
        DatasetSourceModel sourceModel = datasetModel.getSource().get(0);
        assertThat("dataset study summary is the same as from study",
                sourceModel.getStudy(), equalTo(studySummary));

        return datasetModel;
    }

    private void getNonexistentDataset(String id) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        ErrorModel errorModel = objectMapper.readValue(response.getContentAsString(), ErrorModel.class);
        assertThat("proper not found error", errorModel.getMessage(), startsWith("Dataset not found"));
    }

    // TODO: this can probably be common code for anything async
    private MockHttpServletResponse validateJobModelAndWait(MvcResult inResult) throws Exception {
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

    // Get the count of rows in a table or view
    private long queryForCount(String datasetName, String tableName) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM `")
                .append(projectId).append('.').append(datasetName).append('.').append(tableName).append('`');
        String sql = builder.toString();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        TableResult result = bigQuery.query(queryConfig);
        FieldValueList row = result.iterateAll().iterator().next();
        FieldValue countValue = row.get(0);
        return countValue.getLongValue();
    }

    private String randomizedName(String baseName, String infix) {
        String name = baseName + infix + UUID.randomUUID().toString();
        return StringUtils.replaceChars(name, '-', '_');
    }

}
