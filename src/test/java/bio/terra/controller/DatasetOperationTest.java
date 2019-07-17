package bio.terra.controller;

import bio.terra.category.Connected;
import bio.terra.dao.StudyDao;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.JsonLoader;
import bio.terra.fixtures.Names;
import bio.terra.fixtures.ProfileFixtures;
import bio.terra.metadata.BillingProfile;
import bio.terra.metadata.Study;
import bio.terra.metadata.StudyDataProject;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetSourceModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.model.StudySummaryModel;
import bio.terra.pdao.bigquery.BigQueryProject;
import bio.terra.resourcemanagement.dao.ProfileDao;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import bio.terra.service.SamClientService;
import bio.terra.service.dataproject.DataProjectService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
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
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DatasetOperationTest {
    private static final boolean deleteOnTeardown = true;

    // private Logger logger = LoggerFactory.getLogger("bio.terra.controller.DatasetOperationTest");

    // TODO: MORE TESTS to be done when we can ingest data:
    // - test more complex studies with relationships
    // - test relationship walking with valid and invalid setups
    // TODO: MORE TESTS when we separate the value translation from the create
    // - test invalid row ids

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private JsonLoader jsonLoader;
    @Autowired private StudyDao studyDao;
    @Autowired private ProfileDao profileDao;
    @Autowired private DataProjectService dataProjectService;
    @Autowired private GoogleResourceConfiguration googleResourceConfiguration;

    @MockBean
    private SamClientService samService;

    private List<String> createdDatasetIds;
    private List<String> createdStudyIds;
    private String datasetOriginalName;
    private BillingProfile billingProfile;

    @Before
    public void setup() throws Exception {
        // TODO all of this should be refactored to use connected operations, and that should be made a component
        createdDatasetIds = new ArrayList<>();
        createdStudyIds = new ArrayList<>();
        ConnectedOperations.stubOutSamCalls(samService);
        billingProfile = ProfileFixtures.billingProfileForAccount(googleResourceConfiguration.getCoreBillingAccount());
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);
    }

    @After
    public void tearDown() throws Exception {
        if (deleteOnTeardown) {
            for (String datasetId : createdDatasetIds) {
                deleteTestDataset(datasetId);
            }

            for (String studyId : createdStudyIds) {
                deleteTestStudy(studyId);
            }
            profileDao.deleteBillingProfileById(billingProfile.getId());
        }
    }

    @Test
    public void testHappyPath() throws Exception {
        StudySummaryModel studySummary = createTestStudy("dataset-test-study.json");
        loadCsvData(studySummary.getName(), "thetable", "dataset-test-study-data.csv");

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
        BigQueryProject bigQueryProject = bigQueryProjectForStudyName(studySummary.getName());
        long studyParticipants = queryForCount(studyName, "participant", bigQueryProject);
        assertThat("study participants loaded properly", studyParticipants, equalTo(2L));
        long studySamples = queryForCount(studyName, "sample", bigQueryProject);
        assertThat("study samples loaded properly", studySamples, equalTo(5L));

        DatasetRequestModel datasetRequest = makeDatasetTestRequest(studySummary,
                "study-minimal-dataset.json");
        MockHttpServletResponse response = performCreateDataset(datasetRequest, "");
        DatasetSummaryModel summaryModel = handleCreateDatasetSuccessCase(datasetRequest, response);
        getTestDataset(summaryModel.getId(), datasetRequest, studySummary);

        long datasetParticipants = queryForCount(summaryModel.getName(), "participant", bigQueryProject);
        assertThat("study participants loaded properly", datasetParticipants, equalTo(1L));
        long datasetSamples = queryForCount(summaryModel.getName(), "sample", bigQueryProject);
        assertThat("study samples loaded properly", datasetSamples, equalTo(2L));
    }

    @Test
    public void testArrayStruct() throws Exception {
        StudySummaryModel studySummary = setupArrayStructStudy();
        String studyName = PDAO_PREFIX + studySummary.getName();
        BigQueryProject bigQueryProject = bigQueryProjectForStudyName(studySummary.getName());
        long studyParticipants = queryForCount(studyName, "participant", bigQueryProject);
        assertThat("study participants loaded properly", studyParticipants, equalTo(2L));
        long studySamples = queryForCount(studyName, "sample", bigQueryProject);
        assertThat("study samples loaded properly", studySamples, equalTo(5L));

        DatasetRequestModel datasetRequest = makeDatasetTestRequest(studySummary,
            "dataset-array-struct.json");
        MockHttpServletResponse response = performCreateDataset(datasetRequest, "");
        DatasetSummaryModel summaryModel = handleCreateDatasetSuccessCase(datasetRequest, response);
        getTestDataset(summaryModel.getId(), datasetRequest, studySummary);

        long datasetParticipants = queryForCount(summaryModel.getName(), "participant", bigQueryProject);
        assertThat("study participants loaded properly", datasetParticipants, equalTo(2L));
        long datasetSamples = queryForCount(summaryModel.getName(), "sample", bigQueryProject);
        assertThat("study samples loaded properly", datasetSamples, equalTo(3L));
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
        loadCsvData(studySummary.getName(), "thetable", "dataset-test-study-data.csv");
        DatasetRequestModel datasetRequest = makeDatasetTestRequest(studySummary, "dataset-test-dataset.json");

        // Other unit tests exercise the array bounds, so here we don't fuss with that here.
        // Just make sure we get the same dataset summary that we made.
        List<DatasetSummaryModel> datasetList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            MockHttpServletResponse response = performCreateDataset(datasetRequest, "_enum_");
            DatasetSummaryModel summaryModel = handleCreateDatasetSuccessCase(datasetRequest, response);
            datasetList.add(summaryModel);
        }

        EnumerateDatasetModel enumResponse = enumerateTestDatasets();
        List<DatasetSummaryModel> enumeratedArray = enumResponse.getItems();
        assertThat("total is correct", enumResponse.getTotal(), equalTo(5));

        // The enumeratedArray may contain more datasets than just the set we created,
        // but ours should be in order in the enumeration. So we do a merge waiting until we match
        // by id and then comparing contents.
        int compareIndex = 0;
        for (DatasetSummaryModel anEnumeratedDataset : enumeratedArray) {
            if (anEnumeratedDataset.getId().equals(datasetList.get(compareIndex).getId())) {
                assertThat("MetadataEnumeration summary matches create summary",
                    anEnumeratedDataset, equalTo(datasetList.get(compareIndex)));
                compareIndex++;
            }
        }

        assertThat("we found all datasets", compareIndex, equalTo(5));

        for (int i = 0; i < 5; i++) {
            deleteTestDataset(enumeratedArray.get(i).getId());
        }
    }

    @Test
    public void testBadData() throws Exception {
        StudySummaryModel studySummary = createTestStudy("dataset-test-study.json");
        loadCsvData(studySummary.getName(), "thetable", "dataset-test-study-data.csv");
        DatasetRequestModel badDataRequest = makeDatasetTestRequest(studySummary,
                "dataset-test-dataset-baddata.json");

        MockHttpServletResponse response = performCreateDataset(badDataRequest, "_baddata_");
        ErrorModel errorModel = handleCreateDatasetFailureCase(response);
        assertThat(errorModel.getMessage(), containsString("Fred"));
    }

    private StudySummaryModel setupMinimalStudy() throws Exception {
        StudySummaryModel studySummary = createTestStudy("study-minimal.json");
        loadCsvData(studySummary.getName(), "participant", "study-minimal-participant.csv");
        loadCsvData(studySummary.getName(), "sample", "study-minimal-sample.csv");
        return  studySummary;
    }

    private StudySummaryModel setupArrayStructStudy() throws Exception {
        StudySummaryModel studySummary = createTestStudy("study-array-struct.json");
        loadJsonData(studySummary.getName(), "participant", "study-array-struct-participant.json");
        loadJsonData(studySummary.getName(), "sample", "study-array-struct-sample.json");
        return  studySummary;
    }

    // create a study to create datasets in and return its id
    private StudySummaryModel createTestStudy(String resourcePath) throws Exception {
        StudyRequestModel studyRequest = jsonLoader.loadObject(resourcePath, StudyRequestModel.class);
        studyRequest
            .name(Names.randomizeName(studyRequest.getName()))
            .defaultProfileId(billingProfile.getId().toString());

        MvcResult result = mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(studyRequest)))
                .andExpect(status().isCreated())
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        StudySummaryModel studySummaryModel =
                objectMapper.readValue(response.getContentAsString(), StudySummaryModel.class);
        createdStudyIds.add(studySummaryModel.getId());

        return studySummaryModel;
    }

    private void loadCsvData(String studyName, String tableName, String resourcePath) throws Exception {
        FormatOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();
        loadData(studyName, tableName, resourcePath, csvOptions);
    }

    private void loadJsonData(String studyName, String tableName, String resourcePath) throws Exception {
        loadData(studyName, tableName, resourcePath, FormatOptions.json());
    }

    private BigQueryProject bigQueryProjectForStudyName(String studyName) {
        Study study = studyDao.retrieveByName(studyName);
        StudyDataProject dataProject = dataProjectService.getProjectForStudy(study);
        return new BigQueryProject(dataProject.getGoogleProjectId());
    }

    private void loadData(String studyName,
                          String tableName,
                          String resourcePath,
                          FormatOptions options) throws Exception {
        String datasetName = PDAO_PREFIX + studyName;
        String location = "US";
        TableId tableId = TableId.of(datasetName, tableName);
        BigQueryProject bigQueryProject = bigQueryProjectForStudyName(studyName);
        BigQuery bigQuery = bigQueryProject.getBigQuery();

        WriteChannelConfiguration writeChannelConfiguration =
            WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(options).build();

        // The location must be specified; other fields can be auto-detected.
        JobId jobId = JobId.newBuilder().setLocation(location).build();
        TableDataWriteChannel writer = bigQuery.writer(jobId, writeChannelConfiguration);

        // Write data to writer
        try (OutputStream stream = Channels.newOutputStream(writer);
             InputStream csvStream = jsonLoader.getClassLoader().getResourceAsStream(resourcePath)) {
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
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(resourcePath, DatasetRequestModel.class);
        // TODO SingleStudyDataset
        datasetRequest.getContents().get(0).getSource().setStudyName(studySummaryModel.getName());
        datasetRequest.profileId(studySummaryModel.getDefaultProfileId());
        return datasetRequest;
    }

    private MockHttpServletResponse performCreateDataset(DatasetRequestModel datasetRequest, String infix)
            throws Exception {
        datasetOriginalName = datasetRequest.getName();
        String datasetName = Names.randomizeNameInfix(datasetOriginalName, infix);
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
            } else {
                failMessage += " responseBody=" + responseBody;
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
        assertThat(response.getStatus(), equalTo(HttpStatus.OK.value()));
        checkDeleteResponse(response);
    }

    private void deleteTestStudy(String id) throws Exception {
        // We only use this for @After, so we don't check return values
        MvcResult result = mvc.perform(delete("/api/repository/v1/studies/" + id)).andReturn();
        checkDeleteResponse(result.getResponse());
    }

    private void checkDeleteResponse(MockHttpServletResponse response) throws Exception {
        DeleteResponseModel responseModel =
            objectMapper.readValue(response.getContentAsString(), DeleteResponseModel.class);
        assertTrue("Valid delete response object state enumeration",
            (responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.DELETED ||
                responseModel.getObjectState() == DeleteResponseModel.ObjectStateEnum.NOT_FOUND));
    }

    private EnumerateDatasetModel enumerateTestDatasets() throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets?offset=0&limit=1000"))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        EnumerateDatasetModel summary =
                objectMapper.readValue(response.getContentAsString(), EnumerateDatasetModel.class);
        return summary;
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
    private long queryForCount(String datasetName, String tableName, BigQueryProject bigQueryProject) throws Exception {
        String bigQueryProjectId = bigQueryProject.getProjectId();
        BigQuery bigQuery = bigQueryProject.getBigQuery();
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT COUNT(*) FROM `")
                .append(bigQueryProjectId).append('.').append(datasetName).append('.').append(tableName).append('`');
        String sql = builder.toString();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        TableResult result = bigQuery.query(queryConfig);
        FieldValueList row = result.iterateAll().iterator().next();
        FieldValue countValue = row.get(0);
        return countValue.getLongValue();
    }

}
