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
import bio.terra.model.TableModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static bio.terra.pdao.PdaoConstant.PDAO_PREFIX;
import static bio.terra.pdao.PdaoConstant.PDAO_ROW_ID_COLUMN;
import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
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

    private DatasetRequestModel datasetRequest;
    private StudyRequestModel studyRequest;
    private StudySummaryModel studySummary;

    // Has to match data in the dataset-test-dataset.json file
    // and not match data in the dataset-test-baddata.json file
    private final String[] data = {"Andrea", "Dan", "Rori", "Jeremy"};

    @Before
    public void setup() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJson = IOUtils.toString(classLoader.getResourceAsStream("dataset-test-study.json"));
        studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJson);
        studyRequest.setName(randomizedName(studyRequest.getName(), ""));
        createTestStudy();

        String datasetJson = IOUtils.toString(classLoader.getResourceAsStream("dataset-test-dataset.json"));
        datasetRequest = objectMapper.readerFor(DatasetRequestModel.class).readValue(datasetJson);
        datasetRequest.getContents().get(0).getSource().setStudyName(studyRequest.getName());
    }


    // TODO: add @After for study delete when that is hooked up

    @Test
    public void testHappyPath() throws Exception {
        DatasetSummaryModel summaryModel = createTestDataset(datasetRequest, "happy");

        DatasetModel datasetModel = getTestDataset(summaryModel.getId());

        deleteTestDataset(datasetModel.getId());
        // Duplicate delete should work
        deleteTestDataset(datasetModel.getId());

        getNonexistentDataset(datasetModel.getId());
    }

    @Test
    public void testEnumeration() throws Exception {
        // Unit tests exercise the array bounds, so here we don't fuss with that here.
        // Just make sure we get the same dataset summary that we made.
        List<DatasetSummaryModel> datasetList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            DatasetSummaryModel dataset = createTestDataset(datasetRequest, "_enum_");
            datasetList.add(dataset);
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
        ClassLoader classLoader = getClass().getClassLoader();
        String datasetJson = IOUtils.toString(classLoader.getResourceAsStream("dataset-test-dataset-baddata.json"));
        DatasetRequestModel badDataRequest = objectMapper.readerFor(DatasetRequestModel.class).readValue(datasetJson);
        badDataRequest.getContents().get(0).getSource().setStudyName(studyRequest.getName());

        String baseName = badDataRequest.getName();
        String datasetName = randomizedName(baseName, "baddata");
        badDataRequest.setName(datasetName);
        String jsonRequest = objectMapper.writeValueAsString(badDataRequest);

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
// TODO: swagger field validation errors do not set content type; they log and return nothing
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();

        MockHttpServletResponse response = validateJobModelAndWait(result);

// TODO: the status code is always 201 right now and the response is the dataset summary model, because
// jobs doesn't notice that the flight failed...
        assertThat(response.getStatus(), equalTo(201));
//        ErrorModel errorModel = objectMapper.readValue(response.getContentAsString(), ErrorModel.class);
//        assertThat(errorModel.getMessage(), containsString("Fred"));
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

    private DatasetSummaryModel createTestDataset(DatasetRequestModel datasetRequest, String infix) throws Exception {
        String baseName = datasetRequest.getName();
        String datasetName = randomizedName(baseName, infix);
        datasetRequest.setName(datasetName);

        String jsonRequest = objectMapper.writeValueAsString(datasetRequest);

        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(jsonRequest))
// TODO: swagger field validation errors do not set content type; they log and return nothing
//                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();

        MockHttpServletResponse response = validateJobModelAndWait(result);

        DatasetSummaryModel summaryModel = objectMapper.readValue(response.getContentAsString(),
                DatasetSummaryModel.class);

        assertThat(summaryModel.getDescription(), equalTo(datasetRequest.getDescription()));
        assertThat(summaryModel.getName(), equalTo(datasetName));

        // Reset the name in the dataset request
        datasetRequest.setName(baseName);

        return summaryModel;
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

    private DatasetModel getTestDataset(String id) throws Exception {
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
        assertThat("dataset asset name is the same as from study",
                sourceModel.getAsset(), equalTo(studyRequest.getSchema().getAssets().get(0).getName()));

        assertThat("table list has one table",
                datasetModel.getTables().size(), equalTo(1));
        TableModel tableModel = datasetModel.getTables().get(0);
        assertThat("dataset table name is same as from study",
                tableModel.getName(), equalTo(studyRequest.getSchema().getTables().get(0).getName()));

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
            Assert.assertTrue("received expected jobs polling status",
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
                    assertThat("location heaeder for result", locationUrl,
                            equalTo(String.format("/api/repository/v1/jobs/%s/result", jobId)));
                    result = mvc.perform(get(locationUrl).accept(MediaType.APPLICATION_JSON)).andReturn();
                    return result.getResponse();

                default:
                    fail("invalid response status");
            }
        }
    }

    // create a study to create datasets in and return its id
    private void createTestStudy() throws Exception {
        MvcResult result = mvc.perform(post("/api/repository/v1/studies")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(studyRequest)))
                .andExpect(status().isCreated())
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        studySummary = objectMapper.readValue(response.getContentAsString(), StudySummaryModel.class);

        // Use BigQuery directly to load test data into the table.
        String studyDatasetName = PDAO_PREFIX + studySummary.getName();
        TableId bqTableId = TableId.of(studyDatasetName, "thetable");

        InsertAllRequest.Builder requestBuilder = InsertAllRequest.newBuilder(bqTableId);
        for (String datum : data) {
            Map<String, Object> row = new HashMap<>();
            row.put(PDAO_ROW_ID_COLUMN, UUID.randomUUID().toString());
            row.put("thecolumn", datum);
            requestBuilder.addRow(row);
        }
        InsertAllRequest bqrequest = requestBuilder.build();
        InsertAllResponse bqresponse = bigQuery.insertAll(bqrequest);
        assertFalse("no insert errors", bqresponse.hasErrors());
    }

    private String randomizedName(String baseName, String infix) {
        String name = baseName + infix + UUID.randomUUID().toString();
        return StringUtils.replaceChars(name, '-', '_');
    }

}
