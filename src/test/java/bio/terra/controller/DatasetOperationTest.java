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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
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

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private DatasetRequestModel datasetRequest;
    private StudyRequestModel studyRequest;
    private StudySummaryModel studySummary;
    private String studyId;

    @Before
    public void setup() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String studyJson = IOUtils.toString(classLoader.getResourceAsStream("dataset-test-study.json"));
        studyRequest = objectMapper.readerFor(StudyRequestModel.class).readValue(studyJson);
        studyRequest.setName(randomizedName(studyRequest.getName()));
        createTestStudy();

        String datasetJson = IOUtils.toString(classLoader.getResourceAsStream("dataset-test-dataset.json"));
        datasetRequest = objectMapper.readerFor(DatasetRequestModel.class).readValue(datasetJson);
        datasetRequest.getContents().get(0).getSource().setStudyName(studyRequest.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTestStudy();
    }

    // TODO: happy path; study, dataset, retrieve and validate dataset
    // multiple dataset w/enumeration
    // delete dataset

    @Test
    public void testHappyPath() throws Exception {
        DatasetSummaryModel summaryModel = createTestDataset(datasetRequest);

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
            DatasetSummaryModel dataset = createTestDataset(datasetRequest);
            datasetList.add(dataset);
        }

        DatasetSummaryModel[] enumeratedArray = enumerateTestDatasets();
        for (int i = 0; i < 5; i++) {
            // Match the dataset summaries and retrieve and validate the dataset
            assertThat(enumeratedArray[i], equalTo(datasetList.get(i)));
            getTestDataset(datasetList.get(i).getId());
        }

        for (int i = 0; i < 5; i++) {
            deleteTestDataset(enumeratedArray[i].getId());
        }
    }

    private DatasetSummaryModel createTestDataset(DatasetRequestModel datasetRequest) throws Exception {
        String baseName = datasetRequest.getName();
        String datasetName = randomizedName(baseName);
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
        mvc.perform(delete("/api/repository/v1/datasets/" + id)).andExpect(status().isOk());
    }

    private DatasetSummaryModel[] enumerateTestDatasets() throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets?offset=0&limit=10"))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();

        MockHttpServletResponse response = result.getResponse();
        DatasetSummaryModel[] summaryArray =
                objectMapper.readValue(response.getContentAsString(), DatasetSummaryModel[].class);
        return summaryArray;
    }

    private DatasetModel getTestDataset(String id) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets/" + id))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
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
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
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
        studyId = studySummary.getId();
    }

    private void deleteTestStudy() throws Exception {
        String url = "/api/repository/v1/studies/" + studyId;
        mvc.perform(delete(url)).andExpect(status().isOk());
    }

    private String randomizedName(String baseName) {
        return StringUtils.replaceChars(baseName + UUID.randomUUID().toString(), '-', '_');
    }

}
