package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetRequestSourceModel;
import bio.terra.stairway.Stairway;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetTest {

    @MockBean
    private Stairway stairway;

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private DatasetRequestModel datasetRequest;


    @Before
    public void setup() {
        datasetRequest = makeDatasetRequest();
    }

    private void expectBadDatasetCreateRequest(DatasetRequestModel datasetRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(datasetRequest)))
                .andExpect(status().is4xxClientError());
    }

    public DatasetRequestModel makeDatasetRequest() {
        DatasetRequestSourceModel datasetRequestSourceModel = new DatasetRequestSourceModel()
                .assetName("asset")
                .fieldName("field")
                .studyName("study")
                .values(Arrays.asList("sample 1", "sample 2", "sample 3"));
        DatasetRequestModel datasetRequestModel = new DatasetRequestModel()
                .name("dataset")
                .description("dataset description")
                .addSourceItem(datasetRequestSourceModel);
        return datasetRequestModel;
    }


    @Test
    public void testDatasetNameInvalid() throws Exception {
        datasetRequest.name("no spaces");
        expectBadDatasetCreateRequest(datasetRequest);

        datasetRequest.name("no-dashes");
        expectBadDatasetCreateRequest(datasetRequest);

        datasetRequest.name("");
        expectBadDatasetCreateRequest(datasetRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        datasetRequest.name(tooLong);
        expectBadDatasetCreateRequest(datasetRequest);
    }

    @Test
    public void testDatasetValuesListEmpty() throws Exception {
        ArrayList empty = new ArrayList<String>();
        DatasetRequestSourceModel datasetRequestSourceModelEmpty = new DatasetRequestSourceModel()
                .assetName("asset")
                .fieldName("field")
                .studyName("study")
                .values(empty);
        datasetRequest.source(Collections.singletonList(datasetRequestSourceModelEmpty));
        expectBadDatasetCreateRequest(datasetRequest);
    }

    @Test
    public void testStudyNameMissing() throws Exception {
        datasetRequest.name(null);
        expectBadDatasetCreateRequest(datasetRequest);
    }
}
