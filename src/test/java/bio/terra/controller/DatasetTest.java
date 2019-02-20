package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.model.DatasetRequestContentsModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatasetRequestSourceModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetTest {

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
                .studyName("study")
                .assetName("asset");
        DatasetRequestContentsModel datasetRequestContentsModel = new DatasetRequestContentsModel()
                .source(datasetRequestSourceModel)
                .fieldName("field")
                .rootValues(Arrays.asList("sample 1", "sample 2", "sample 3"));
        DatasetRequestModel datasetRequestModel = new DatasetRequestModel()
                .name("dataset")
                .description("dataset description")
                .addContentsItem(datasetRequestContentsModel);
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
    public void testDatasetDescriptionInvalid() throws Exception {
        String tooLongDescription = "People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good, " +
                "People = Good, People = Good, People = Good, People = Good, People = Good, People = Good";
        datasetRequest.description(tooLongDescription);
        expectBadDatasetCreateRequest(datasetRequest);

        datasetRequest.description(null);
        expectBadDatasetCreateRequest(datasetRequest);
    }

    @Test
    public void testDatasetValuesListEmpty() throws Exception {
        ArrayList empty = new ArrayList<String>();
        DatasetRequestSourceModel datasetRequestSourceModel = new DatasetRequestSourceModel()
                .studyName("study")
                .assetName("asset");
        DatasetRequestContentsModel datasetRequestContentsModel = new DatasetRequestContentsModel()
                .source(datasetRequestSourceModel)
                .fieldName("field")
                .rootValues(empty);
        datasetRequest.contents(Collections.singletonList(datasetRequestContentsModel));
        expectBadDatasetCreateRequest(datasetRequest);
    }

    @Test
    public void testStudyNameMissing() throws Exception {
        datasetRequest.name(null);
        expectBadDatasetCreateRequest(datasetRequest);
    }
}
