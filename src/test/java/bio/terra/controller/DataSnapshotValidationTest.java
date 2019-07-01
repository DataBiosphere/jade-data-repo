package bio.terra.controller;

import bio.terra.category.Unit;
import bio.terra.model.DataSnapshotRequestContentsModel;
import bio.terra.model.DataSnapshotRequestModel;
import bio.terra.model.DataSnapshotRequestSourceModel;
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
import org.springframework.test.context.ActiveProfiles;
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
@ActiveProfiles("google")
@Category(Unit.class)
public class DataSnapshotValidationTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private DataSnapshotRequestModel dataSnapshotRequest;


    @Before
    public void setup() {
        dataSnapshotRequest = makeDataSnapshotRequest();
    }

    private void expectBadDataSnapshotCreateRequest(DataSnapshotRequestModel dataSnapshotRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/datasnapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(dataSnapshotRequest)))
                .andExpect(status().is4xxClientError());
    }

    public DataSnapshotRequestModel makeDataSnapshotRequest() {
        DataSnapshotRequestSourceModel dataSnapshotRequestSourceModel = new DataSnapshotRequestSourceModel()
                .datasetName("dataset")
                .assetName("asset");
        DataSnapshotRequestContentsModel dataSnapshotRequestContentsModel = new DataSnapshotRequestContentsModel()
                .source(dataSnapshotRequestSourceModel)
                .rootValues(Arrays.asList("sample 1", "sample 2", "sample 3"));
        DataSnapshotRequestModel dataSnapshotRequestModel = new DataSnapshotRequestModel()
                .name("dataSnapshot")
                .description("dataSnapshot description")
                .addContentsItem(dataSnapshotRequestContentsModel);
        return dataSnapshotRequestModel;
    }


    @Test
    public void testDataSnapshotNameInvalid() throws Exception {
        dataSnapshotRequest.name("no spaces");
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        dataSnapshotRequest.name("no-dashes");
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        dataSnapshotRequest.name("");
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        dataSnapshotRequest.name(tooLong);
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);
    }

    @Test
    public void testDataSnapshotDescriptionInvalid() throws Exception {
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
        dataSnapshotRequest.description(tooLongDescription);
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        dataSnapshotRequest.description(null);
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);
    }

    @Test
    public void testDataSnapshotValuesListEmpty() throws Exception {
        ArrayList empty = new ArrayList<String>();
        DataSnapshotRequestSourceModel dataSnapshotRequestSourceModel = new DataSnapshotRequestSourceModel()
                .datasetName("dataset")
                .assetName("asset");
        DataSnapshotRequestContentsModel dataSnapshotRequestContentsModel = new DataSnapshotRequestContentsModel()
                .source(dataSnapshotRequestSourceModel)
                .rootValues(empty);
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);
    }

    @Test
    public void testDataSnapshotDatasetNameInvalid() throws Exception {
        DataSnapshotRequestSourceModel dataSnapshotRequestSourceModel = new DataSnapshotRequestSourceModel()
                .datasetName("no spaces")
                .assetName("asset");
        DataSnapshotRequestContentsModel dataSnapshotRequestContentsModel = new DataSnapshotRequestContentsModel()
                .source(dataSnapshotRequestSourceModel)
                .rootValues(Collections.singletonList("root"));
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        dataSnapshotRequestSourceModel.datasetName("no-dashes");
        dataSnapshotRequestContentsModel.source(dataSnapshotRequestSourceModel);
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        dataSnapshotRequestSourceModel.datasetName("");
        dataSnapshotRequestContentsModel.source(dataSnapshotRequestSourceModel);
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        dataSnapshotRequestSourceModel.datasetName(tooLong);
        dataSnapshotRequestContentsModel.source(dataSnapshotRequestSourceModel);
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);
    }


    @Test
    public void testDataSnapshotAssetNameInvalid() throws Exception {
        DataSnapshotRequestSourceModel dataSnapshotRequestSourceModel = new DataSnapshotRequestSourceModel()
                .datasetName("dataset")
                .assetName("no spaces");
        DataSnapshotRequestContentsModel dataSnapshotRequestContentsModel = new DataSnapshotRequestContentsModel()
                .source(dataSnapshotRequestSourceModel)
                .rootValues(Collections.singletonList("root"));
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        dataSnapshotRequestSourceModel.assetName("no-dashes");
        dataSnapshotRequestContentsModel.source(dataSnapshotRequestSourceModel);
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        dataSnapshotRequestSourceModel.assetName("");
        dataSnapshotRequestContentsModel.source(dataSnapshotRequestSourceModel);
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        dataSnapshotRequestSourceModel.assetName(tooLong);
        dataSnapshotRequestContentsModel.source(dataSnapshotRequestSourceModel);
        dataSnapshotRequest.contents(Collections.singletonList(dataSnapshotRequestContentsModel));
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);
    }

    @Test
    public void testDatasetNameMissing() throws Exception {
        dataSnapshotRequest.name(null);
        expectBadDataSnapshotCreateRequest(dataSnapshotRequest);
    }
}
