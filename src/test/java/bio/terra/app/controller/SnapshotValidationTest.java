package bio.terra.app.controller;

import bio.terra.category.Unit;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestSourceModel;
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
public class SnapshotValidationTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private SnapshotRequestModel snapshotRequest;


    @Before
    public void setup() {
        snapshotRequest = makeSnapshotRequest();
    }

    private void expectBadSnapshotCreateRequest(SnapshotRequestModel snapshotRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(snapshotRequest)))
                .andExpect(status().is4xxClientError());
    }

    public SnapshotRequestModel makeSnapshotRequest() {
        SnapshotRequestSourceModel snapshotRequestSourceModel = new SnapshotRequestSourceModel()
                .datasetName("dataset")
                .assetName("asset");
        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
                .source(snapshotRequestSourceModel)
                .rootValues(Arrays.asList("sample 1", "sample 2", "sample 3"));
        SnapshotRequestModel snapshotRequestModel = new SnapshotRequestModel()
                .name("snapshot")
                .description("snapshot description")
                .addContentsItem(snapshotRequestContentsModel);
        return snapshotRequestModel;
    }


    @Test
    public void testSnapshotNameInvalid() throws Exception {
        snapshotRequest.name("no spaces");
        expectBadSnapshotCreateRequest(snapshotRequest);

        snapshotRequest.name("no-dashes");
        expectBadSnapshotCreateRequest(snapshotRequest);

        snapshotRequest.name("");
        expectBadSnapshotCreateRequest(snapshotRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        snapshotRequest.name(tooLong);
        expectBadSnapshotCreateRequest(snapshotRequest);
    }

    @Test
    public void testSnapshotDescriptionInvalid() throws Exception {
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
        snapshotRequest.description(tooLongDescription);
        expectBadSnapshotCreateRequest(snapshotRequest);

        snapshotRequest.description(null);
        expectBadSnapshotCreateRequest(snapshotRequest);
    }

    @Test
    public void testSnapshotValuesListEmpty() throws Exception {
        ArrayList empty = new ArrayList<String>();
        SnapshotRequestSourceModel snapshotRequestSourceModel = new SnapshotRequestSourceModel()
                .datasetName("dataset")
                .assetName("asset");
        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
                .source(snapshotRequestSourceModel)
                .rootValues(empty);
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);
    }

    @Test
    public void testSnapshotDatasetNameInvalid() throws Exception {
        SnapshotRequestSourceModel snapshotRequestSourceModel = new SnapshotRequestSourceModel()
                .datasetName("no spaces")
                .assetName("asset");
        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
                .source(snapshotRequestSourceModel)
                .rootValues(Collections.singletonList("root"));
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);

        snapshotRequestSourceModel.datasetName("no-dashes");
        snapshotRequestContentsModel.source(snapshotRequestSourceModel);
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);

        snapshotRequestSourceModel.datasetName("");
        snapshotRequestContentsModel.source(snapshotRequestSourceModel);
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        snapshotRequestSourceModel.datasetName(tooLong);
        snapshotRequestContentsModel.source(snapshotRequestSourceModel);
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);
    }


    @Test
    public void testSnapshotAssetNameInvalid() throws Exception {
        SnapshotRequestSourceModel snapshotRequestSourceModel = new SnapshotRequestSourceModel()
                .datasetName("dataset")
                .assetName("no spaces");
        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
                .source(snapshotRequestSourceModel)
                .rootValues(Collections.singletonList("root"));
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);

        snapshotRequestSourceModel.assetName("no-dashes");
        snapshotRequestContentsModel.source(snapshotRequestSourceModel);
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);

        snapshotRequestSourceModel.assetName("");
        snapshotRequestContentsModel.source(snapshotRequestSourceModel);
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        snapshotRequestSourceModel.assetName(tooLong);
        snapshotRequestContentsModel.source(snapshotRequestSourceModel);
        snapshotRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotRequest);
    }

    @Test
    public void testDatasetNameMissing() throws Exception {
        snapshotRequest.name(null);
        expectBadSnapshotCreateRequest(snapshotRequest);
    }
}
