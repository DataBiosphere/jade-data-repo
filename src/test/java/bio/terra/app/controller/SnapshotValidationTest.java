package bio.terra.app.controller;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotProvidedIdsRequestContentsModel;
import bio.terra.model.SnapshotProvidedIdsRequestModel;
import bio.terra.model.SnapshotProvidedIdsRequestTableModel;
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
import java.util.List;

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

    private SnapshotProvidedIdsRequestModel snapshotProvidedIdsRequestModel;


    @Before
    public void setup() {
        snapshotRequest = makeSnapshotRequest();
        snapshotProvidedIdsRequestModel = makeSnapshotProvidedIdsRequest();
    }

    private void expectBadSnapshotCreateRequest(SnapshotRequestModel snapshotRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(snapshotRequest)))
                .andExpect(status().is4xxClientError());
    }

    private void expectBadSPIdsCreateRequest(SnapshotProvidedIdsRequestModel snapshotRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/snapshots/ids")
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

    public SnapshotProvidedIdsRequestModel makeSnapshotProvidedIdsRequest() {
        List<String> columns = new ArrayList<>();
        List<String> rowIds = new ArrayList<>();
        List<SnapshotProvidedIdsRequestContentsModel> contents = new ArrayList<>();
        List<SnapshotProvidedIdsRequestTableModel> tables = new ArrayList<>();
        SnapshotProvidedIdsRequestTableModel snapshotRequestTableModel = new SnapshotProvidedIdsRequestTableModel()
            .tableName("snapshot")
            .columns(columns)
            .rowIds(rowIds);
        tables.add(snapshotRequestTableModel);
        SnapshotProvidedIdsRequestContentsModel snapshotRequestContentsModel = new SnapshotProvidedIdsRequestContentsModel()
            .datasetName("dataset")
            .tables(tables);
        contents.add(snapshotRequestContentsModel);
        SnapshotProvidedIdsRequestModel snapshotRequestModel = new SnapshotProvidedIdsRequestModel()
            .contents(contents);
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
    public void testSnapshotProvidedIdsMismatch() throws Exception {
        // Test that missing columns and row ids do not succeed
        ArrayList empty = new ArrayList<String>();
        SnapshotProvidedIdsRequestTableModel snapshotProvidedIdsRequestTableModel = new SnapshotProvidedIdsRequestTableModel()
            .tableName("table")
            .columns(empty)
            .rowIds(empty);
        ArrayList tables = new ArrayList<SnapshotProvidedIdsRequestTableModel>();
        tables.add(snapshotProvidedIdsRequestTableModel);
        SnapshotProvidedIdsRequestContentsModel snapshotRequestContentsModel = new SnapshotProvidedIdsRequestContentsModel()
            .datasetName("dataset")
            .tables(tables);
        snapshotProvidedIdsRequestModel.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSPIdsCreateRequest(snapshotProvidedIdsRequestModel);
    }

    @Test
    public void testDatasetNameMissing() throws Exception {
        snapshotRequest.name(null);
        expectBadSnapshotCreateRequest(snapshotRequest);
    }

    @Test
    public void testMissingProfileId() throws Exception {
        snapshotRequest.profileId(null);
        expectBadSnapshotCreateRequest(snapshotRequest);
    }
}
