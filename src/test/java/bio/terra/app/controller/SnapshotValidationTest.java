package bio.terra.app.controller;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
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

    private SnapshotRequestModel snapshotByAssetRequest;

    private SnapshotRequestModel snapshotByRowIdsRequestModel;

    private SnapshotRequestModel snapshotByQueryRequestModel;



    @Before
    public void setup() {
        snapshotByAssetRequest = makeSnapshotAssetRequest();
        snapshotByRowIdsRequestModel = makeSnapshotRowIdsRequest();
        snapshotByQueryRequestModel = makeSnapshotByQueryRequest();
    }

    private void expectBadSnapshotCreateRequest(SnapshotRequestModel snapshotRequest) throws Exception {
        mvc.perform(post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(snapshotRequest)))
                .andExpect(status().is4xxClientError());
    }

    // Generate a valid snapshot-by-asset request, we will tweak individual pieces to test validation below
    public SnapshotRequestModel makeSnapshotAssetRequest() {
        SnapshotRequestAssetModel assetSpec = new SnapshotRequestAssetModel()
            .assetName("asset")
            .rootValues(Arrays.asList("sample 1", "sample 2", "sample 3"));

        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYASSET)
            .assetSpec(assetSpec);

        return new SnapshotRequestModel()
            .name("snapshot")
            .description("snapshot description")
            .addContentsItem(snapshotRequestContentsModel);
    }

    // Generate a valid snapshot-by-rowId request, we will tweak individual pieces to test validation below
    public SnapshotRequestModel makeSnapshotRowIdsRequest() {
        SnapshotRequestRowIdTableModel snapshotRequestTableModel = new SnapshotRequestRowIdTableModel()
            .tableName("snapshot")
            .columns(Arrays.asList("col1", "col2", "col3"))
            .rowIds(Arrays.asList("row1", "row2", "row3"));

        SnapshotRequestRowIdModel rowIdSpec = new SnapshotRequestRowIdModel()
            .tables(Collections.singletonList(snapshotRequestTableModel));

        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYROWID)
            .rowIdSpec(rowIdSpec);

        return new SnapshotRequestModel()
            .contents(Collections.singletonList(snapshotRequestContentsModel));
    }

    // Generate a valid snapshot-by-rowId request, we will tweak individual pieces to test validation below
    public SnapshotRequestModel makeSnapshotByQueryRequest() {
        SnapshotRequestQueryModel querySpec = new SnapshotRequestQueryModel()
            .assetName("asset")
            .query("SELECT * FROM dataset");

        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYQUERY)
            .querySpec(querySpec);

        return new SnapshotRequestModel()
            .contents(Collections.singletonList(snapshotRequestContentsModel));
    }


    @Test
    public void testSnapshotNameInvalid() throws Exception {
        snapshotByAssetRequest.name("no spaces");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        snapshotByAssetRequest.name("no-dashes");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        snapshotByAssetRequest.name("");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        snapshotByAssetRequest.name(tooLong);
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);
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
        snapshotByAssetRequest.description(tooLongDescription);
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        snapshotByAssetRequest.description(null);
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    }

    @Test
    public void testSnapshotValuesListEmpty() throws Exception {
        SnapshotRequestAssetModel assetSpec = new SnapshotRequestAssetModel()
            .assetName("asset")
            .rootValues(Collections.emptyList());

        SnapshotRequestContentsModel snapshotRequestContentsModel = new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYASSET)
            .assetSpec(assetSpec);

        snapshotByAssetRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    }

    @Test
    public void testSnapshotDatasetNameInvalid() throws Exception {
        // snapshotByAssetRequest is assumed to be valid, we will just mess with the dataset name in the contents
        SnapshotRequestContentsModel contents = snapshotByAssetRequest.getContents().get(0);
        contents.setDatasetName("no spaces");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        contents.setDatasetName("no-dashes");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        contents.setDatasetName("");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        contents.setDatasetName(tooLong);
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    }


    @Test
    public void testSnapshotAssetNameInvalid() throws Exception {
        SnapshotRequestAssetModel assetSpec = snapshotByAssetRequest.getContents().get(0).getAssetSpec();
        assetSpec.setAssetName("no spaces");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        assetSpec.setAssetName("no-dashes");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        assetSpec.setAssetName("");
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        assetSpec.setAssetName(tooLong);
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    }

    @Test
    public void testSnapshotRowIdsEmptyColumns() throws Exception {
        SnapshotRequestRowIdModel rowIdSpec = snapshotByRowIdsRequestModel.getContents().get(0).getRowIdSpec();
        rowIdSpec.getTables().get(0).setColumns(Collections.emptyList());
        expectBadSnapshotCreateRequest(snapshotByRowIdsRequestModel);
    }

    @Test
    public void testSnapshotRowIdsEmptyRowIds() throws Exception {
        SnapshotRequestRowIdModel rowIdSpec = snapshotByRowIdsRequestModel.getContents().get(0).getRowIdSpec();
        rowIdSpec.getTables().get(0).setRowIds(Collections.emptyList());
        expectBadSnapshotCreateRequest(snapshotByRowIdsRequestModel);
    }

    @Test
    public void testSnapshotByQuery() throws Exception {
        SnapshotRequestModel querySpec = this.snapshotByQueryRequestModel;
        querySpec.getContents().get(0).getQuerySpec()
            .setQuery(null);
        expectBadSnapshotCreateRequest(snapshotByQueryRequestModel);
    }

    @Test
    public void testSnapshotNameMissing() throws Exception {
        snapshotByAssetRequest.name(null);
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    }

    @Test
    public void testMissingProfileId() throws Exception {
        snapshotByAssetRequest.profileId(null);
        expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    }
}
