package bio.terra.app.controller;

import bio.terra.category.Unit;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.TableModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static bio.terra.common.fixtures.DatasetFixtures.buildAsset;
import static bio.terra.common.fixtures.DatasetFixtures.buildAssetParticipantTable;
import static bio.terra.common.fixtures.DatasetFixtures.buildAssetSampleTable;
import static bio.terra.common.fixtures.DatasetFixtures.buildParticipantSampleRelationship;
import static bio.terra.common.fixtures.DatasetFixtures.buildSampleTerm;
import static bio.terra.common.fixtures.DatasetFixtures.buildDatasetRequest;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetValidationsTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    private ObjectMapper objectMapper;

    private ErrorModel expectBadDatasetCreateRequest(DatasetRequestModel datasetRequest) throws Exception {
        MvcResult result = mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(datasetRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

        MockHttpServletResponse response = result.getResponse();
        String responseBody = response.getContentAsString();

        assertTrue("Error model was returned on failure",
            StringUtils.contains(responseBody, "message"));

        ErrorModel errorModel = objectMapper.readValue(responseBody, ErrorModel.class);
        return errorModel;
    }

    private void expectBadDatasetEnumerateRequest(
        Integer offset,
        Integer limit,
        String sort,
        String direction,
        String expectedMessage,
        List<String> errors
    ) throws Exception {
        MvcResult result = mvc.perform(get("/api/repository/v1/datasets")
            .param("offset", offset.toString())
            .param("limit", limit.toString())
            .param("sort", sort)
            .param("direction", direction)
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(buildDatasetRequest())))
            .andExpect(status().is4xxClientError())
            .andReturn();

        MockHttpServletResponse response = result.getResponse();
        String responseBody = response.getContentAsString();

        assertTrue("Error model was returned on failure",
            StringUtils.contains(responseBody, "message"));

        ErrorModel errorModel = objectMapper.readValue(responseBody, ErrorModel.class);
        assertThat("correct error message", errorModel.getMessage(), equalTo(expectedMessage));
        List<String> responseErrors = errorModel.getErrorDetail();
        if (errors == null || errors.isEmpty()) {
            assertTrue("No details expected", (responseErrors == null || responseErrors.size() == 0));
        } else {
            assertTrue("Same number of errors", responseErrors.size() == errors.size());
            assertArrayEquals("Error details match", responseErrors.toArray(), errors.toArray());
        }
    }

    @Test
    public void testInvalidDatasetRequest() throws Exception {
        mvc.perform(post("/api/repository/v1/datasets")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{}"))
            .andExpect(status().is4xxClientError());
    }

    @Test
    public void testDuplicateTableNames() throws Exception {
        ColumnModel column = new ColumnModel().name("id").datatype("string");
        TableModel table = new TableModel()
            .name("duplicate")
            .columns(Collections.singletonList(column));

        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().tables(Arrays.asList(table, table));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("duplicateTableNames", errorModel,
            new String[]{"DuplicateTableNames", "InvalidRelationshipTermTableColumn",
                "InvalidRelationshipTermTableColumn", "InvalidAssetTable",
                "InvalidAssetTableColumn", "InvalidAssetTableColumn", "InvalidRootColumn"});
    }

    @Test
    public void testDuplicateColumnNames() throws Exception {
        ColumnModel column = new ColumnModel().name("id").datatype("string");
        TableModel table = new TableModel()
            .name("table")
            .columns(Arrays.asList(column, column));

        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().tables(Collections.singletonList(table));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("duplicateColumnNames", errorModel,
            new String[]{"DuplicateColumnNames", "InvalidRelationshipTermTableColumn",
                "InvalidRelationshipTermTableColumn", "InvalidAssetTable",
                "InvalidAssetTableColumn", "InvalidAssetTableColumn", "InvalidRootColumn"});
    }

    @Test
    public void testMissingAssets() throws Exception {
        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().assets(null);
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("missingAssets", errorModel,
            new String[]{"NotNull"});

        req.getSchema().assets(Collections.emptyList());
        errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("missingAssets", errorModel,
            new String[]{"NoAssets"});
    }

    @Test
    public void testDuplicateAssetNames() throws Exception {
        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().assets(Arrays.asList(buildAsset(), buildAsset()));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("duplicateAssetNames", errorModel,
            new String[]{"DuplicateAssetNames"});
    }

    @Test
    public void testDuplicateRelationshipNames() throws Exception {
        DatasetRequestModel req = buildDatasetRequest();
        RelationshipModel relationship = buildParticipantSampleRelationship();
        req.getSchema().relationships(Arrays.asList(relationship, relationship));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("duplicateRelationshipNames", errorModel,
            new String[]{"DuplicateRelationshipNames"});
    }

    @Test
    public void testInvalidAssetTable() throws Exception {
        AssetTableModel invalidAssetTable = new AssetTableModel()
            .name("mismatched_table_name")
            .columns(Collections.emptyList());

        AssetModel asset = new AssetModel()
            .name("bad_asset")
            .rootTable("mismatched_table_name")
            .tables(Collections.singletonList(invalidAssetTable))
            .follow(Collections.singletonList("participant_sample"));

        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().assets(Collections.singletonList(asset));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("invalidAssetTable", errorModel,
            new String[]{"NotNull", "InvalidAssetTable", "InvalidRootColumn"});
    }

    @Test
    public void testInvalidAssetTableColumn() throws Exception {
        // participant is a valid table but date_collected is in the sample table
        AssetTableModel invalidAssetTable = new AssetTableModel()
            .name("participant")
            .columns(Collections.singletonList("date_collected"));

        AssetModel asset = new AssetModel()
            .name("mismatched")
            .rootTable("participant")
            .rootColumn("date_collected")
            .tables(Collections.singletonList(invalidAssetTable))
            .follow(Collections.singletonList("participant_sample"));

        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().assets(Collections.singletonList(asset));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("invalidAssetTableColumn", errorModel,
            new String[]{"InvalidAssetTableColumn", "InvalidRootColumn"});
    }

    @Test
    public void testInvalidFollowsRelationship() throws Exception {
        AssetModel asset = new AssetModel()
            .name("bad_follows")
            .tables(Arrays.asList(buildAssetSampleTable(), buildAssetParticipantTable()))
            .follow(Collections.singletonList("missing"));

        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().assets(Collections.singletonList(asset));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("invalidFollowsRelationship", errorModel,
            new String[]{"NotNull", "NotNull", "NoRootTable", "InvalidFollowsRelationship"});
    }

    @Test
    public void testInvalidRelationshipTermTableColumn() throws Exception {
        // participant_id is part of the sample table, not participant
        RelationshipTermModel mismatchedTerm = new RelationshipTermModel()
            .table("participant")
            .column("participant_id")
            .cardinality(RelationshipTermModel.CardinalityEnum.ONE);

        RelationshipModel mismatchedRelationship = new RelationshipModel()
            .name("participant_sample")
            .from(mismatchedTerm)
            .to(buildSampleTerm());

        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().relationships(Collections.singletonList(mismatchedRelationship));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("invalidRelationshipTermTableColumn", errorModel,
            new String[]{"InvalidRelationshipTermTableColumn"});
    }


    @Test
    public void testNoRootTable() throws Exception {
        AssetModel noRoot = new AssetModel()
            .name("bad")
            // In the fixtures, the participant asset table has isRoot set to false.
            .tables(Collections.singletonList(buildAssetParticipantTable()))
            .follow(Collections.singletonList("participant_sample"));
        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema().assets(Collections.singletonList(noRoot));
        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("noRootTable", errorModel, new String[]{"NotNull", "NotNull", "NoRootTable"});
    }

    @Test
    public void testDatasetNameInvalid() throws Exception {
        ErrorModel errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name("no spaces"));
        checkValidationErrorModel("datasetNameInvalid", errorModel, new String[]{"DatasetNameInvalid"});

        errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name("no-dashes"));
        checkValidationErrorModel("datasetNameInvalid", errorModel, new String[]{"DatasetNameInvalid"});

        errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(""));
        checkValidationErrorModel("datasetNameInvalid", errorModel, new String[]{"DatasetNameInvalid"});

        // Make a 64 character string, it should be considered too long by the validation.
        String tooLong = StringUtils.repeat("a", 64);
        errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(tooLong));
        checkValidationErrorModel("datasetNameInvalid", errorModel, new String[]{"DatasetNameInvalid"});
    }

    @Test
    public void testDatasetNameMissing() throws Exception {
        ErrorModel errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(null));
        checkValidationErrorModel("datasetNameMissing", errorModel,
            new String[]{"NotNull", "DatasetNameMissing"});
    }

    @Test
    public void testDatasetEnumerateValidations() throws Exception {
        String expected = "Invalid enumerate parameter(s).";
        expectBadDatasetEnumerateRequest(-1, 3, null, null, expected,
            Collections.singletonList("offset must be greater than or equal to 0."));
        expectBadDatasetEnumerateRequest(1, 0, null, null, expected,
            Collections.singletonList("limit must be greater than or equal to 1."));
        expectBadDatasetEnumerateRequest(-1, 0, null, null, expected,
            Arrays.asList("offset must be greater than or equal to 0.", "limit must be greater than or equal to 1."));
        expectBadDatasetEnumerateRequest(0, 10, "invalid", null, expected,
            Collections.singletonList("sort must be one of: (name, description, created_date)."));
        expectBadDatasetEnumerateRequest(0, 10, "name", "invalid", expected,
            Collections.singletonList("direction must be one of: (asc, desc)."));
    }

    @Test
    public void testMissingPrimaryKeyColumn() throws Exception {
        TableModel table = new TableModel()
            .name("table")
            .columns(Collections.emptyList())
            .primaryKey(Collections.singletonList("not_a_column"));
        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema()
            .tables(Collections.singletonList(table))
            .relationships(Collections.emptyList())
            .assets(Collections.emptyList());

        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("primaryKeyColumnMissing", errorModel,
            new String[]{"MissingPrimaryKeyColumn", "NoAssets"});
    }

    @Test
    public void testArrayPrimaryKeyColumn() throws Exception {
        ColumnModel column = new ColumnModel()
            .name("array_column")
            .datatype("string")
            .arrayOf(true);
        TableModel table = new TableModel()
            .name("table")
            .columns(Collections.singletonList(column))
            .primaryKey(Collections.singletonList(column.getName()));
        DatasetRequestModel req = buildDatasetRequest();
        req.getSchema()
            .tables(Collections.singletonList(table))
            .relationships(Collections.emptyList())
            .assets(Collections.emptyList());

        ErrorModel errorModel = expectBadDatasetCreateRequest(req);
        checkValidationErrorModel("primaryKeyColumnMissing", errorModel,
            new String[]{"PrimaryKeyArrayColumn", "NoAssets"});
    }

    private void checkValidationErrorModel(String context,
                                           ErrorModel errorModel,
                                           String[] messageCodes) {
        List<String> details = errorModel.getErrorDetail();
        int requiredDetailSize = messageCodes.length;
        assertThat("Got the expected error details", details.size(), equalTo(requiredDetailSize));
        assertThat("Main message is right",
            errorModel.getMessage(), containsString("Validation errors - see error details"));
        for (int i = 0; i < messageCodes.length; i++) {
            String code = messageCodes[i];
            assertThat(context + ": correct message code (" + i + ")",
                /**
                 * The global exception handler logs in this format:
                 *
                 * <fieldName>: '<messageCode>' (<defaultMessage>)
                 *
                 * We check to see if the code is wrapped in quotes to prevent matching on substrings.
                 */
                details.get(i), containsString("'" + messageCodes[i] + "'"));
        }
    }
}
