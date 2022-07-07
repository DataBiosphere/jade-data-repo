package bio.terra.service.dataset;

import static bio.terra.common.fixtures.DatasetFixtures.buildAsset;
import static bio.terra.common.fixtures.DatasetFixtures.buildAssetParticipantTable;
import static bio.terra.common.fixtures.DatasetFixtures.buildAssetSampleTable;
import static bio.terra.common.fixtures.DatasetFixtures.buildDatasetRequest;
import static bio.terra.common.fixtures.DatasetFixtures.buildParticipantSampleRelationship;
import static bio.terra.common.fixtures.DatasetFixtures.buildSampleTerm;
import static bio.terra.service.dataset.ValidatorTestUtils.checkValidationErrorModel;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatePartitionOptionsModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IntPartitionOptionsModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DatasetRequestValidatorTest {

  @Autowired private MockMvc mvc;

  @Autowired private ObjectMapper objectMapper;

  @MockBean private DatasetService datasetService;

  @Autowired private JsonLoader jsonLoader;

  private ErrorModel expectBadDatasetCreateRequest(DatasetRequestModel datasetRequest)
      throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(datasetRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();

    assertTrue(
        "Error model was returned on failure", StringUtils.contains(responseBody, "message"));

    ErrorModel errorModel = TestUtils.mapFromJson(responseBody, ErrorModel.class);
    return errorModel;
  }

  private void expectBadDatasetEnumerateRequest(
      Integer offset,
      Integer limit,
      String sort,
      String direction,
      String expectedMessage,
      List<String> errors)
      throws Exception {
    MvcResult result =
        mvc.perform(
                get("/api/repository/v1/datasets")
                    .param("offset", offset.toString())
                    .param("limit", limit.toString())
                    .param("sort", sort)
                    .param("direction", direction)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(buildDatasetRequest())))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();

    assertTrue(
        "Error model was returned on failure", StringUtils.contains(responseBody, "message"));

    ErrorModel errorModel = TestUtils.mapFromJson(responseBody, ErrorModel.class);
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
    mvc.perform(
            post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().is4xxClientError());
  }

  @Test
  public void testJsonParsingErrors() throws Exception {
    String invalidSchema =
        "{\"name\":\"no_response\","
            + "\"description\":\"Invalid dataset schema leads to no response body\","
            + "\"defaultProfileId\":\"390e7a85-d47f-4531-b612-165fc977d3bd\","
            + "\"schema\":{\"tables\":[{\"name\":\"table\",\"columns\":"
            + "[{\"name\":\"column\",\"datatype\":\"fileref\",\"is_array\":true}]}]}}";
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(invalidSchema))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();
    assertTrue(
        "Json parsing errors are logged and returned",
        responseBody.contains("JSON parse error: Unrecognized field \\\"is_array\\\""));
  }

  @Test
  public void testDuplicateTableNames() throws Exception {
    ColumnModel column = new ColumnModel().name("id").datatype(TableDataType.STRING);
    TableModel table =
        new TableModel().name("duplicate").columns(Collections.singletonList(column));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().tables(Arrays.asList(table, table));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        new String[] {
          "DuplicateTableNames",
          "InvalidRelationshipTermTableColumn",
          "InvalidRelationshipTermTableColumn",
          "InvalidAssetTable",
          "InvalidAssetTableColumn",
          "InvalidAssetTableColumn",
          "InvalidRootColumn"
        });
  }

  @Test
  public void testDuplicateColumnNames() throws Exception {
    ColumnModel column = new ColumnModel().name("id").datatype(TableDataType.STRING);
    TableModel table = new TableModel().name("table").columns(Arrays.asList(column, column));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().tables(Collections.singletonList(table));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        new String[] {
          "DuplicateColumnNames",
          "InvalidRelationshipTermTableColumn",
          "InvalidRelationshipTermTableColumn",
          "InvalidAssetTable",
          "InvalidAssetTableColumn",
          "InvalidAssetTableColumn",
          "InvalidRootColumn"
        });
  }

  @Test
  public void testInvalidKeyType() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    TableModel testTable = req.getSchema().getTables().get(0);
    testTable.setPrimaryKey(List.of("id", "age"));

    ColumnModel badColumnFileRefArray = testTable.getColumns().get(0);
    badColumnFileRefArray.setArrayOf(true);
    badColumnFileRefArray.setDatatype(TableDataType.FILEREF);

    ColumnModel badColumnDirref = testTable.getColumns().get(1);
    badColumnDirref.setDatatype(TableDataType.DIRREF);

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        new String[] {
          "InvalidPrimaryKey", "InvalidPrimaryKey", "InvalidPrimaryKey", "InvalidForeignKey"
        });
  }

  @Test
  public void testInvalidColumnMode() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    TableModel testTable = req.getSchema().getTables().get(0);
    // Test that a required, array_of column causes a validation error
    ColumnModel badColumn = testTable.getColumns().get(0);
    badColumn.setArrayOf(true);
    badColumn.setRequired(true);

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"InvalidColumnMode"});
  }

  @Test
  public void testDuplicateAssetNames() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Arrays.asList(buildAsset(), buildAsset()));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"DuplicateAssetNames"});
  }

  @Test
  public void testDuplicateRelationshipNames() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    RelationshipModel relationship = buildParticipantSampleRelationship();
    req.getSchema().relationships(Arrays.asList(relationship, relationship));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"DuplicateRelationshipNames"});
  }

  @Test
  public void testInvalidAssetTable() throws Exception {
    AssetTableModel invalidAssetTable =
        new AssetTableModel().name("mismatched_table_name").columns(Collections.emptyList());

    AssetModel asset =
        new AssetModel()
            .name("bad_asset")
            .rootTable("mismatched_table_name")
            .tables(Collections.singletonList(invalidAssetTable))
            .follow(Collections.singletonList("participant_sample"));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Collections.singletonList(asset));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, new String[] {"NotNull", "InvalidAssetTable", "InvalidRootColumn"});
  }

  @Test
  public void testInvalidAssetTableColumn() throws Exception {
    // participant is a valid table but date_collected is in the sample table
    AssetTableModel invalidAssetTable =
        new AssetTableModel()
            .name("participant")
            .columns(Collections.singletonList("date_collected"));

    AssetModel asset =
        new AssetModel()
            .name("mismatched")
            .rootTable("participant")
            .rootColumn("date_collected")
            .tables(Collections.singletonList(invalidAssetTable))
            .follow(Collections.singletonList("participant_sample"));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Collections.singletonList(asset));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, new String[] {"InvalidAssetTableColumn", "InvalidRootColumn"});
  }

  @Test
  public void testArrayAssetRootColumn() throws Exception {
    ColumnModel arrayColumn =
        new ColumnModel().name("array_data").arrayOf(true).datatype(TableDataType.STRING);

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().getTables().stream()
        .filter(table -> table.getName().equals("sample"))
        .findFirst()
        .ifPresent(
            sampleTable -> {
              ArrayList<ColumnModel> columns = new ArrayList<>(sampleTable.getColumns());
              columns.add(arrayColumn);
              sampleTable.setColumns(columns);
            });

    AssetTableModel assetTable =
        new AssetTableModel().name("sample").columns(Collections.emptyList());

    AssetModel asset =
        new AssetModel()
            .name("bad_root")
            .rootTable("sample")
            .rootColumn(arrayColumn.getName())
            .tables(Collections.singletonList(assetTable))
            .follow(Collections.singletonList("participant_sample"));

    req.getSchema().setAssets(Collections.singletonList(asset));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"InvalidArrayRootColumn"});
  }

  @Test
  public void testInvalidFollowsRelationship() throws Exception {
    AssetModel asset =
        new AssetModel()
            .name("bad_follows")
            .tables(Arrays.asList(buildAssetSampleTable(), buildAssetParticipantTable()))
            .follow(Collections.singletonList("missing"));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Collections.singletonList(asset));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        new String[] {"NotNull", "NotNull", "NoRootTable", "InvalidFollowsRelationship"});
  }

  @Test
  public void testInvalidRelationshipTermTableColumn() throws Exception {
    // participant_id is part of the sample table, not participant
    RelationshipTermModel mismatchedTerm =
        new RelationshipTermModel().table("participant").column("participant_id");

    RelationshipModel mismatchedRelationship =
        new RelationshipModel()
            .name("participant_sample")
            .from(mismatchedTerm)
            .to(buildSampleTerm());

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().relationships(Collections.singletonList(mismatchedRelationship));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"InvalidRelationshipTermTableColumn"});
  }

  @Test
  public void testNoRootTable() throws Exception {
    AssetModel noRoot =
        new AssetModel()
            .name("bad")
            // In the fixtures, the participant asset table has isRoot set to false.
            .tables(Collections.singletonList(buildAssetParticipantTable()))
            .follow(Collections.singletonList("participant_sample"));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Collections.singletonList(noRoot));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"NotNull", "NotNull", "NoRootTable"});
  }

  @Test
  public void testTableSchemaInvalidDataType() throws Exception {
    String invalidSchema = jsonLoader.loadJson("./dataset/create/invalid-schema.json");
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(invalidSchema))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();
    assertTrue(
        "Invalid DataTypes are logged and returned",
        responseBody.contains(
            "invalid datatype in table column(s): bad_column, "
                + "DataTypes must be lowercase, valid DataTypes are [string, boolean, bytes, date, datetime, dirref, fileref, "
                + "float, float64, integer, int64, numeric, record, text, time, timestamp]"));
  }

  @Test
  public void testDatasetNameInvalid() throws Exception {
    ErrorModel errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name("no spaces"));
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name("no-dashes"));
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(""));
    checkValidationErrorModel(errorModel, new String[] {"Size", "Pattern"});

    // Make a 512 character string, it should be considered too long by the validation.
    String tooLong = StringUtils.repeat("a", 512);
    errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(tooLong));
    checkValidationErrorModel(errorModel, new String[] {"Size"});
  }

  @Test
  public void testDatasetNameMissing() throws Exception {
    ErrorModel errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(null));
    checkValidationErrorModel(errorModel, new String[] {"NotNull", "DatasetNameMissing"});
  }

  @Test
  public void testDatasetEnumerateValidations() throws Exception {
    String expected = "Invalid enumerate parameter(s).";
    String expectedEnum = "Invalid enum parameter: %s.";
    expectBadDatasetEnumerateRequest(
        -1,
        3,
        null,
        null,
        expected,
        Collections.singletonList("offset must be greater than or equal to 0."));
    expectBadDatasetEnumerateRequest(
        1,
        0,
        null,
        null,
        expected,
        Collections.singletonList("limit must be greater than or equal to 1."));
    expectBadDatasetEnumerateRequest(
        -1,
        0,
        null,
        null,
        expected,
        Arrays.asList(
            "offset must be greater than or equal to 0.",
            "limit must be greater than or equal to 1."));
    expectBadDatasetEnumerateRequest(
        0,
        10,
        "invalid",
        null,
        String.format(expectedEnum, "invalid"),
        Collections.singletonList("sort must be one of: [name, description, created_date]."));
    expectBadDatasetEnumerateRequest(
        0,
        10,
        "name",
        "invalid",
        String.format(expectedEnum, "invalid"),
        Collections.singletonList("direction must be one of: [asc, desc]."));
  }

  @Test
  public void testMissingPrimaryKeyColumn() throws Exception {
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.emptyList())
            .primaryKey(Collections.singletonList("not_a_column"));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, new String[] {"MissingPrimaryKeyColumn", "IncompleteSchemaDefinition"});
  }

  @Test
  public void testNonRequiredPrimaryKeyColumn() throws Exception {
    TableModel table =
        new TableModel()
            .name("table")
            .columns(
                List.of(
                    new ColumnModel()
                        .name("pkColumn")
                        .datatype(TableDataType.STRING)
                        .required(false)))
            .primaryKey(Collections.singletonList("pkColumn"));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"OptionalPrimaryKeyColumn"});
  }

  @Test
  public void testDatePartitionWithBadOptions() throws Exception {
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.emptyList())
            .partitionMode(TableModel.PartitionModeEnum.DATE)
            .intPartitionOptions(
                new IntPartitionOptionsModel().column("foo").min(1L).max(2L).interval(1L));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        new String[] {
          "MissingDatePartitionOptions", "InvalidIntPartitionOptions", "IncompleteSchemaDefinition"
        });
  }

  @Test
  public void testDatePartitionWithMissingColumn() throws Exception {
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.emptyList())
            .partitionMode(TableModel.PartitionModeEnum.DATE)
            .datePartitionOptions(new DatePartitionOptionsModel().column("not_a_column"));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, new String[] {"InvalidDatePartitionColumnName", "IncompleteSchemaDefinition"});
  }

  @Test
  public void testDatePartitionWithMismatchedType() throws Exception {
    ColumnModel column = new ColumnModel().name("column").datatype(TableDataType.INT64);
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.singletonList(column))
            .partitionMode(TableModel.PartitionModeEnum.DATE)
            .datePartitionOptions(new DatePartitionOptionsModel().column(column.getName()));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"InvalidDatePartitionColumnType"});
  }

  @Test
  public void testIntPartitionWithBadOptions() throws Exception {
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.emptyList())
            .partitionMode(TableModel.PartitionModeEnum.INT)
            .datePartitionOptions(new DatePartitionOptionsModel().column("foo"));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        new String[] {
          "InvalidDatePartitionOptions", "MissingIntPartitionOptions", "IncompleteSchemaDefinition"
        });
  }

  @Test
  public void testIntPartitionWithMissingColumn() throws Exception {
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.emptyList())
            .partitionMode(TableModel.PartitionModeEnum.INT)
            .intPartitionOptions(
                new IntPartitionOptionsModel().column("not_a_column").min(1L).max(2L).interval(1L));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, new String[] {"InvalidIntPartitionColumnName", "IncompleteSchemaDefinition"});
  }

  @Test
  public void testIntPartitionWithMismatchedType() throws Exception {
    ColumnModel column = new ColumnModel().name("column").datatype(TableDataType.TIMESTAMP);
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.singletonList(column))
            .partitionMode(TableModel.PartitionModeEnum.INT)
            .intPartitionOptions(
                new IntPartitionOptionsModel()
                    .column(column.getName())
                    .min(1L)
                    .max(2L)
                    .interval(1L));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"InvalidIntPartitionColumnType"});
  }

  @Test
  public void testIntPartitionWithBadRange() throws Exception {
    ColumnModel column = new ColumnModel().name("column").datatype(TableDataType.INT64);
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.singletonList(column))
            .partitionMode(TableModel.PartitionModeEnum.INT)
            .intPartitionOptions(
                new IntPartitionOptionsModel()
                    .column(column.getName())
                    .min(5L)
                    .max(2L)
                    .interval(-1L));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, new String[] {"InvalidIntPartitionRange", "InvalidIntPartitionInterval"});
  }

  @Test
  public void testIntPartitionTooManyPartitions() throws Exception {
    ColumnModel column = new ColumnModel().name("column").datatype(TableDataType.INT64);
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.singletonList(column))
            .partitionMode(TableModel.PartitionModeEnum.INT)
            .intPartitionOptions(
                new IntPartitionOptionsModel()
                    .column(column.getName())
                    .min(0L)
                    .max(4001L)
                    .interval(1L));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"TooManyIntPartitions"});
  }

  @Test
  public void testPartitionOptionsWithoutMode() throws Exception {
    TableModel table =
        new TableModel()
            .name("table")
            .columns(Collections.emptyList())
            .datePartitionOptions(new DatePartitionOptionsModel().column("foo"))
            .intPartitionOptions(
                new IntPartitionOptionsModel().column("bar").min(1L).max(2L).interval(1L));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        new String[] {
          "InvalidDatePartitionOptions", "InvalidIntPartitionOptions", "IncompleteSchemaDefinition"
        });
  }

  @Test
  public void testAzureIngestRequestParameters() throws Exception {
    Dataset dataset = mock(Dataset.class);
    DatasetSummary datasetSummary = mock(DatasetSummary.class);
    when(datasetSummary.getStorageCloudPlatform()).thenReturn(CloudPlatform.AZURE);
    when(dataset.getDatasetSummary()).thenReturn(datasetSummary);
    when(datasetService.retrieve(any())).thenReturn(dataset);

    var nullIngest =
        new IngestRequestModel()
            .path("foo/bar")
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(null)
            .csvFieldDelimiter(null)
            .csvQuote(null);

    var nullResult =
        mvc.perform(
                post(String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(nullIngest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse nullResponse = nullResult.getResponse();
    String nullResponseBody = nullResponse.getContentAsString();
    ErrorModel nullErrorModel = TestUtils.mapFromJson(nullResponseBody, ErrorModel.class);
    assertThat(
        "Validation catches all null parameters", nullErrorModel.getErrorDetail(), hasSize(3));
    for (String error : nullErrorModel.getErrorDetail()) {
      assertThat("Validation catches null parameters", error, containsString("defined"));
    }

    var invalidIngest =
        new IngestRequestModel()
            .path("foo/bar")
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.CSV)
            .csvSkipLeadingRows(-1)
            .csvFieldDelimiter("toolong")
            .csvQuote("toolong");

    var invalidResult =
        mvc.perform(
                post(String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(invalidIngest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse invalidResponse = invalidResult.getResponse();
    String invalidResponseBody = invalidResponse.getContentAsString();
    ErrorModel invalidErrorModel = TestUtils.mapFromJson(invalidResponseBody, ErrorModel.class);
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(3));
    var csvSkipLeadingRowsError = invalidErrorModel.getErrorDetail().get(0);
    var csvFieldDelimiterError = invalidErrorModel.getErrorDetail().get(1);
    var csvQuoteError = invalidErrorModel.getErrorDetail().get(2);

    assertThat(
        "Validator catches invalid 'csvSkipLeadingRows'",
        csvSkipLeadingRowsError,
        containsString("'csvSkipLeadingRows' must be a positive integer, was '-1."));
    assertThat(
        "Validator catches invalid 'csvFieldDelimiter'",
        csvFieldDelimiterError,
        containsString("'csvFieldDelimiter' must be a single character, was 'toolong'."));
    assertThat(
        "Validator catches invalid 'csvQuote'",
        csvQuoteError,
        containsString("'csvQuote' must be a single character, was 'toolong'."));
  }

  @Test
  public void testInvalidIngestByArray() throws Exception {
    var invalidIngest =
        new IngestRequestModel()
            .path("foo/bar")
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.ARRAY);

    var invalidResult =
        mvc.perform(
                post(String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(invalidIngest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse invalidResponse = invalidResult.getResponse();
    String invalidResponseBody = invalidResponse.getContentAsString();
    ErrorModel invalidErrorModel = TestUtils.mapFromJson(invalidResponseBody, ErrorModel.class);
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(2));
    var pathIsPresentError = invalidErrorModel.getErrorDetail().get(0);
    var payloadIsMissingError = invalidErrorModel.getErrorDetail().get(1);

    assertThat(
        "Validator catches invalid 'path' and 'format' combo",
        pathIsPresentError,
        containsString("Path should not be specified when ingesting from an array"));
    assertThat(
        "Validator catches invalid 'format' and 'records' combo",
        payloadIsMissingError,
        containsString("Records is required when ingesting as an array"));
  }

  @Test
  public void testInvalidIngestByPath() throws Exception {
    var invalidIngest =
        new IngestRequestModel()
            .table("myTable")
            .format(IngestRequestModel.FormatEnum.JSON)
            .addRecordsItem(Map.of("foo", "bar"));

    var invalidResult =
        mvc.perform(
                post(String.format("/api/repository/v1/datasets/%s/ingest", UUID.randomUUID()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(invalidIngest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse invalidResponse = invalidResult.getResponse();
    String invalidResponseBody = invalidResponse.getContentAsString();
    ErrorModel invalidErrorModel = TestUtils.mapFromJson(invalidResponseBody, ErrorModel.class);
    assertThat(
        "Validation catches all invalid parameters",
        invalidErrorModel.getErrorDetail(),
        hasSize(2));
    var pathIsMissingError = invalidErrorModel.getErrorDetail().get(0);
    var payloadIsPresentError = invalidErrorModel.getErrorDetail().get(1);

    assertThat(
        "Validator catches invalid 'path' and 'format' combo",
        pathIsMissingError,
        containsString("Path is required when ingesting from a cloud object"));
    assertThat(
        "Validator catches invalid 'records' and 'format' combo",
        payloadIsPresentError,
        containsString("Records should not be specified when ingesting from a path"));
  }

  @Test
  public void testNoTablesProvided() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.emptyList())
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"IncompleteSchemaDefinition"});
  }

  @Test
  public void testNoColumnsProvided() throws Exception {
    TableModel table = new TableModel().name("table").columns(Collections.emptyList());
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, new String[] {"IncompleteSchemaDefinition"});
  }
}
