package bio.terra.service.dataset;

import static bio.terra.common.fixtures.DatasetFixtures.buildAsset;
import static bio.terra.common.fixtures.DatasetFixtures.buildAssetParticipantTable;
import static bio.terra.common.fixtures.DatasetFixtures.buildAssetSampleTable;
import static bio.terra.common.fixtures.DatasetFixtures.buildDatasetRequest;
import static bio.terra.common.fixtures.DatasetFixtures.buildParticipantSampleRelationship;
import static bio.terra.common.fixtures.DatasetFixtures.buildSampleTerm;
import static bio.terra.service.dataset.ValidatorTestUtils.checkValidationErrorModel;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.controller.ApiValidationExceptionHandler;
import bio.terra.app.controller.DatasetsApiController;
import bio.terra.app.controller.GlobalExceptionHandler;
import bio.terra.app.controller.converters.EnumerateSortByParamConverter;
import bio.terra.app.controller.converters.SqlSortDirectionAscDefaultConverter;
import bio.terra.app.controller.converters.SqlSortDirectionDescDefaultConverter;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.UnitTestConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.AssetModel;
import bio.terra.model.AssetTableModel;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.model.DatePartitionOptionsModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.IntPartitionOptionsModel;
import bio.terra.model.RelationshipModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(
    classes = {
      DatasetRequestValidator.class,
      DatasetsApiController.class,
      ApiValidationExceptionHandler.class,
      GlobalExceptionHandler.class,
      EnumerateSortByParamConverter.class,
      SqlSortDirectionAscDefaultConverter.class,
      SqlSortDirectionDescDefaultConverter.class,
      UnitTestConfiguration.class
    })
@WebMvcTest
@Tag(Unit.TAG)
class DatasetRequestValidatorTest {

  @Autowired private MockMvc mvc;

  @MockBean private JobService jobService;
  @MockBean private DatasetService datasetService;
  @MockBean private IamService iamService;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private SnapshotBuilderService snapshotBuilderService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private AssetModelValidator assetModelValidator;
  @MockBean private DatasetSchemaUpdateValidator datasetSchemaUpdateValidator;
  @MockBean private DataDeletionRequestValidator dataDeletionRequestValidator;

  @BeforeEach
  void setup() throws Exception {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(dataDeletionRequestValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
    when(datasetSchemaUpdateValidator.supports(any())).thenReturn(true);
  }

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

    assertThat("Error model was returned on failure", responseBody, containsString("message"));

    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
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

    assertThat("Error model was returned on failure", responseBody, containsString("message"));

    ErrorModel errorModel = TestUtils.mapFromJson(responseBody, ErrorModel.class);
    assertThat("correct error message", errorModel.getMessage(), equalTo(expectedMessage));
    List<String> responseErrors = errorModel.getErrorDetail();
    assertThat("Error details match", responseErrors, contains(errors.toArray()));
  }

  @Test
  void testInvalidDatasetRequest() throws Exception {
    mvc.perform(
            post("/api/repository/v1/datasets")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
        .andExpect(status().is4xxClientError());
  }

  @Test
  void testJsonParsingErrors() throws Exception {
    String invalidSchema =
        """
            {"name":"no_response",
            "description":"Invalid dataset schema leads to no response body",
            "defaultProfileId":"390e7a85-d47f-4531-b612-165fc977d3bd",
            "schema":{"tables":[{"name":"table","columns":
            [{"name":"column","datatype":"fileref","is_array":true}]}]}}""";
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(invalidSchema))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();
    assertThat(
        "Json parsing errors are logged and returned",
        responseBody,
        containsString("JSON parse error: Unrecognized field \\\"is_array\\\""));
  }

  @Test
  void testDuplicateTableNames() throws Exception {
    ColumnModel column = new ColumnModel().name("id").datatype(TableDataType.STRING);
    TableModel table =
        new TableModel().name("duplicate").columns(Collections.singletonList(column));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().tables(Arrays.asList(table, table));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        "DuplicateTableNames",
        "InvalidRelationshipTermTable",
        "InvalidRelationshipTermTable",
        "InvalidAssetTable",
        "InvalidAssetTableColumn",
        "InvalidAssetTableColumn",
        "InvalidRootColumn");
  }

  /**
   * Modify and return the request so that it has a single table with the specified name, no
   * relationships, and no assets.
   */
  private DatasetRequestModel withNamedTable(DatasetRequestModel request, String tableName) {
    ColumnModel column = new ColumnModel().name("id").datatype(TableDataType.STRING);
    TableModel table = new TableModel().name(tableName).columns(List.of(column));
    request.getSchema().tables(List.of(table)).relationships(List.of()).assets(List.of());

    return request;
  }

  @Test
  void testInvalidTableName() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();

    // Table names with leading underscores are invalid
    List<String> invalidPatternNames = List.of("_", "_a_column", "_1_column");
    for (String name : invalidPatternNames) {
      checkValidationErrorModel(
          expectBadDatasetCreateRequest(withNamedTable(req, name)), "Pattern");
    }

    // Table names over 63 characters are invalid
    checkValidationErrorModel(
        expectBadDatasetCreateRequest(withNamedTable(req, "a".repeat(64))), "Size");
  }

  @Test
  void testDuplicateColumnNames() throws Exception {
    ColumnModel column = new ColumnModel().name("id").datatype(TableDataType.STRING);
    TableModel table = new TableModel().name("table").columns(Arrays.asList(column, column));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().tables(Collections.singletonList(table));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel,
        "DuplicateColumnNames",
        "InvalidRelationshipTermTable",
        "InvalidRelationshipTermTable",
        "InvalidAssetTable",
        "InvalidAssetTableColumn",
        "InvalidAssetTableColumn",
        "InvalidRootColumn");
  }

  @Test
  void testInvalidKeyType() throws Exception {
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
        "InvalidPrimaryKey",
        "InvalidPrimaryKey",
        "InvalidPrimaryKey",
        "InvalidRelationshipColumnType");
  }

  @Test
  void testInvalidColumnMode() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    TableModel testTable = req.getSchema().getTables().get(0);
    // Test that a required, array_of column causes a validation error
    // whether it is also a primary key
    ColumnModel badPrimaryKey = testTable.getColumns().get(0);
    testTable.setPrimaryKey(List.of(badPrimaryKey.getName()));
    badPrimaryKey.setArrayOf(true);
    badPrimaryKey.setRequired(true);

    ColumnModel badRequiredKey = testTable.getColumns().get(1);
    badRequiredKey.setArrayOf(true);
    badRequiredKey.setRequired(true);

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, "InvalidPrimaryKey", "InvalidColumnMode", "InvalidColumnMode");
  }

  /**
   * Modify and return the request so that it has a single table with a single column of the
   * specified name, no relationships, and no assets.
   */
  private DatasetRequestModel withNamedColumn(DatasetRequestModel request, String columnName) {
    ColumnModel column = new ColumnModel().name(columnName).datatype(TableDataType.STRING);
    TableModel table = new TableModel().name("table").columns(List.of(column));
    request.getSchema().tables(List.of(table)).relationships(List.of()).assets(List.of());

    return request;
  }

  @Test
  void testInvalidColumnName() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();

    // Table names with leading numbers or leading underscores are invalid
    List<String> invalidPatternNames = List.of("_", "_a_column", "_1_column", "1", "1_column");
    for (String name : invalidPatternNames) {
      checkValidationErrorModel(
          expectBadDatasetCreateRequest(withNamedColumn(req, name)), "Pattern");
    }

    // Column names over 63 characters are invalid
    checkValidationErrorModel(
        expectBadDatasetCreateRequest(withNamedColumn(req, "a".repeat(64))), "Size");
  }

  @Test
  void testDuplicateAssetNames() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Arrays.asList(buildAsset(), buildAsset()));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, "DuplicateAssetNames");
  }

  @Test
  void testDuplicateRelationshipNames() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    RelationshipModel relationship = buildParticipantSampleRelationship();
    req.getSchema().relationships(Arrays.asList(relationship, relationship));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, "DuplicateRelationshipNames");
  }

  @Test
  void testInvalidAssetTable() throws Exception {
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
    checkValidationErrorModel(errorModel, "NotNull", "InvalidAssetTable", "InvalidRootColumn");
  }

  @Test
  void testInvalidAssetTableColumn() throws Exception {
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
    checkValidationErrorModel(errorModel, "InvalidAssetTableColumn", "InvalidRootColumn");
  }

  @Test
  void testArrayAssetRootColumn() throws Exception {
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
    checkValidationErrorModel(errorModel, "InvalidArrayRootColumn");
  }

  @Test
  void testInvalidFollowsRelationship() throws Exception {
    AssetModel asset =
        new AssetModel()
            .name("bad_follows")
            .tables(Arrays.asList(buildAssetSampleTable(), buildAssetParticipantTable()))
            .follow(Collections.singletonList("missing"));

    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Collections.singletonList(asset));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(
        errorModel, "NotNull", "NotNull", "NoRootTable", "InvalidFollowsRelationship");
  }

  @Test
  void testInvalidRelationshipTermTableColumn() throws Exception {
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
    checkValidationErrorModel(errorModel, "InvalidRelationshipTermTableColumn");
  }

  @Test
  void testNoRootTable() throws Exception {
    AssetModel noRoot =
        new AssetModel()
            .name("bad")
            // In the fixtures, the participant asset table has isRoot set to false.
            .tables(Collections.singletonList(buildAssetParticipantTable()))
            .follow(Collections.singletonList("participant_sample"));
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema().assets(Collections.singletonList(noRoot));
    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, "NotNull", "NotNull", "NoRootTable");
  }

  @Test
  void testTableSchemaInvalidDataType() throws Exception {
    String invalidSchema = TestUtils.loadJson("./dataset/create/invalid-schema.json");
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(invalidSchema))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();
    assertThat(
        "Invalid DataTypes are logged and returned",
        responseBody,
        containsString(
            "invalid datatype in table column(s): bad_column, "
                + "DataTypes must be lowercase, valid DataTypes are [string, boolean, bytes, date, datetime, dirref, fileref, "
                + "float, float64, integer, int64, numeric, record, text, time, timestamp]"));
  }

  @Test
  void testDatasetNameInvalid() throws Exception {
    ErrorModel errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name("no spaces"));
    checkValidationErrorModel(errorModel, "Pattern");

    errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name("no-dashes"));
    checkValidationErrorModel(errorModel, "Pattern");

    errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(""));
    checkValidationErrorModel(errorModel, "Size", "Pattern");

    // Make a 512 character string, it should be considered too long by the validation.
    String tooLong = "a".repeat(512);
    errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(tooLong));
    checkValidationErrorModel(errorModel, "Size");
  }

  @Test
  void testDatasetNameMissing() throws Exception {
    ErrorModel errorModel = expectBadDatasetCreateRequest(buildDatasetRequest().name(null));
    checkValidationErrorModel(errorModel, "NotNull", "DatasetNameMissing");
  }

  @Test
  void testDatasetEnumerateValidations() throws Exception {
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
  void testMissingPrimaryKeyColumn() throws Exception {
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
    checkValidationErrorModel(errorModel, "MissingPrimaryKeyColumn", "IncompleteSchemaDefinition");
  }

  @Test
  void testNonRequiredPrimaryKeyColumn() throws Exception {
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
    checkValidationErrorModel(errorModel, "OptionalPrimaryKeyColumn");
  }

  @Test
  void testDatePartitionWithBadOptions() throws Exception {
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
        "MissingDatePartitionOptions",
        "InvalidIntPartitionOptions",
        "IncompleteSchemaDefinition");
  }

  @Test
  void testDatePartitionWithMissingColumn() throws Exception {
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
        errorModel, "InvalidDatePartitionColumnName", "IncompleteSchemaDefinition");
  }

  @Test
  void testDatePartitionWithMismatchedType() throws Exception {
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
    checkValidationErrorModel(errorModel, "InvalidDatePartitionColumnType");
  }

  @Test
  void testIntPartitionWithBadOptions() throws Exception {
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
        "InvalidDatePartitionOptions",
        "MissingIntPartitionOptions",
        "IncompleteSchemaDefinition");
  }

  @Test
  void testIntPartitionWithMissingColumn() throws Exception {
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
        errorModel, "InvalidIntPartitionColumnName", "IncompleteSchemaDefinition");
  }

  @Test
  void testIntPartitionWithMismatchedType() throws Exception {
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
    checkValidationErrorModel(errorModel, "InvalidIntPartitionColumnType");
  }

  @Test
  void testIntPartitionWithBadRange() throws Exception {
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
        errorModel, "InvalidIntPartitionRange", "InvalidIntPartitionInterval");
  }

  @Test
  void testIntPartitionTooManyPartitions() throws Exception {
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
    checkValidationErrorModel(errorModel, "TooManyIntPartitions");
  }

  @Test
  void testPartitionOptionsWithoutMode() throws Exception {
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
        "InvalidDatePartitionOptions",
        "InvalidIntPartitionOptions",
        "IncompleteSchemaDefinition");
  }

  @Test
  void testNoTablesProvided() throws Exception {
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.emptyList())
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, "IncompleteSchemaDefinition");
  }

  @Test
  void testNoColumnsProvided() throws Exception {
    TableModel table = new TableModel().name("table").columns(Collections.emptyList());
    DatasetRequestModel req = buildDatasetRequest();
    req.getSchema()
        .tables(Collections.singletonList(table))
        .relationships(Collections.emptyList())
        .assets(Collections.emptyList());

    ErrorModel errorModel = expectBadDatasetCreateRequest(req);
    checkValidationErrorModel(errorModel, "IncompleteSchemaDefinition");
  }
}
