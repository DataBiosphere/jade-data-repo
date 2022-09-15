package bio.terra.app.controller;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.ErrorModel;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
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
public class SnapshotValidationTest {

  @Autowired private MockMvc mvc;

  @Autowired private ObjectMapper objectMapper;

  private SnapshotRequestModel snapshotByAssetRequest;

  private SnapshotRequestModel snapshotByRowIdsRequestModel;

  private SnapshotRequestModel snapshotByQueryRequestModel;

  @Before
  public void setup() {
    snapshotByAssetRequest = makeSnapshotAssetRequest();
    snapshotByRowIdsRequestModel = makeSnapshotRowIdsRequest();
    snapshotByQueryRequestModel = makeSnapshotByQueryRequest();
  }

  private ErrorModel expectBadSnapshotCreateRequest(SnapshotRequestModel snapshotRequest)
      throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/snapshots")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(snapshotRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();

    assertTrue(
        "Error model was returned on failure", StringUtils.contains(responseBody, "message"));

    ErrorModel errorModel = TestUtils.mapFromJson(responseBody, ErrorModel.class);
    return errorModel;
  }

  // Generate a valid snapshot-by-asset request, we will tweak individual pieces to test validation
  // below
  public SnapshotRequestModel makeSnapshotAssetRequest() {
    SnapshotRequestAssetModel assetSpec =
        new SnapshotRequestAssetModel()
            .assetName("asset")
            .rootValues(Arrays.asList("sample 1", "sample 2", "sample 3"));

    SnapshotRequestContentsModel snapshotRequestContentsModel =
        new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYASSET)
            .assetSpec(assetSpec);

    return new SnapshotRequestModel()
        .name("snapshot")
        .description("snapshot description")
        .profileId(UUID.randomUUID())
        .addContentsItem(snapshotRequestContentsModel);
  }

  // Generate a valid snapshot-by-rowId request, we will tweak individual pieces to test validation
  // below
  public SnapshotRequestModel makeSnapshotRowIdsRequest() {
    SnapshotRequestRowIdTableModel snapshotRequestTableModel =
        new SnapshotRequestRowIdTableModel()
            .tableName("snapshot")
            .columns(Arrays.asList("col1", "col2", "col3"))
            .rowIds(Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()));

    SnapshotRequestRowIdModel rowIdSpec =
        new SnapshotRequestRowIdModel()
            .tables(Collections.singletonList(snapshotRequestTableModel));

    SnapshotRequestContentsModel snapshotRequestContentsModel =
        new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYROWID)
            .rowIdSpec(rowIdSpec);

    return new SnapshotRequestModel()
        .name("snapshot")
        .description("snapshot description")
        .profileId(UUID.randomUUID())
        .contents(Collections.singletonList(snapshotRequestContentsModel));
  }

  // Generate a valid snapshot-by-query request, we will tweak individual pieces to test validation
  // below
  public SnapshotRequestModel makeSnapshotByQueryRequest() {
    SnapshotRequestQueryModel querySpec =
        new SnapshotRequestQueryModel().assetName("asset").query("SELECT * FROM dataset");

    SnapshotRequestContentsModel snapshotRequestContentsModel =
        new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYQUERY)
            .querySpec(querySpec);

    return new SnapshotRequestModel()
        .name("snapshot")
        .description("snapshot description")
        .profileId(UUID.randomUUID())
        .contents(Collections.singletonList(snapshotRequestContentsModel));
  }

  // Generate a valid snapshot-by-fullView request, we will tweak individual pieces to test
  // validation below
  public SnapshotRequestModel makeSnapshotByFullViewRequest() {
    SnapshotRequestContentsModel snapshotRequestContentsModel =
        new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYFULLVIEW);

    return new SnapshotRequestModel()
        .name("snapshot")
        .description("snapshot description")
        .profileId(UUID.randomUUID())
        .contents(Collections.singletonList(snapshotRequestContentsModel));
  }

  @Test
  public void testSnapshotNameInvalid() throws Exception {
    snapshotByAssetRequest.name("no spaces");
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    snapshotByAssetRequest.name("no-dashes");
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    snapshotByAssetRequest.name("");
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Size", "Pattern"});

    // Make a 512 character string, it should be considered too long by the validation.
    // Note: a 511 character string, we are okay with
    String tooLong = StringUtils.repeat("a", 512);
    snapshotByAssetRequest.name(tooLong);
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Size"});
  }

  @Test
  public void testSnapshotDescriptionInvalid() throws Exception {
    String tooLong = StringUtils.repeat("a", 2048);
    snapshotByAssetRequest.description(tooLong);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotDescriptionTooLong"});

    snapshotByAssetRequest.description(null);
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotDescriptionMissing"});
  }

  @Test
  public void testSnapshotValuesListEmpty() throws Exception {
    SnapshotRequestAssetModel assetSpec =
        new SnapshotRequestAssetModel().assetName("asset").rootValues(Collections.emptyList());

    SnapshotRequestContentsModel snapshotRequestContentsModel =
        new SnapshotRequestContentsModel()
            .datasetName("dataset")
            .mode(SnapshotRequestContentsModel.ModeEnum.BYASSET)
            .assetSpec(assetSpec);

    snapshotByAssetRequest.contents(Collections.singletonList(snapshotRequestContentsModel));
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotRootValuesListEmpty"});
  }

  @Test
  public void testSnapshotDatasetNameInvalid() throws Exception {
    // snapshotByAssetRequest is assumed to be valid, we will just mess with the dataset name in the
    // contents
    SnapshotRequestContentsModel contents = snapshotByAssetRequest.getContents().get(0);
    contents.setDatasetName("no spaces");
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    contents.setDatasetName("no-dashes");
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    contents.setDatasetName("");
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Size", "Pattern"});

    // Make a 512 character string, it should be considered too long by the validation.
    String tooLong = StringUtils.repeat("a", 512);
    contents.setDatasetName(tooLong);
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Size"});
  }

  @Test
  public void testSnapshotAssetNameInvalid() throws Exception {
    SnapshotRequestAssetModel assetSpec =
        snapshotByAssetRequest.getContents().get(0).getAssetSpec();
    assetSpec.setAssetName("no spaces");
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    assetSpec.setAssetName("no-dashes");
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Pattern"});

    assetSpec.setAssetName("");
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Size", "Pattern"});

    // Make a 64 character string, it should be considered too long by the validation.
    String tooLong = StringUtils.repeat("a", 64);
    assetSpec.setAssetName(tooLong);
    errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"Size"});
  }

  @Test
  public void testSnapshotRowIdsEmptyColumns() throws Exception {
    SnapshotRequestRowIdModel rowIdSpec =
        snapshotByRowIdsRequestModel.getContents().get(0).getRowIdSpec();
    rowIdSpec.getTables().get(0).setColumns(Collections.emptyList());
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByRowIdsRequestModel);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotTableColumnsMissing"});
  }

  @Test
  public void testSnapshotRowIdsEmptyRowIds() throws Exception {
    SnapshotRequestRowIdModel rowIdSpec =
        snapshotByRowIdsRequestModel.getContents().get(0).getRowIdSpec();
    rowIdSpec.getTables().get(0).setRowIds(Collections.emptyList());
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByRowIdsRequestModel);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotTableRowIdsMissing"});
  }

  @Test
  public void testSnapshotByQuery() throws Exception {
    SnapshotRequestModel querySpec = this.snapshotByQueryRequestModel;
    querySpec.getContents().get(0).getQuerySpec().setQuery(null);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByQueryRequestModel);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotQueryEmpty", "NotNull"});
  }

  @Test
  public void testSnapshotNameMissing() throws Exception {
    snapshotByAssetRequest.name(null);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotNameMissing", "NotNull"});
  }

  private void checkValidationErrorModel(ErrorModel errorModel, String[] messageCodes) {
    List<String> details = errorModel.getErrorDetail();
    assertThat(
        "Main message is right",
        errorModel.getMessage(),
        containsString("Validation errors - see error details"));
    /*
     * The global exception handler logs in this format:
     *
     * <fieldName>: '<messageCode>' (<defaultMessage>)
     *
     * We check to see if the code is wrapped in quotes to prevent matching on substrings.
     */
    List<Matcher<? super String>> expectedMatches =
        Arrays.stream(messageCodes)
            .map(code -> containsString("'" + code + "'"))
            .collect(Collectors.toList());
    assertThat("Detail codes are right", details, containsInAnyOrder(expectedMatches));
  }
}
