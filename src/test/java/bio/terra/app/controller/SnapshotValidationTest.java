package bio.terra.app.controller;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.ErrorModel;
import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.model.SnapshotRequestRowIdTableModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import bio.terra.service.snapshot.SnapshotService;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(
    classes = {
      ApiValidationExceptionHandler.class,
      AssetModelValidator.class,
      GlobalExceptionHandler.class,
      SnapshotsApiController.class,
      SnapshotRequestValidator.class
    })
@Tag(Unit.TAG)
@WebMvcTest
class SnapshotValidationTest {

  @Autowired private MockMvc mvc;
  @MockBean private JobService jobService;
  @MockBean private SnapshotService snapshotService;
  @MockBean private IamService iamService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @SpyBean private ApplicationConfiguration applicationConfiguration;

  private SnapshotRequestModel snapshotByAssetRequest;

  private SnapshotRequestModel snapshotByRowIdsRequestModel;

  private SnapshotRequestModel snapshotByQueryRequestModel;

  @BeforeEach
  void setup() {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
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

    assertThat(
        "Error model was returned on failure", responseBody, Matchers.containsString("message"));
    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
  }

  // Generate a valid snapshot-by-asset request, we will tweak individual pieces to test validation
  // below
  private SnapshotRequestModel makeSnapshotAssetRequest() {
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
  private SnapshotRequestModel makeSnapshotRowIdsRequest() {
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
  private SnapshotRequestModel makeSnapshotByQueryRequest() {
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

  @ParameterizedTest
  @MethodSource("invalidResourceNames")
  void testSnapshotNameInvalid(String invalidName, String[] messageCodes) throws Exception {
    snapshotByAssetRequest.name(invalidName);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, messageCodes);
  }

  @ParameterizedTest
  @MethodSource("invalidResourceNames")
  void testSnapshotDatasetNameInvalid(String invalidName, String[] messageCodes) throws Exception {
    // snapshotByAssetRequest is assumed to be valid, we will just mess with the dataset name in the
    // contents
    SnapshotRequestContentsModel contents = snapshotByAssetRequest.getContents().get(0);
    contents.setDatasetName(invalidName);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, messageCodes);
  }

  private static Stream<Arguments> invalidResourceNames() {
    return Stream.of(
        arguments("no spaces", new String[] {"Pattern"}),
        arguments("no-dashes", new String[] {"Pattern"}),
        arguments("", new String[] {"Size", "Pattern"}),
        // Make a 512 character string, it should be considered too long by the validation.
        // Note: a 511 character string, we are okay with
        arguments(StringUtils.repeat("a", 512), new String[] {"Size"}));
  }

  @ParameterizedTest
  @MethodSource
  void testSnapshotDescriptionInvalid(String invalidDescription, String[] messageCodes)
      throws Exception {
    snapshotByAssetRequest.description(invalidDescription);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, messageCodes);
  }

  private static Stream<Arguments> testSnapshotDescriptionInvalid() {
    return Stream.of(
        arguments(StringUtils.repeat("a", 2048), new String[] {"SnapshotDescriptionTooLong"}),
        arguments(null, new String[] {"SnapshotDescriptionMissing"}));
  }

  @Test
  void testSnapshotValuesListEmpty() throws Exception {
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

  @ParameterizedTest
  @MethodSource
  void testSnapshotAssetNameInvalid(String invalidAssetName, String[] messageCodes)
      throws Exception {
    SnapshotRequestAssetModel assetSpec =
        snapshotByAssetRequest.getContents().get(0).getAssetSpec();
    assetSpec.setAssetName(invalidAssetName);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, messageCodes);
  }

  private static Stream<Arguments> testSnapshotAssetNameInvalid() {
    return Stream.of(
        arguments("no spaces", new String[] {"Pattern"}),
        arguments("no-dashes", new String[] {"Pattern"}),
        arguments("", new String[] {"Size", "Pattern"}),
        // Make a 64 character string, it should be considered too long by the validation.
        arguments(StringUtils.repeat("a", 64), new String[] {"Size"}));
  }

  @Test
  void testSnapshotRowIdsEmptyColumns() throws Exception {
    SnapshotRequestRowIdModel rowIdSpec =
        snapshotByRowIdsRequestModel.getContents().get(0).getRowIdSpec();
    rowIdSpec.getTables().get(0).setColumns(Collections.emptyList());
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByRowIdsRequestModel);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotTableColumnsMissing"});
  }

  @Test
  void testSnapshotRowIdsEmptyRowIds() throws Exception {
    SnapshotRequestRowIdModel rowIdSpec =
        snapshotByRowIdsRequestModel.getContents().get(0).getRowIdSpec();
    rowIdSpec.getTables().get(0).setRowIds(Collections.emptyList());
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByRowIdsRequestModel);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotTableRowIdsMissing"});
  }

  @Test
  void testSnapshotByQuery() throws Exception {
    SnapshotRequestModel querySpec = this.snapshotByQueryRequestModel;
    querySpec.getContents().get(0).getQuerySpec().setQuery(null);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByQueryRequestModel);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotQueryEmpty", "NotNull"});
  }

  @Test
  void testSnapshotNameMissing() throws Exception {
    snapshotByAssetRequest.name(null);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotNameMissing", "NotNull"});
  }

  @Test
  void testSnapshotValidCompactIdPrefix() throws Exception {
    when(applicationConfiguration.getCompactIdPrefixAllowList()).thenReturn(List.of("foo.0"));
    // Set the name to null since we need the request to fail to keep consistent
    snapshotByAssetRequest.compactIdPrefix("foo.0").name(null);
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"SnapshotNameMissing", "NotNull"});
  }

  @Test
  void testSnapshotInvalidCompactIdPrefix() throws Exception {
    when(applicationConfiguration.getCompactIdPrefixAllowList()).thenReturn(List.of("foo.0"));
    snapshotByAssetRequest.compactIdPrefix("bar.0");
    ErrorModel errorModel = expectBadSnapshotCreateRequest(snapshotByAssetRequest);
    checkValidationErrorModel(errorModel, new String[] {"InvalidCompactIdPrefix"});
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
