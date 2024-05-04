package bio.terra.service.dataset;

import static bio.terra.model.DataDeletionRequest.SpecTypeEnum.GCSFILE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.controller.ApiValidationExceptionHandler;
import bio.terra.app.controller.DatasetsApiController;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionJsonArrayModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(
    classes = {
      DataDeletionRequestValidator.class,
      DatasetsApiController.class,
      ApiValidationExceptionHandler.class
    })
@WebMvcTest
@Tag(Unit.TAG)
class DataDeletionRequestValidatorTest {

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
  @MockBean private DatasetRequestValidator datasetRequestValidator;

  private DataDeletionRequest goodGcsRequest;
  private DataDeletionRequest goodJsonArrayRequest;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setup() throws Exception {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(datasetRequestValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
    when(datasetSchemaUpdateValidator.supports(any())).thenReturn(true);

    when(authenticatedUserRequestFactory.from(any())).thenReturn(TEST_USER);
    when(datasetService.retrieveDatasetSummary(any())).thenReturn(new DatasetSummaryModel());
    when(datasetService.deleteTabularData(any(), any(), any())).thenReturn("mock-job-id");
    when(jobService.retrieveJob(any(), any()))
        .thenReturn(new JobModel().id("mock-job-id").jobStatus(JobModel.JobStatusEnum.RUNNING));
    goodGcsRequest =
        new DataDeletionRequest()
            .specType(GCSFILE)
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .tables(
                List.of(
                    new DataDeletionTableModel()
                        .tableName("my-gcs-table")
                        .gcsFileSpec(
                            new DataDeletionGcsFileModel()
                                .path("gs://my-bucket/my-folder/my-file.json")
                                .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV))));

    goodJsonArrayRequest =
        new DataDeletionRequest()
            .specType(DataDeletionRequest.SpecTypeEnum.JSONARRAY)
            .deleteType(DataDeletionRequest.DeleteTypeEnum.SOFT)
            .tables(
                List.of(
                    new DataDeletionTableModel()
                        .tableName("my-json-table")
                        .jsonArraySpec(
                            new DataDeletionJsonArrayModel().rowIds(List.of(UUID.randomUUID())))));
  }

  @Test
  void goodGcsFileSpecTest() throws Exception {
    mvc.perform(
            post("/api/repository/v1/datasets/{id}/deletes", UUID.randomUUID())
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(goodGcsRequest)))
        .andExpect(status().is2xxSuccessful())
        .andReturn();
  }

  void goodJsonArraySpecTest() throws Exception {
    mvc.perform(
            post("/api/repository/v1/datasets/{id}/deletes", UUID.randomUUID())
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(goodJsonArrayRequest)))
        .andExpect(status().is2xxSuccessful())
        .andReturn();
  }

  @Test
  void badGcsFileSpecTest() throws Exception {
    DataDeletionRequest badGcsRequest =
        goodGcsRequest.tables(
            List.of(
                new DataDeletionTableModel()
                    .tableName("badPathTable")
                    .gcsFileSpec(
                        new DataDeletionGcsFileModel()
                            .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)
                            .path("invalidpath")),
                new DataDeletionTableModel()
                    .tableName("")
                    .gcsFileSpec(new DataDeletionGcsFileModel().fileType(null).path(null)),
                new DataDeletionTableModel()
                    .tableName("invalidSpecTable")
                    .jsonArraySpec(
                        new DataDeletionJsonArrayModel().rowIds(List.of(UUID.randomUUID()))),
                new DataDeletionTableModel().tableName("noSpecTable")));
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets/{id}/deletes", UUID.randomUUID())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(badGcsRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();
    var response = result.getResponse();

    ErrorModel errorModel = TestUtils.mapFromJson(response.getContentAsString(), ErrorModel.class);

    Set<String> expectedErrors =
        Set.of(
            "tables[0].gcsFileSpec.path: 'dataDeletion.tables.gcsFileSpec.path.invalid'",
            "tables[1].tableName: 'dataDeletionRequest.tables.name.empty'",
            "tables[1].gcsFileSpec.path: 'NotNull'",
            "tables[1].gcsFileSpec.fileType: 'NotNull'",
            "tables[2].jsonArraySpec: 'dataDeletion.specType.mismatch'",
            "tables[3].gcsFileSpec: 'dataDeletion.tables.gcsFileSpec.missing'");

    assertThat(
        "The invalid GcsFileSpec request has the right errors",
        getErrors(errorModel),
        equalTo(expectedErrors));
  }

  @Test
  void badJsonArraySpecTest() throws Exception {
    DataDeletionRequest badJsonRequest =
        goodJsonArrayRequest.tables(
            List.of(
                new DataDeletionTableModel()
                    .tableName("badPathTable")
                    .jsonArraySpec(new DataDeletionJsonArrayModel().rowIds(null)),
                new DataDeletionTableModel()
                    .tableName("invalidSpecTable")
                    .gcsFileSpec(
                        new DataDeletionGcsFileModel()
                            .path("gs://not/the/right/spec.csv")
                            .fileType(DataDeletionGcsFileModel.FileTypeEnum.CSV)),
                new DataDeletionTableModel().tableName("noSpecTable")));
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets/{id}/deletes", UUID.randomUUID())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(badJsonRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();
    var response = result.getResponse();

    ErrorModel errorModel = TestUtils.mapFromJson(response.getContentAsString(), ErrorModel.class);
    Set<String> expectedErrors =
        Set.of(
            "tables[0].jsonArraySpec.rowIds: 'NotNull'",
            "tables[1].gcsFileSpec: 'dataDeletion.specType.mismatch'",
            "tables[2].jsonArraySpec: 'dataDeletion.tables.jsonArraySpec.missing'");
    assertThat(
        "The invalid GcsFileSpec request has the right errors",
        getErrors(errorModel),
        equalTo(expectedErrors));
  }

  @Test
  void testBadRequest() throws Exception {
    DataDeletionRequest badRequest = new DataDeletionRequest().addTablesItem(null);
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets/{id}/deletes", UUID.randomUUID())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(badRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();
    var response = result.getResponse();

    ErrorModel errorModel = TestUtils.mapFromJson(response.getContentAsString(), ErrorModel.class);
    Set<String> expectedErrors = Set.of("specType: 'NotNull'", "deleteType: 'NotNull'");
    assertThat(
        "The invalid request has the right errors", getErrors(errorModel), equalTo(expectedErrors));
  }

  private Set<String> getErrors(ErrorModel errorModel) {
    return errorModel.getErrorDetail().stream()
        .map(d -> d.replaceAll("\\(.*\\)", "").trim())
        .collect(Collectors.toSet());
  }
}
