package bio.terra.service.dataset;

import static bio.terra.service.dataset.ValidatorTestUtils.checkValidationErrorModel;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.controller.ApiValidationExceptionHandler;
import bio.terra.app.controller.DatasetsApiController;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.ErrorModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(
    classes = {
      AssetModelValidator.class,
      DatasetsApiController.class,
      ApiValidationExceptionHandler.class
    })
@WebMvcTest
@Tag(Unit.TAG)
class AssetModelValidatorTest {
  @MockitoBean private JobService jobService;
  @MockitoBean private DatasetService datasetService;
  @MockitoBean private IamService iamService;
  @MockitoBean private FileService fileService;
  @MockitoBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockitoBean private SnapshotBuilderService snapshotBuilderService;
  @MockitoBean private IngestRequestValidator ingestRequestValidator;
  @MockitoBean private DataDeletionRequestValidator dataDeletionRequestValidator;
  @MockitoBean private DatasetSchemaUpdateValidator datasetSchemaUpdateValidator;
  @MockitoBean private DatasetRequestValidator datasetRequestValidator;

  @Autowired private MockMvc mvc;

  @BeforeEach
  void beforeEach() {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(datasetRequestValidator.supports(any())).thenReturn(true);
    when(dataDeletionRequestValidator.supports(any())).thenReturn(true);
    when(datasetSchemaUpdateValidator.supports(any())).thenReturn(true);
  }

  private ErrorModel expectBadAssetCreateRequest(String jsonModel) throws Exception {
    String responseBody =
        mvc.perform(
                post("/api/repository/v1/datasets/" + UUID.randomUUID() + "/assets")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(jsonModel))
            .andExpect(status().is4xxClientError())
            .andReturn()
            .getResponse()
            .getContentAsString();

    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
  }

  @Test
  void testInvalidAssetCreateRequest() throws Exception {
    ErrorModel errorModel = expectBadAssetCreateRequest("{}");
    checkValidationErrorModel(errorModel, "NotNull", "NotNull", "NotNull");
  }

  @Test
  void testDuplicateColumnAssetCreateRequest() throws Exception {
    String jsonModel = TestUtils.loadJson("dataset-asset-duplicate-column.json");
    ErrorModel errorModel = expectBadAssetCreateRequest(jsonModel);
    checkValidationErrorModel(errorModel, "DuplicateColumnNames");
  }
}
