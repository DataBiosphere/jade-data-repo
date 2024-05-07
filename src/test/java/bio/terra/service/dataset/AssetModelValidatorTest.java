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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
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
  @MockBean private JobService jobService;
  @MockBean private DatasetService datasetService;
  @MockBean private IamService iamService;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private SnapshotBuilderService snapshotBuilderService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private DataDeletionRequestValidator dataDeletionRequestValidator;
  @MockBean private DatasetSchemaUpdateValidator datasetSchemaUpdateValidator;
  @MockBean private DatasetRequestValidator datasetRequestValidator;

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
