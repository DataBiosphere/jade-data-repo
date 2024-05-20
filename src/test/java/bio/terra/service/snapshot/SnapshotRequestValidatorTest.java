package bio.terra.service.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.controller.ApiValidationExceptionHandler;
import bio.terra.app.controller.GlobalExceptionHandler;
import bio.terra.app.controller.SnapshotsApiController;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.UnitTestConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.JobModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
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
      SnapshotRequestValidator.class,
      SnapshotsApiController.class,
      ApiValidationExceptionHandler.class,
      GlobalExceptionHandler.class,
      UnitTestConfiguration.class
    })
@WebMvcTest
@Tag(Unit.TAG)
class SnapshotRequestValidatorTest {
  @Autowired private MockMvc mvc;
  @MockBean private JobService jobService;
  @MockBean private SnapshotService snapshotService;
  @MockBean private IamService iamService;
  @MockBean private FileService fileService;
  @MockBean private ApplicationConfiguration applicationConfiguration;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private SnapshotBuilderService snapshotBuilderService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private AssetModelValidator assetModelValidator;

  @BeforeEach
  void setup() {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
  }

  @Test
  void validateByRequestIdFail() throws Exception {
    String invalidRequestModel =
        """
        {
            "name": "test_byRequestId",
            "description": "",
            "policies": {},
            "contents": [
                {
                    "datasetName": "it_dataset_omop",
                    "mode": "byRequestId"
                }
            ]
        }
        """;
    mvc.perform(
            post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidRequestModel))
        .andExpect(status().is4xxClientError());
  }

  @Test
  void validateByRequestId() throws Exception {
    when(jobService.retrieveJob(any(), any()))
        .thenReturn(new JobModel().jobStatus(JobModel.JobStatusEnum.SUCCEEDED));
    String validRequestModel =
        """
        {
            "name": "test_byRequestId",
            "description": "",
            "policies": {},
            "contents": [
                {
                    "datasetName": "it_dataset_omop",
                    "mode": "byRequestId",
                    "requestIdSpec": {
                            "snapshotRequestId": "756693d8-c7f2-40a7-aeba-4fa98f67dda3"
                    }
                }
            ]
        }
        """;
    mvc.perform(
            post("/api/repository/v1/snapshots")
                .contentType(MediaType.APPLICATION_JSON)
                .content(validRequestModel))
        .andExpect(status().is2xxSuccessful());
  }
}
