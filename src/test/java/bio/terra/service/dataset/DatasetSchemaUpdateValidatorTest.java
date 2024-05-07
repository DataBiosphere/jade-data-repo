package bio.terra.service.dataset;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.controller.ApiValidationExceptionHandler;
import bio.terra.app.controller.DatasetsApiController;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.ErrorModel;
import bio.terra.model.TableDataType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.filedata.FileService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
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
      DatasetSchemaUpdateValidator.class,
      DatasetsApiController.class,
      ApiValidationExceptionHandler.class
    })
@WebMvcTest
@Tag(Unit.TAG)
class DatasetSchemaUpdateValidatorTest {

  @Autowired private MockMvc mvc;

  @MockBean private JobService jobService;
  @MockBean private DatasetService datasetService;
  @MockBean private IamService iamService;
  @MockBean private FileService fileService;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private SnapshotBuilderService snapshotBuilderService;
  @MockBean private IngestRequestValidator ingestRequestValidator;
  @MockBean private AssetModelValidator assetModelValidator;
  @MockBean private DataDeletionRequestValidator dataDeletionRequestValidator;
  @MockBean private DatasetRequestValidator datasetRequestValidator;

  @BeforeEach
  void setup() throws Exception {
    when(ingestRequestValidator.supports(any())).thenReturn(true);
    when(datasetRequestValidator.supports(any())).thenReturn(true);
    when(assetModelValidator.supports(any())).thenReturn(true);
    when(dataDeletionRequestValidator.supports(any())).thenReturn(true);
  }

  private ErrorModel expectBadDatasetUpdateRequest(DatasetSchemaUpdateModel datasetRequest)
      throws Exception {
    MvcResult result =
        mvc.perform(
                post("/api/repository/v1/datasets/{id}/updateSchema", UUID.randomUUID())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(TestUtils.mapToJson(datasetRequest)))
            .andExpect(status().is4xxClientError())
            .andReturn();

    MockHttpServletResponse response = result.getResponse();
    String responseBody = response.getContentAsString();

    assertTrue(
        StringUtils.contains(responseBody, "message"), "Error model was returned on failure");

    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
  }

  @Test
  void testSchemaUpdateWithDuplicateTables() throws Exception {
    String newTableName = "new_table";
    String newTableColumnName = "new_table_column";
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("column addition tests")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addTables(
                        List.of(
                            DatasetFixtures.tableModel(newTableName, List.of(newTableColumnName)),
                            DatasetFixtures.tableModel(
                                newTableName, List.of(newTableColumnName)))));
    ErrorModel errorModel = expectBadDatasetUpdateRequest(updateModel);
    assertThat(
        "Required column throws error",
        errorModel.getErrorDetail().get(0),
        containsString("DuplicateTableNames"));
  }

  @Test
  void testSchemaUpdateWithNewRequiredColumn() throws Exception {
    String existingTableName = "thetable";
    String newRequiredColumnName = "required_column";
    List<ColumnModel> newColumns =
        List.of(
            DatasetFixtures.columnModel(newRequiredColumnName, TableDataType.STRING, false, true));

    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("column addition tests")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addColumns(
                        List.of(DatasetFixtures.columnUpdateModel(existingTableName, newColumns))));
    ErrorModel errorModel = expectBadDatasetUpdateRequest(updateModel);
    assertThat(
        "Required column throws error",
        errorModel.getErrorDetail().get(0),
        containsString("RequiredColumns"));
  }

  @Test
  void testSchemaUpdateWithDuplicateRelationships() throws Exception {
    DatasetSchemaUpdateModel updateModel =
        new DatasetSchemaUpdateModel()
            .description("duplicate relationship test")
            .changes(
                new DatasetSchemaUpdateModelChanges()
                    .addRelationships(
                        List.of(
                            DatasetFixtures.buildParticipantSampleRelationship(),
                            DatasetFixtures.buildParticipantSampleRelationship())));
    ErrorModel errorModel = expectBadDatasetUpdateRequest(updateModel);
    assertThat(
        "Duplicate relationship throws error",
        errorModel.getErrorDetail().get(0),
        containsString("DuplicateRelationshipNames"));
  }
}
