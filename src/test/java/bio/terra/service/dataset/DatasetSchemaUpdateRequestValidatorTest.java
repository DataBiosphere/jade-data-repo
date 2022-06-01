package bio.terra.service.dataset;

import static bio.terra.common.fixtures.DatasetFixtures.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DatasetFixtures;
import bio.terra.model.ColumnModel;
import bio.terra.model.DatasetSchemaUpdateModel;
import bio.terra.model.DatasetSchemaUpdateModelChanges;
import bio.terra.model.ErrorModel;
import bio.terra.model.TableDataType;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
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
public class DatasetSchemaUpdateRequestValidatorTest {

  @Autowired private MockMvc mvc;

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
        "Error model was returned on failure", StringUtils.contains(responseBody, "message"));

    return TestUtils.mapFromJson(responseBody, ErrorModel.class);
  }

  @Test
  public void testSchemaUpdateWithDuplicateTables() throws Exception {
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
  public void testSchemaUpdateWithNewRequiredColumn() throws Exception {
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
                    .addColumns(List.of(columnUpdateModel(existingTableName, newColumns))));
    ErrorModel errorModel = expectBadDatasetUpdateRequest(updateModel);
    assertThat(
        "Required column throws error",
        errorModel.getErrorDetail().get(0),
        containsString("RequiredColumns"));
  }
}
