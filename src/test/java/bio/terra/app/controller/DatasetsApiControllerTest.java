package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetDataModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetDataException;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@ActiveProfiles({"google", "unittest"})
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetsApiControllerTest {
  @MockBean private DatasetService datasetService;
  @Autowired private MockMvc mvc;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();
  private static final String GET_PREVIEW_ENDPOINT =
      "/api/repository/v1/datasets/{id}/data/{table}";
  private static final SqlSortDirection DIRECTION = SqlSortDirection.ASC;
  private static final int LIMIT = 10;
  private static final int OFFSET = 0;
  private static final String FILTER = null;

  @Test
  public void testDatasetViewDataById() throws Exception {
    var id = UUID.randomUUID();
    var table = "good_table";
    var column = "good_column";
    mockDatasetViewDataByIdSuccess(id, table, column);
    verify(datasetService).verifyDatasetReadable(eq(id), any());
    verify(datasetService)
        .retrieveData(
            any(AuthenticatedUserRequest.class),
            eq(id),
            eq(table),
            eq(LIMIT),
            eq(OFFSET),
            eq(column),
            eq(DIRECTION),
            eq(FILTER));
  }

  @Test
  public void testDatasetViewDataByIdHandlesDataRepoRowId() throws Exception {
    var id = UUID.randomUUID();
    var table = "good_table";
    var column = "datarepo_row_id";
    mockDatasetViewDataByIdSuccess(id, table, column);
    verify(datasetService).verifyDatasetReadable(eq(id), any());
    verify(datasetService)
        .retrieveData(
            any(AuthenticatedUserRequest.class),
            eq(id),
            eq(table),
            eq(LIMIT),
            eq(OFFSET),
            eq(column),
            eq(DIRECTION),
            eq(FILTER));
  }

  @Test(expected = DatasetDataException.class)
  public void testDatasetViewDataByIdBadColumn() throws Exception {
    var id = UUID.randomUUID();
    var table = "good_table";
    var column = "bad_column";
    mockDatasetViewDataByIdError(id, table, column);
    verify(datasetService).verifyDatasetReadable(eq(id), any(AuthenticatedUserRequest.class));
    datasetService.retrieveData(TEST_USER, id, table, LIMIT, OFFSET, column, DIRECTION, FILTER);
  }

  @Test(expected = DatasetDataException.class)
  public void testDatasetViewDataByIdBadTable() throws Exception {
    var id = UUID.randomUUID();
    var table = "bad_table";
    var column = "good_column";
    mockDatasetViewDataByIdError(id, table, column);
    verify(datasetService).verifyDatasetReadable(eq(id), any());
    datasetService.retrieveData(TEST_USER, id, table, LIMIT, OFFSET, column, DIRECTION, FILTER);
  }

  private void mockDatasetViewDataByIdSuccess(UUID id, String table, String column)
      throws Exception {
    var list = List.of("hello", "world");
    var result = new DatasetDataModel().result(List.copyOf(list));
    when(datasetService.retrieveData(
            any(AuthenticatedUserRequest.class),
            eq(id),
            eq(table),
            eq(LIMIT),
            eq(OFFSET),
            eq(column),
            eq(DIRECTION),
            eq(FILTER)))
        .thenReturn(result);
    mvc.perform(
            get(GET_PREVIEW_ENDPOINT, id, table)
                .queryParam("limit", String.valueOf(LIMIT))
                .queryParam("offset", String.valueOf(OFFSET))
                .queryParam("sort", column)
                .queryParam("direction", String.valueOf(DIRECTION)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result").isArray());
  }

  private void mockDatasetViewDataByIdError(UUID id, String table, String column) throws Exception {
    when(datasetService.retrieveData(
            any(AuthenticatedUserRequest.class),
            eq(id),
            eq(table),
            eq(LIMIT),
            eq(OFFSET),
            eq(column),
            eq(DIRECTION),
            eq(FILTER)))
        .thenThrow(DatasetDataException.class);
    mvc.perform(
            get(GET_PREVIEW_ENDPOINT, id, table)
                .queryParam("limit", String.valueOf(LIMIT))
                .queryParam("offset", String.valueOf(OFFSET))
                .queryParam("sort", column)
                .queryParam("direction", String.valueOf(DIRECTION)))
        .andExpect(status().is5xxServerError());
  }
}
