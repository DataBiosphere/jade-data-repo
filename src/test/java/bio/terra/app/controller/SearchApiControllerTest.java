package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.search.SearchService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotPreviewException;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest(
    properties = {"features.search.api=enabled", "datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SearchApiControllerTest {

  private static final String GET_PREVIEW_ENDPOINT =
      "/api/repository/v1/snapshots/{id}/data/{table}";
  private static final String UPSERT_DELETE_ENDPOINT = "/api/repository/v1/search/{id}/metadata";
  private static final SqlSortDirection DIRECTION = SqlSortDirection.ASC;
  private static final int LIMIT = 10;
  private static final int OFFSET = 0;

  @Autowired private MockMvc mvc;

  @MockBean private IamService iamService;

  @MockBean private SearchService searchService;

  @MockBean private SnapshotService snapshotService;

  private void mockSnapshotPreviewByIdSuccess(UUID id, String table, String column)
      throws Exception {
    var list = List.of("hello", "world");
    var result = new SnapshotPreviewModel().result(List.copyOf(list));
    when(snapshotService.retrievePreview(id, table, LIMIT, OFFSET, column, DIRECTION))
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

  private void mockSnapshotPreviewByIdError(UUID id, String table, String column) throws Exception {
    when(snapshotService.retrievePreview(id, table, LIMIT, OFFSET, column, DIRECTION))
        .thenThrow(SnapshotPreviewException.class);
    mvc.perform(
            get(GET_PREVIEW_ENDPOINT, id, table)
                .queryParam("limit", String.valueOf(LIMIT))
                .queryParam("offset", String.valueOf(OFFSET))
                .queryParam("sort", column)
                .queryParam("direction", String.valueOf(DIRECTION)))
        .andExpect(status().is5xxServerError());
  }

  @Test
  public void testSnapshotPreviewById() throws Exception {
    var id = UUID.randomUUID();
    var table = "good_table";
    var column = "good_column";
    mockSnapshotPreviewByIdSuccess(id, table, column);
    verify(iamService)
        .verifyAuthorization(
            any(), eq(IamResourceType.DATASNAPSHOT), eq(id.toString()), eq(IamAction.READ_DATA));
    verify(snapshotService).retrievePreview(id, table, LIMIT, OFFSET, column, DIRECTION);
  }

  @Test
  public void testSnapshotPreviewByIdHandlesDataRepoRowId() throws Exception {
    var id = UUID.randomUUID();
    var table = "good_table";
    var column = "datarepo_row_id";
    mockSnapshotPreviewByIdSuccess(id, table, column);
    verify(iamService)
        .verifyAuthorization(
            any(), eq(IamResourceType.DATASNAPSHOT), eq(id.toString()), eq(IamAction.READ_DATA));
    verify(snapshotService).retrievePreview(id, table, LIMIT, OFFSET, column, DIRECTION);
  }

  @Test(expected = SnapshotPreviewException.class)
  public void testSnapshotPreviewByIdBadColumn() throws Exception {
    var id = UUID.randomUUID();
    var table = "good_table";
    var column = "bad_column";
    mockSnapshotPreviewByIdError(id, table, column);
    verify(iamService)
        .verifyAuthorization(
            any(), eq(IamResourceType.DATASNAPSHOT), eq(id.toString()), eq(IamAction.READ_DATA));
    snapshotService.retrievePreview(id, table, LIMIT, OFFSET, column, DIRECTION);
  }

  @Test(expected = SnapshotPreviewException.class)
  public void testSnapshotPreviewByIdBadTable() throws Exception {
    var id = UUID.randomUUID();
    var table = "bad_table";
    var column = "good_column";
    mockSnapshotPreviewByIdError(id, table, column);
    verify(iamService)
        .verifyAuthorization(
            any(), eq(IamResourceType.DATASNAPSHOT), eq(id.toString()), eq(IamAction.READ_DATA));
    snapshotService.retrievePreview(id, table, LIMIT, OFFSET, column, DIRECTION);
  }

  @Test
  public void testCreateSearchIndex() throws Exception {
    var endpoint = "/api/repository/v1/search/{id}/index";
    var query = "query";
    var searchIndexRequest = new SearchIndexRequest().sql(query);
    var id = UUID.randomUUID();
    var snapshot = new Snapshot().id(id);
    when(snapshotService.retrieve(eq(id))).thenReturn(snapshot);
    mvc.perform(
            post(endpoint, id.toString())
                .contentType(MediaType.APPLICATION_JSON)
                .content(TestUtils.mapToJson(searchIndexRequest)))
        .andExpect(status().isOk());
    verify(iamService)
        .verifyAuthorization(any(), eq(IamResourceType.DATAREPO), any(), eq(IamAction.CONFIGURE));
    verify(iamService)
        .verifyAuthorization(
            any(), eq(IamResourceType.DATASNAPSHOT), eq(id.toString()), eq(IamAction.READ_DATA));
    verify(searchService).indexSnapshot(eq(snapshot), eq(searchIndexRequest));
  }
}
