package bio.terra.app.controller;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexRequest;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.search.SearchService;
import bio.terra.service.search.SnapshotSearchMetadataDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  @Autowired private MockMvc mvc;

  @MockBean private IamService iamService;

  @MockBean private SnapshotSearchMetadataDao snapshotMetadataDao;

  @MockBean private SearchService searchService;

  @MockBean private SnapshotService snapshotService;

  @Test
  public void testSnapshotPreviewById() throws Exception {
    var id = UUID.randomUUID();
    var table = "good_table";
    var column = "good_column";
    var direction = SqlSortDirection.ASC;
    var limit = 10;
    var offset = 0;
    var list = List.of("hello", "world");
    var result = new SnapshotPreviewModel().result(List.copyOf(list));
    when(snapshotService.retrievePreview(id, table, limit, offset, column, direction))
        .thenReturn(result);
    mvc.perform(
            get(GET_PREVIEW_ENDPOINT, id, table)
                .queryParam("limit", String.valueOf(limit))
                .queryParam("offset", String.valueOf(offset))
                .queryParam("sort", column)
                .queryParam("direction", String.valueOf(direction))
        )
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result").isArray());
    verify(iamService)
        .verifyAuthorization(
            any(), eq(IamResourceType.DATASNAPSHOT), eq(id.toString()), eq(IamAction.READ_DATA));
    verify(snapshotService).retrievePreview(id, table, limit, offset, column, direction);
  }

  @Test
  public void testUpsert() throws Exception {
    var id = UUID.randomUUID();
    String json = "{\"dct:identifier\": \"my snapshot\", \"dcat:byteSize\" : \"10000\"}";
    mvc.perform(
            put(UPSERT_DELETE_ENDPOINT, id).contentType(MediaType.APPLICATION_JSON).content(json))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.metadataSummary").value(containsString(id.toString())));
    verify(iamService)
        .verifyAuthorization(
            any(),
            eq(IamResourceType.DATASNAPSHOT),
            eq(id.toString()),
            eq(IamAction.UPDATE_SNAPSHOT));
    verify(snapshotMetadataDao).putMetadata(id, json);
  }

  @Test
  public void testDelete() throws Exception {
    var id = UUID.randomUUID();
    mvc.perform(delete(UPSERT_DELETE_ENDPOINT, id)).andExpect(status().isNoContent());
    verify(iamService)
        .verifyAuthorization(
            any(),
            eq(IamResourceType.DATASNAPSHOT),
            eq(id.toString()),
            eq(IamAction.UPDATE_SNAPSHOT));
    verify(snapshotMetadataDao).deleteMetadata(id);
  }

  @Test
  public void testEnumerateSnapshotSearch() throws Exception {
    var endpoint = "/api/repository/v1/search/metadata";
    var json = "{\"test\":\"data\"}";
    UUID uuid = UUID.randomUUID();
    Set<UUID> uuids = Set.of(uuid);
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(uuid, Set.of(IamRole.DISCOVERER));
    when(iamService.listAuthorizedResources(any(), eq(IamResourceType.DATASNAPSHOT)))
        .thenReturn(resourcesAndRoles);
    when(snapshotMetadataDao.getMetadata(uuids)).thenReturn(Map.of(uuid, json));
    mvc.perform(get(endpoint))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result[0].test").value("data"));
    verify(iamService).listAuthorizedResources(any(), eq(IamResourceType.DATASNAPSHOT));
    verify(snapshotMetadataDao).getMetadata(uuids);
  }

  @Test
  public void testEnumerateSnapshotSearchPermission() throws Exception {
    var endpoint = "/api/repository/v1/search/metadata";
    var json = "{}";
    UUID uuid = UUID.randomUUID();
    Set<UUID> uuids = Set.of(uuid);
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(uuid, Set.of(IamRole.DISCOVERER));
    when(iamService.listAuthorizedResources(any(), eq(IamResourceType.DATASNAPSHOT)))
        .thenReturn(resourcesAndRoles);
    when(snapshotMetadataDao.getMetadata(uuids)).thenReturn(Map.of(uuid, json));
    mvc.perform(get(endpoint))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result[0].roles").value("discoverer"));
    verify(iamService).listAuthorizedResources(any(), eq(IamResourceType.DATASNAPSHOT));
    verify(snapshotMetadataDao).getMetadata(uuids);
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
