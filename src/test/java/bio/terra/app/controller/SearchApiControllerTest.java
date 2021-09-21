package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.SearchIndexRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.search.SearchService;
import bio.terra.service.search.SnapshotSearchMetadataDao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = "features.search.api=enabled")
@AutoConfigureMockMvc
@Category(Unit.class)
public class SearchApiControllerTest {

  @Autowired private MockMvc mvc;

  @MockBean private IamService iamService;

  @MockBean private SnapshotSearchMetadataDao snapshotMetadataDao;

  @MockBean private SearchService searchService;

  @MockBean private SnapshotService snapshotService;

  @Test
  public void testUpsert() throws Exception {
    var endpoint = "/api/repository/v1/search/{id}/metadata";
    var id = UUID.randomUUID();
    var jsonString = "{\"dct:identifier\": \"my snapshot\", \"dcat:byteSize\" : \"10000\"}";
    mvc.perform(put(endpoint, id).contentType(MediaType.APPLICATION_JSON).content(jsonString))
        .andExpect(status().isOk());
    verify(iamService)
        .verifyAuthorization(
            any(),
            eq(IamResourceType.DATASNAPSHOT),
            eq(id.toString()),
            eq(IamAction.UPDATE_SNAPSHOT));
    verify(searchService).upsertSearchMetadata(id, jsonString);
  }

  @Test
  public void testDelete() throws Exception {
    var endpoint = "/api/repository/v1/search/{id}/metadata";
    var id = UUID.randomUUID();
    mvc.perform(delete(endpoint, id)).andExpect(status().isNoContent());
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
    mvc.perform(get(endpoint)).andExpect(status().isOk());
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
  }
}
