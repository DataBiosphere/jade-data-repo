package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.search.SnapshotSearchMetadataDao;
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

  private static final String ENDPOINT = "/api/repository/v1/search/{id}/metadata";

  @Test
  public void testUpsert() throws Exception {
    var id = UUID.randomUUID();
    var json = "{\"actual\": \"data\"}\"}";
    mvc.perform(put(ENDPOINT, id).contentType(MediaType.APPLICATION_JSON).content(json))
        .andExpect(status().isOk());
    verifyAuthApi(id);
    verify(snapshotMetadataDao).putMetadata(id, json);
  }

  @Test
  public void testDelete() throws Exception {
    var id = UUID.randomUUID();
    mvc.perform(delete(ENDPOINT, id)).andExpect(status().isNoContent());
    verifyAuthApi(id);
    verify(snapshotMetadataDao).deleteMetadata(id);
  }

  private void verifyAuthApi(UUID id) {
    verify(iamService)
        .verifyAuthorization(
            any(),
            eq(IamResourceType.DATASNAPSHOT),
            eq(id.toString()),
            eq(IamAction.UPDATE_SNAPSHOT));
  }
}
