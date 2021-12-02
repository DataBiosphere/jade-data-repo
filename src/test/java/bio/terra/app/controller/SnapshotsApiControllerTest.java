package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotPreviewModel;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
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
@SpringBootTest(
    properties = {"features.search.api=enabled", "datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SnapshotsApiControllerTest {

  private static final String GET_PREVIEW_ENDPOINT = "/api/repository/v1/snapshots/{id}/data";

  @Autowired private MockMvc mvc;

  @MockBean private IamService iamService;

  @MockBean private SnapshotService snapshotService;

  @Test
  public void lookupSnapshotPreviewById() throws Exception {
    var id = UUID.randomUUID();
    var table = "some_table";
    var limit = 10;
    var result = new SnapshotPreviewModel();
    when(snapshotService.retrievePreview(any(), any(), any(), any())).thenReturn(result);
    mvc.perform(
            get(GET_PREVIEW_ENDPOINT, id)
                .queryParam("table", table)
                .queryParam("limit", String.valueOf(limit)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.result").value(""));
    verify(iamService)
        .verifyAuthorization(
            any(), eq(IamResourceType.DATASNAPSHOT), eq(id.toString()), eq(IamAction.READ_DATA));
    verify(snapshotService).retrievePreview(id, table, limit, 0);
  }
}
