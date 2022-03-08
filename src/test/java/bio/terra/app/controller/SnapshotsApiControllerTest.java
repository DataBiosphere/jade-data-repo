package bio.terra.app.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.common.category.Unit;
import bio.terra.model.EnumerateSnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest("datarepo.testWithEmbeddedDatabase=false")
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SnapshotsApiControllerTest {

  @Autowired private MockMvc mvc;

  @MockBean private IamService iamService;

  @MockBean private SnapshotService snapshotService;

  @Test
  public void enumerateSnapshots() throws Exception {
    var endpoint = "/api/repository/v1/snapshots";
    UUID uuid = UUID.randomUUID();
    IamRole role = IamRole.DISCOVERER;
    Set<UUID> uuids = Set.of(uuid);
    Map<UUID, Set<IamRole>> resourcesAndRoles = Map.of(uuid, Set.of(role));
    when(iamService.listAuthorizedResources(any(), eq(IamResourceType.DATASNAPSHOT)))
        .thenReturn(resourcesAndRoles);
    SnapshotSummaryModel summaryModel = new SnapshotSummaryModel();
    summaryModel.setId(uuid);
    EnumerateSnapshotModel enumerateModel = new EnumerateSnapshotModel();
    enumerateModel.addItemsItem(summaryModel);
    when(snapshotService.enumerateSnapshots(
            anyInt(), anyInt(), any(), any(), any(), any(), any(), eq(uuids)))
        .thenReturn(enumerateModel);
    mvc.perform(get(endpoint))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items[0].id").value(uuid.toString()))
        .andExpect(jsonPath("$.items[0].roles[0]").value(role.toString()));
    verify(iamService).listAuthorizedResources(any(), eq(IamResourceType.DATASNAPSHOT));
  }
}
