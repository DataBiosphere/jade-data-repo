package bio.terra.service.tabulardata.google.bigquery;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.tabulardata.google.BigQueryProject;
import com.google.cloud.bigquery.Acl;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class BigQuerySnapshotPdaoTest {

  @Mock private BigQueryProject bigQueryProject;

  private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private Snapshot snapshot;
  private static final String NAME = "test";
  private static final String EMAIL = "email";

  @BeforeEach
  void beforeEach() {
    GoogleProjectResource resource = new GoogleProjectResource().googleProjectId("project id");
    snapshot = new Snapshot().name(NAME).projectResource(resource);
    when(bigQueryProject.getProjectId()).thenReturn(resource.getGoogleProjectId());
    BigQueryProject.put(bigQueryProject);
    bigQuerySnapshotPdao = new BigQuerySnapshotPdao(mock(), mock());
  }

  @Test
  void grantReadAccessToSnapshot() throws Exception {
    bigQuerySnapshotPdao.grantReadAccessToSnapshot(snapshot, List.of(EMAIL));
    verify(bigQueryProject)
        .addDatasetAcls(NAME, List.of(Acl.of(new Acl.Group(EMAIL), Acl.Role.READER)));
  }

  @Test
  void revokeReadAccessToSnapshot() throws Exception {
    bigQuerySnapshotPdao.revokeReadAccessToSnapshot(snapshot, List.of(EMAIL));
    verify(bigQueryProject)
        .removeDatasetAcls(NAME, List.of(Acl.of(new Acl.Group(EMAIL), Acl.Role.READER)));
  }
}
