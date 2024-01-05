package bio.terra.service.snapshotbuilder;

import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotAccessRequestResponse;
import java.util.UUID;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotBuilderServiceTest {
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private DatasetService datasetService;
  @Mock private BigQueryPdao bigQueryPdao;
  private SnapshotBuilderService snapshotBuilderService;

  @BeforeEach
  public void beforeEach() {
    snapshotBuilderService = new SnapshotBuilderService(snapshotRequestDao, datasetService, bigQueryPdao);
  }

  @Test
  void createSnapshotRequest() {
    UUID datasetId = UUID.randomUUID();
    String email = "user@gmail.com";
    SnapshotAccessRequestResponse response = new SnapshotAccessRequestResponse();
    when(snapshotRequestDao.create(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email))
        .thenReturn(response);
    Assertions.assertEquals(
        snapshotBuilderService.createSnapshotRequest(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email),
        response);
  }
}
