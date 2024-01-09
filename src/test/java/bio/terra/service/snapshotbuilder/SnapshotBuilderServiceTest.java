package bio.terra.service.snapshotbuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.service.dataset.DatasetService;
import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;
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
  private SnapshotBuilderService snapshotBuilderService;

  @BeforeEach
  public void beforeEach() {
    snapshotBuilderService = new SnapshotBuilderService(snapshotRequestDao, datasetService);
  }

  @Test
  void createSnapshotRequest() {
    UUID datasetId = UUID.randomUUID();
    String email = "user@gmail.com";
    SnapshotAccessRequestResponse response = new SnapshotAccessRequestResponse();
    when(snapshotRequestDao.create(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email))
        .thenReturn(response);
    assertEquals(
        snapshotBuilderService.createSnapshotRequest(
            datasetId, SnapshotBuilderTestData.createSnapshotAccessRequest(), email),
        response);
  }

  @Test
  void getConceptChildrenEmpty() throws InterruptedException {
    AuthenticatedUserRequest testUser = AuthenticationFixtures.randomUserRequest();
    UUID datasetId = UUID.randomUUID();
    DatasetModel datasetModel =
        new DatasetModel().name("name");
    Schema schema = Schema.of();
    TableResult tableResult = new TableResult(schema, 1, new EmptyTableResult(schema));
    when(datasetService.retrieveDatasetModel(datasetId, testUser)).thenReturn(datasetModel);
    when(datasetService.query( anyString(), eq(datasetId)))
        .thenReturn(tableResult);
    SnapshotBuilderGetConceptsResponse expected = new SnapshotBuilderGetConceptsResponse();
    assertEquals(snapshotBuilderService.getConceptChildren(datasetId, 100, testUser), expected);
  }
}
