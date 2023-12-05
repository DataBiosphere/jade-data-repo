package bio.terra.service.snapshotbuilder;

import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
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
  private SnapshotBuilderService snapshotBuilderService;

  @BeforeEach
  public void setUp() {
    snapshotBuilderService = new SnapshotBuilderService(snapshotRequestDao);
  }

  @Test
  void createSnapshotRequest() {
    UUID datasetId = UUID.randomUUID();
    String email = "user@gmail.com";
    snapshotBuilderService.createSnapshotRequest(
        datasetId, SnapshotBuilderTestData.ACCESS_REQUEST, email);
    verify(snapshotRequestDao).create(datasetId, SnapshotBuilderTestData.ACCESS_REQUEST, email);
  }
}
