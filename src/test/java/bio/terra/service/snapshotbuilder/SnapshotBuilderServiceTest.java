package bio.terra.service.snapshotbuilder;

import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.EnumerateSnapshotAccessRequest;
import bio.terra.model.EnumerateSnapshotAccessRequestItem;
import bio.terra.model.SnapshotAccessRequestResponse;
import java.util.List;
import java.util.UUID;
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
  private SnapshotBuilderService snapshotBuilderService;

  @BeforeEach
  public void beforeEach() {
    snapshotBuilderService = new SnapshotBuilderService(snapshotRequestDao);
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

  @Test
  void enumerateSnapshotRequestsByDatasetId() {
    UUID datasetId = UUID.randomUUID();
    SnapshotAccessRequestResponse responseItem = new SnapshotAccessRequestResponse();
    List<SnapshotAccessRequestResponse> response = List.of(responseItem, responseItem);
    when(snapshotRequestDao.enumerateByDatasetId(datasetId)).thenReturn(response);
    Assertions.assertEquals(
        snapshotBuilderService.enumerateByDatasetId(datasetId),
        snapshotBuilderService.convertToEnumerateModel(response));
  }

  @Test
  void convertToEnumerateModel() {
    SnapshotAccessRequestResponse inputItem =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse();
    List<SnapshotAccessRequestResponse> response = List.of(inputItem);

    EnumerateSnapshotAccessRequestItem expectedItem = new EnumerateSnapshotAccessRequestItem();
    expectedItem.id(inputItem.getId());
    expectedItem.status(inputItem.getStatus());
    expectedItem.createdDate(inputItem.getCreatedDate());
    expectedItem.name(inputItem.getSnapshotName());
    expectedItem.researchPurpose(inputItem.getSnapshotResearchPurpose());
    EnumerateSnapshotAccessRequest expected = new EnumerateSnapshotAccessRequest();
    expected.add(expectedItem);

    EnumerateSnapshotAccessRequest converted =
        snapshotBuilderService.convertToEnumerateModel(response);
    EnumerateSnapshotAccessRequestItem convertedItem = converted.get(0);

    Assertions.assertEquals(converted.size(), expected.size());
    Assertions.assertEquals(convertedItem, expectedItem);
  }
}
