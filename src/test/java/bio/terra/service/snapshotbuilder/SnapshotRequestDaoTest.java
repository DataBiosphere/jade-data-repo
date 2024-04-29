package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class SnapshotRequestDaoTest {

  @Autowired private DaoOperations daoOperations;
  @Autowired private SnapshotRequestDao snapshotRequestDao;

  private Dataset dataset;
  private Snapshot snapshot;
  private SnapshotAccessRequest snapshotAccessRequest;

  private static final String EMAIL = "user@gmail.com";

  @BeforeEach
  void beforeEach() throws IOException {
    dataset = daoOperations.createDataset(DaoOperations.DATASET_MINIMAL);
    snapshot = daoOperations.createAndIngestSnapshot(dataset, DaoOperations.SNAPSHOT_MINIMAL);
    snapshotAccessRequest = SnapshotBuilderTestData.createSnapshotAccessRequest();
  }

  private SnapshotAccessRequestResponse createRequest_old() {
    return snapshotRequestDao.create_old(dataset.getId(), snapshotAccessRequest, EMAIL);
  }

  private SnapshotAccessRequestResponse createRequest() {
    return snapshotRequestDao.create(snapshot.getId(), snapshotAccessRequest, EMAIL);
  }

  private void verifyResponseContents(SnapshotAccessRequestResponse response, boolean isOld) {
    SnapshotAccessRequestResponse expected = new SnapshotAccessRequestResponse();
    if (isOld) {
      expected.datasetId(dataset.getId());
    } else {
      expected.sourceSnapshotId(snapshot.getId());
    }
    expected.snapshotName(
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse().getSnapshotName());
    expected.snapshotResearchPurpose(
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse().getSnapshotResearchPurpose());
    expected.snapshotSpecification(SnapshotBuilderTestData.createSnapshotBuilderRequest());
    expected.createdBy(EMAIL);
    expected.status(SnapshotAccessRequestStatus.SUBMITTED);
    assertThat(
        "Given response is the same as expected.",
        response,
        samePropertyValuesAs(expected, "id", "createdDate"));
    assertNotNull(response.getId(), "Snapshot Access Request Response should have an id");
    assertNotNull(
        response.getCreatedDate(),
        "Snapshot Access Request Response should have a create date timestamp");
  }

  @Test
  void getById_old() {
    SnapshotAccessRequestResponse response = createRequest_old();
    SnapshotAccessRequestResponse retrieved = snapshotRequestDao.getById(response.getId());
    verifyResponseContents(retrieved, true);
  }

  @Test
  void getById() {
    SnapshotAccessRequestResponse response = createRequest();
    SnapshotAccessRequestResponse retrieved = snapshotRequestDao.getById(response.getId());
    verifyResponseContents(retrieved, false);
  }

  @Test
  void getByIdNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(UUID.randomUUID()));
  }

  @Test
  void enumerateByDatasetId_old() {
    SnapshotAccessRequestResponse response = createRequest_old();
    SnapshotAccessRequestResponse response1 = createRequest_old();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerateByDatasetId_old(dataset.getId()),
        contains(response, response1));
  }

  @Test
  void enumerateByDatasetIdNotFound_old() {
    assertThat(
        "For a dataset id that does not exist nothing is returned",
        snapshotRequestDao.enumerateByDatasetId_old(UUID.randomUUID()),
        empty());
  }

  @Test
  void enumerateBySnapshotId() {
    SnapshotAccessRequestResponse response = createRequest();
    SnapshotAccessRequestResponse response1 = createRequest();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerateBySnapshotId(snapshot.getId()),
        contains(response, response1));
  }

  @Test
  void enumerateBySnapshotIdNotFound() {
    assertThat(
        "For a dataset id that does not exist nothing is returned",
        snapshotRequestDao.enumerateBySnapshotId(UUID.randomUUID()),
        empty());
  }

  @Test
  void create_old() {
    SnapshotAccessRequestResponse response = createRequest_old();
    verifyResponseContents(response, true);

    SnapshotAccessRequestResponse response1 =
        snapshotRequestDao.create_old(dataset.getId(), snapshotAccessRequest, EMAIL);

    assertNotEquals(
        response1.getId(),
        response.getId(),
        "Snapshot Access Request Response should have unique request id");
    assertNotEquals(
        response1.getCreatedDate(),
        response.getCreatedDate(),
        "Snapshot Access Request Response should have unique create date timestamp");
  }

  @Test
  void createDatasetIdNotFound_old() {
    assertThrows(
        NotFoundException.class,
        () -> snapshotRequestDao.create_old(UUID.randomUUID(), snapshotAccessRequest, EMAIL));
  }

  @Test
  void create() {
    SnapshotAccessRequestResponse response = createRequest();
    verifyResponseContents(response, false);

    SnapshotAccessRequestResponse response1 =
        snapshotRequestDao.create(snapshot.getId(), snapshotAccessRequest, EMAIL);

    assertNotEquals(
        response1.getId(),
        response.getId(),
        "Snapshot Access Request Response should have unique request id");
    assertNotEquals(
        response1.getCreatedDate(),
        response.getCreatedDate(),
        "Snapshot Access Request Response should have unique create date timestamp");
  }

  @Test
  void createSnapshotIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () -> snapshotRequestDao.create(UUID.randomUUID(), snapshotAccessRequest, EMAIL));
  }

  @Test
  void update_old() {
    SnapshotAccessRequestResponse response = createRequest_old();
    assertNull(response.getUpdatedDate(), "Response was never updated.");
    verifyResponseContents(response, true);

    SnapshotAccessRequestResponse updatedResponse =
        snapshotRequestDao.update(response.getId(), SnapshotAccessRequestStatus.APPROVED);

    assertThat(
        "Updated Snapshot Access Request Response should have approved status",
        updatedResponse.getStatus(),
        equalTo(SnapshotAccessRequestStatus.APPROVED));
    assertNotNull(
        updatedResponse.getUpdatedDate(),
        "Updated Snapshot Access Request Response should have an update date");
  }

  @Test
  void update() {
    SnapshotAccessRequestResponse response = createRequest();
    assertNull(response.getUpdatedDate(), "Response was never updated.");
    verifyResponseContents(response, false);

    SnapshotAccessRequestResponse updatedResponse =
        snapshotRequestDao.update(response.getId(), SnapshotAccessRequestStatus.APPROVED);

    assertThat(
        "Updated Snapshot Access Request Response should have approved status",
        updatedResponse.getStatus(),
        equalTo(SnapshotAccessRequestStatus.APPROVED));
    assertNotNull(
        updatedResponse.getUpdatedDate(),
        "Updated Snapshot Access Request Response should have an update date");
  }

  @Test
  void updateIdNotFound() {
    System.out.println(TestUtils.mapToJson(SnapshotBuilderTestData.SETTINGS));
    assertThrows(
        NotFoundException.class,
        () -> snapshotRequestDao.update(UUID.randomUUID(), SnapshotAccessRequestStatus.APPROVED));
  }

  @Test
  void delete() {
    SnapshotAccessRequestResponse response = createRequest_old();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.getById(response.getId()),
        equalTo(response));
    snapshotRequestDao.delete(response.getId());
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(response.getId()));
  }

  @Test
  void deleteNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.delete(UUID.randomUUID()));
  }
}
