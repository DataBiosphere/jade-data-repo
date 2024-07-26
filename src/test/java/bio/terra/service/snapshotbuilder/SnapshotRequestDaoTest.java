package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotAccessRequestStatus;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import java.io.IOException;
import java.util.Set;
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
  private Snapshot sourceSnapshot;
  private Snapshot createdSnapshot;
  private SnapshotAccessRequest snapshotAccessRequest;

  private static final String EMAIL = "user@gmail.com";
  private static final String FLIGHT_ID = "flight_id";

  @BeforeEach
  void beforeEach() throws IOException {
    dataset = daoOperations.createDataset(DaoOperations.DATASET_MINIMAL);
    sourceSnapshot = daoOperations.createAndIngestSnapshot(dataset, DaoOperations.SNAPSHOT_MINIMAL);
    createdSnapshot =
        daoOperations.createAndIngestSnapshot(dataset, DaoOperations.SNAPSHOT_MINIMAL);
    snapshotAccessRequest =
        SnapshotBuilderTestData.createSnapshotAccessRequest(sourceSnapshot.getId());
  }

  private SnapshotAccessRequestResponse createRequest() {
    return snapshotRequestDao.create(snapshotAccessRequest, EMAIL);
  }

  private SnapshotAccessRequestResponse createRequest(SnapshotAccessRequest snapshotAccessRequest) {
    return snapshotRequestDao.create(snapshotAccessRequest, EMAIL);
  }

  private void verifyResponseContents(SnapshotAccessRequestResponse response) {
    SnapshotAccessRequestResponse expected =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(sourceSnapshot.getId());
    expected.sourceSnapshotId(sourceSnapshot.getId());

    expected.createdBy(EMAIL);
    expected.status(SnapshotAccessRequestStatus.SUBMITTED);
    assertThat(
        "Given response is the same as expected.",
        response,
        samePropertyValuesAs(
            expected, "id", "createdDate", "datasetId", "flightid", "createdSnapshotId"));
    assertNotNull(response.getId(), "Snapshot Access Request Response should have an id");
    assertNotNull(
        response.getCreatedDate(),
        "Snapshot Access Request Response should have a create date timestamp");
  }

  @Test
  void getById() {
    SnapshotAccessRequestResponse response = createRequest();
    SnapshotAccessRequestResponse retrieved = snapshotRequestDao.getById(response.getId());
    verifyResponseContents(retrieved);
  }

  @Test
  void getByIdNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(UUID.randomUUID()));
  }

  @Test
  void enumerate() {
    SnapshotAccessRequestResponse response = createRequest();
    SnapshotAccessRequestResponse response1 = createRequest();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerate(Set.of(response.getId(), response1.getId())),
        contains(response, response1));
  }

  @Test
  void enumerateIgnoresDeletedRequests() {
    SnapshotAccessRequestResponse response = createRequest();
    SnapshotAccessRequestResponse response1 = createRequest();
    snapshotRequestDao.updateStatus(response1.getId(), SnapshotAccessRequestStatus.DELETED);
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerate(Set.of(response.getId(), response1.getId())),
        contains(response));
  }

  @Test
  void enumerateBySnapshotId() throws IOException {
    Snapshot secondSnapshot =
        daoOperations.createAndIngestSnapshot(dataset, DaoOperations.SNAPSHOT_MINIMAL);
    SnapshotAccessRequestResponse response = createRequest();
    createRequest(SnapshotBuilderTestData.createSnapshotAccessRequest(secondSnapshot.getId()));
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerateBySnapshot(sourceSnapshot.getId()),
        contains(response));
  }

  @Test
  void enumerateBySnapshotIdExcludesDeletedRequests() throws IOException {
    Snapshot secondSnapshot =
        daoOperations.createAndIngestSnapshot(dataset, DaoOperations.SNAPSHOT_MINIMAL);
    SnapshotAccessRequestResponse response = createRequest();
    createRequest(SnapshotBuilderTestData.createSnapshotAccessRequest(secondSnapshot.getId()));
    snapshotRequestDao.updateStatus(response.getId(), SnapshotAccessRequestStatus.DELETED);
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerateBySnapshot(sourceSnapshot.getId()).size(),
        is(0));
  }

  @Test
  void enumerateNotFound() {
    assertThat(
        "For a dataset id that does not exist nothing is returned",
        snapshotRequestDao.enumerate(Set.of()),
        empty());
  }

  @Test
  void create() {
    SnapshotAccessRequestResponse response = createRequest();
    verifyResponseContents(response);

    SnapshotAccessRequestResponse response1 =
        snapshotRequestDao.create(snapshotAccessRequest, EMAIL);

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
        () ->
            snapshotRequestDao.create(
                snapshotAccessRequest.sourceSnapshotId(UUID.randomUUID()), EMAIL));
  }

  @Test
  void updateStatus() {
    SnapshotAccessRequestResponse response = createRequest();
    assertNull(response.getStatusUpdatedDate(), "Status was never updated.");
    verifyResponseContents(response);
    snapshotRequestDao.updateStatus(response.getId(), SnapshotAccessRequestStatus.APPROVED);
    SnapshotAccessRequestResponse updatedResponse = snapshotRequestDao.getById(response.getId());
    assertThat(
        "Updated Snapshot Access Request Response should have approved status",
        updatedResponse.getStatus(),
        equalTo(SnapshotAccessRequestStatus.APPROVED));
    assertNotNull(
        updatedResponse.getStatusUpdatedDate(),
        "Updated Snapshot Access Request Response should have a status update date");
  }

  @Test
  void updateStatusIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () ->
            snapshotRequestDao.updateStatus(
                UUID.randomUUID(), SnapshotAccessRequestStatus.APPROVED));
  }

  @Test
  void updateFlightId() {
    SnapshotAccessRequestResponse response = createRequest();
    verifyResponseContents(response);
    snapshotRequestDao.updateFlightId(response.getId(), FLIGHT_ID);
    SnapshotAccessRequestResponse updatedResponse = snapshotRequestDao.getById(response.getId());

    // only the flightId is updated
    verifyResponseContents(updatedResponse);
    assertThat(
        "Updated Snapshot Access Request Response should have flight id",
        updatedResponse.getFlightid(),
        equalTo(FLIGHT_ID));
  }

  @Test
  void updateFlightIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () -> snapshotRequestDao.updateFlightId(UUID.randomUUID(), FLIGHT_ID));
  }

  @Test
  void updateCreatedSnapshotId() {
    SnapshotAccessRequestResponse response = createRequest();
    verifyResponseContents(response);
    snapshotRequestDao.updateCreatedSnapshotId(response.getId(), createdSnapshot.getId());
    SnapshotAccessRequestResponse updatedResponse = snapshotRequestDao.getById(response.getId());

    // only the createdSnapshotId is updated
    verifyResponseContents(updatedResponse);
    assertThat(
        "Updated Snapshot Access Request Response should have created snapshot id",
        updatedResponse.getCreatedSnapshotId(),
        equalTo(createdSnapshot.getId()));
  }

  @Test
  void updateCreatedSnapshotSourceIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () ->
            snapshotRequestDao.updateCreatedSnapshotId(UUID.randomUUID(), createdSnapshot.getId()));
  }

  @Test
  void updateCreatedSnapshotIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () ->
            snapshotRequestDao.updateCreatedSnapshotId(sourceSnapshot.getId(), UUID.randomUUID()));
  }

  @Test
  void deleteSourceSnapshot() {
    SnapshotAccessRequestResponse response = createRequest();
    assertThat(
        "We can retrieve the snapshot request",
        snapshotRequestDao.getById(response.getId()),
        equalTo(response));
    // delete the source snapshot
    // This should also delete the snapshot request
    daoOperations.deleteSnapshot(sourceSnapshot.getId());
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(response.getId()));
  }

  @Test
  void deleteNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.delete(UUID.randomUUID()));
  }
}
