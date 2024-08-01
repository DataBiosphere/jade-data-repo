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
  private bio.terra.model.SnapshotAccessRequest snapshotAccessRequest;

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

  private SnapshotAccessRequestModel createRequest() {
    return snapshotRequestDao.create(snapshotAccessRequest, EMAIL);
  }

  private SnapshotAccessRequestModel createRequest(SnapshotAccessRequest snapshotAccessRequest) {
    return snapshotRequestDao.create(snapshotAccessRequest, EMAIL);
  }

  private void verifyResponseContents(SnapshotAccessRequestModel response) {
    SnapshotAccessRequestModel expected =
        SnapshotBuilderTestData.createSnapshotAccessRequestModel(sourceSnapshot.getId());

    assertThat(
        "Given response is the same as expected.",
        response,
        samePropertyValuesAs(expected, "id", "createdDate", "flightid", "createdSnapshotId"));
    assertNotNull(response.id(), "Snapshot Access Request Response should have an id");
    assertNotNull(
        response.createdDate(),
        "Snapshot Access Request Response should have a create date timestamp");
  }

  @Test
  void getById() {
    SnapshotAccessRequestModel response = createRequest();
    SnapshotAccessRequestModel retrieved = snapshotRequestDao.getById(response.id());
    verifyResponseContents(retrieved);
  }

  @Test
  void getByIdNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(UUID.randomUUID()));
  }

  @Test
  void enumerate() {
    SnapshotAccessRequestModel response = createRequest();
    SnapshotAccessRequestModel response1 = createRequest();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerate(Set.of(response.id(), response1.id())),
        contains(response, response1));
  }

  @Test
  void enumerateIgnoresDeletedRequests() {
    SnapshotAccessRequestModel response = createRequest();
    SnapshotAccessRequestModel response1 = createRequest();
    snapshotRequestDao.updateStatus(response1.id(), SnapshotAccessRequestStatus.DELETED);
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerate(Set.of(response.id(), response1.id())),
        contains(response));
  }

  @Test
  void enumerateBySnapshotId() throws IOException {
    Snapshot secondSnapshot =
        daoOperations.createAndIngestSnapshot(dataset, DaoOperations.SNAPSHOT_MINIMAL);
    SnapshotAccessRequestModel response = createRequest();
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
    SnapshotAccessRequestModel response = createRequest();
    createRequest(SnapshotBuilderTestData.createSnapshotAccessRequest(secondSnapshot.getId()));
    snapshotRequestDao.updateStatus(response.id(), SnapshotAccessRequestStatus.DELETED);
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
    SnapshotAccessRequestModel response = createRequest();
    verifyResponseContents(response);

    SnapshotAccessRequestModel response1 = snapshotRequestDao.create(snapshotAccessRequest, EMAIL);

    assertNotEquals(
        response1.id(),
        response.id(),
        "Snapshot Access Request Response should have unique request id");
    assertNotEquals(
        response1.createdDate(),
        response.createdDate(),
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
    SnapshotAccessRequestModel response = createRequest();
    assertNull(response.statusUpdatedDate(), "Status was never updated.");
    verifyResponseContents(response);
    snapshotRequestDao.updateStatus(response.id(), SnapshotAccessRequestStatus.APPROVED);
    SnapshotAccessRequestModel updatedResponse = snapshotRequestDao.getById(response.id());
    assertThat(
        "Updated Snapshot Access Request Response should have approved status",
        updatedResponse.status(),
        equalTo(SnapshotAccessRequestStatus.APPROVED));
    assertNotNull(
        updatedResponse.statusUpdatedDate(),
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
    SnapshotAccessRequestModel response = createRequest();
    verifyResponseContents(response);
    snapshotRequestDao.updateFlightId(response.id(), FLIGHT_ID);
    SnapshotAccessRequestModel updatedResponse = snapshotRequestDao.getById(response.id());

    // only the flightId is updated
    verifyResponseContents(updatedResponse);
    assertThat(
        "Updated Snapshot Access Request Response should have flight id",
        updatedResponse.flightid(),
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
    SnapshotAccessRequestModel response = createRequest();
    verifyResponseContents(response);
    snapshotRequestDao.updateCreatedSnapshotId(response.id(), createdSnapshot.getId());
    SnapshotAccessRequestModel updatedResponse = snapshotRequestDao.getById(response.id());

    // only the createdSnapshotId is updated
    verifyResponseContents(updatedResponse);
    assertThat(
        "Updated Snapshot Access Request Response should have created snapshot id",
        updatedResponse.createdSnapshotId(),
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
    SnapshotAccessRequestModel response = createRequest();
    UUID responseId = response.id();
    assertThat(
        "We can retrieve the snapshot request",
        snapshotRequestDao.getById(responseId),
        equalTo(response));
    // delete the source snapshot
    // This should also delete the snapshot request
    daoOperations.deleteSnapshot(sourceSnapshot.getId());
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(responseId));
  }

  @Test
  void deleteNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.delete(UUID.randomUUID()));
  }
}
