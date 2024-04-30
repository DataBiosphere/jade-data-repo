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
  private Snapshot snapshot;
  private SnapshotAccessRequest snapshotAccessRequest;

  private static final String EMAIL = "user@gmail.com";

  @BeforeEach
  void beforeEach() throws IOException {
    dataset = daoOperations.createDataset(DaoOperations.DATASET_MINIMAL);
    snapshotAccessRequest = SnapshotBuilderTestData.createSnapshotAccessRequest(dataset.getId());
    snapshot = daoOperations.createAndIngestSnapshot(dataset, DaoOperations.SNAPSHOT_MINIMAL);
    snapshotAccessRequest = SnapshotBuilderTestData.createSnapshotAccessRequest(snapshot.getId());
  }

  private SnapshotAccessRequestResponse createRequest() {
    return snapshotRequestDao.create(snapshotAccessRequest, EMAIL);
  }

  private void verifyResponseContents(SnapshotAccessRequestResponse response) {
    SnapshotAccessRequestResponse expected =
        SnapshotBuilderTestData.createSnapshotAccessRequestResponse(snapshot.getId());
    expected.sourceSnapshotId(snapshot.getId());

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
        NotFoundException.class, () -> snapshotRequestDao.create(snapshotAccessRequest, EMAIL));
  }

  @Test
  void update() {
    SnapshotAccessRequestResponse response = createRequest();
    assertNull(response.getUpdatedDate(), "Response was never updated.");
    verifyResponseContents(response);

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
  void deleteNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.delete(UUID.randomUUID()));
  }
}
