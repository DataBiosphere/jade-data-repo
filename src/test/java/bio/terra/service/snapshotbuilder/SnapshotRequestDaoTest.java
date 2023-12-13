package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.SnapshotAccessRequest;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
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
  @Autowired private ProfileDao profileDao;
  @Autowired private GoogleResourceDao resourceDao;
  @Autowired private SnapshotRequestDao snapshotRequestDao;

  private Dataset dataset;
  private SnapshotAccessRequest snapshotAccessRequest;

  private static final String EMAIL = "user@gmail.com";

  @BeforeEach
  void beforeEach() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    BillingProfileModel billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    dataset = daoOperations.createMinimalDataset(billingProfile.getId(), projectId);

    snapshotAccessRequest = SnapshotBuilderTestData.createSnapshotAccessRequest();
  }

  private SnapshotAccessRequestResponse createRequest() {
    return snapshotRequestDao.create(dataset.getId(), snapshotAccessRequest, EMAIL);
  }

  private void verifyResponseContents(SnapshotAccessRequestResponse response) {
    assertNotNull(
        response.getId(), "Snapshot Access Request Response should have an id");
    assertNotNull(
        response.getCreatedDate(),
        "Snapshot Access Request Response should have a create date timestamp");
    assertThat(
        "Snapshot Access Request Response should contain the same datasetId as the example",
        response.getDatasetId(),
        equalTo(dataset.getId()));
    assertThat(
        "Snapshot Access Request Response should contain the same name as the example",
        response.getSnapshotName(),
        equalTo(SnapshotBuilderTestData.createSnapshotAccessRequestResponse().getSnapshotName()));
    assertThat(
        "Snapshot Access Request Response should contain the same research purpose as the example",
        response.getSnapshotResearchPurpose(),
        equalTo(SnapshotBuilderTestData.createSnapshotAccessRequestResponse().getSnapshotResearchPurpose()));
    assertThat(
        "Snapshot Access Request Response should contain the same snapshot builder request as the example",
        response.getSnapshotSpecification(),
        equalTo(SnapshotBuilderTestData.createSnapshotBuilderRequest()));
    assertThat(
        "Snapshot Access Request Response should contain the same user email as the example",
        response.getCreatedBy(),
        equalTo(EMAIL));
    assertThat(
        "New Snapshot Access Request Response should have submitted status",
        response.getStatus(),
        equalTo(SnapshotAccessRequestResponse.StatusEnum.SUBMITTED));
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
  void enumerateByDatasetId() {
    SnapshotAccessRequestResponse response = createRequest();
    SnapshotAccessRequestResponse response1 = createRequest();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerateByDatasetId(dataset.getId()),
        contains(response, response1));
  }

  @Test
  void enumerateByDatasetIdNotFound() {
    assertThat(
        "For a dataset id that does not exist nothing is returned",
        snapshotRequestDao.enumerateByDatasetId(UUID.randomUUID()),
        empty());
  }

  @Test
  void create() {
    SnapshotAccessRequestResponse response = createRequest();
    verifyResponseContents(response);

    SnapshotAccessRequestResponse response1 =
        snapshotRequestDao.create(dataset.getId(), snapshotAccessRequest, EMAIL);

    Assertions.assertNotEquals(
        response1.getId(),
        response.getId(),
        "Snapshot Access Request Response should have unique request id");
    Assertions.assertNotEquals(
        response1.getCreatedDate(),
        response.getCreatedDate(),
        "Snapshot Access Request Response should have unique create date timestamp");
  }

  @Test
  void createDatasetIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () -> snapshotRequestDao.create(UUID.randomUUID(), snapshotAccessRequest, EMAIL));
  }

  @Test
  void update() {
    SnapshotAccessRequestResponse response = createRequest();
    assertNull(response.getUpdatedDate(), "Response was never updated.");
    verifyResponseContents(response);

    SnapshotAccessRequestResponse updatedResponse =
        snapshotRequestDao.update(
            response.getId(), SnapshotAccessRequestResponse.StatusEnum.APPROVED);

    assertThat(
        "Updated Snapshot Access Request Response should have approved status",
        updatedResponse.getStatus(),
        equalTo(SnapshotAccessRequestResponse.StatusEnum.APPROVED));
    assertNotNull(updatedResponse.getUpdatedDate(), "Updated Snapshot Access Request Response should have an update date");
  }

  @Test
  void updateIdNotFound() {
    System.out.println(TestUtils.mapToJson(SnapshotBuilderTestData.SETTINGS));
    assertThrows(
        NotFoundException.class,
        () ->
            snapshotRequestDao.update(
                UUID.randomUUID(), SnapshotAccessRequestResponse.StatusEnum.APPROVED));
  }

  @Test
  void delete() {
    SnapshotAccessRequestResponse response = createRequest();
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
