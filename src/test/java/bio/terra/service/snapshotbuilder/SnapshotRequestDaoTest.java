package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

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
import org.junit.Assert;
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

  private BillingProfileModel billingProfile;
  private GoogleProjectResource projectResource;
  private Dataset dataset;
  private SnapshotAccessRequest snapshotAccessRequest;

  private static final String EMAIL = "user@gmail.com";

  @BeforeEach
  void setUp() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    dataset = daoOperations.createMinimalDataset(billingProfile.getId(), projectId);

    snapshotAccessRequest = SnapshotBuilderTestData.ACCESS_REQUEST;
  }

  private SnapshotAccessRequestResponse create() {
    return snapshotRequestDao.create(dataset.getId(), snapshotAccessRequest, EMAIL);
  }

  private void verifyResponseContents(SnapshotAccessRequestResponse response1) {
    Assertions.assertNotNull(
        response1.getId(), "Snapshot Access Request Response should have an id");
    Assertions.assertNotNull(
        response1.getCreatedDate(),
        "Snapshot Access Request Response should have a create date timestamp");
    assertThat(
        "Snapshot Access Request Response should contain the same datasetId as the example",
        response1.getDatasetId(),
        equalTo(dataset.getId()));
    assertThat(
        "Snapshot Access Request Response should contain the same name as the example",
        response1.getSnapshotName(),
        equalTo(SnapshotBuilderTestData.RESPONSE.getSnapshotName()));
    assertThat(
        "Snapshot Access Request Response should contain the same research purpose as the example",
        response1.getSnapshotResearchPurpose(),
        equalTo(SnapshotBuilderTestData.RESPONSE.getSnapshotResearchPurpose()));
    assertThat(
        "Snapshot Access Request Response should contain the same snapshot builder request as the example",
        response1.getSnapshotSpecification(),
        equalTo(SnapshotBuilderTestData.BUILDER_REQUEST));
    assertThat(
        "Snapshot Access Request Response should contain the same user email as the example",
        response1.getCreatedBy(),
        equalTo(EMAIL));
    assertThat(
        "New Snapshot Access Request Response should have submitted status",
        response1.getStatus(),
        equalTo(SnapshotAccessRequestResponse.StatusEnum.SUBMITTED));
  }

  @Test
  void getByIdTest() {
    SnapshotAccessRequestResponse response = create();
    SnapshotAccessRequestResponse retrieved = snapshotRequestDao.getById(response.getId());
    verifyResponseContents(retrieved);
  }

  @Test
  void getByIdNotFoundTest() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(UUID.randomUUID()));
  }

  @Test
  void enumerateByDatasetIdTest() {
    SnapshotAccessRequestResponse response = create();
    SnapshotAccessRequestResponse response1 = create();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerateByDatasetId(dataset.getId()),
        contains(response, response1));
  }

  @Test
  void enumerateByDatasetIdNotFoundTest() {
    assertThat(
        "For a dataset id that does not exist nothing is returned",
        snapshotRequestDao.enumerateByDatasetId(UUID.randomUUID()),
        empty());
  }

  @Test
  void createTest() {
    SnapshotAccessRequestResponse response = create();
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
  void createDatasetIdNotFoundTest() {
    assertThrows(
        NotFoundException.class,
        () -> snapshotRequestDao.create(UUID.randomUUID(), snapshotAccessRequest, EMAIL));
  }

  @Test
  void updateTest() {
    SnapshotAccessRequestResponse response = create();
    Assert.assertNull("Response was never updated.", response.getUpdatedDate());
    verifyResponseContents(response);

    SnapshotAccessRequestResponse response1 =
        snapshotRequestDao.update(
            response.getId(), SnapshotAccessRequestResponse.StatusEnum.APPROVED);

    assertThat(
        "Updated Snapshot Access Request Response should have approved status",
        response1.getStatus(),
        equalTo(SnapshotAccessRequestResponse.StatusEnum.APPROVED));
    assertNotNull(
        "Updated Snapshot Access Request Response should have an update date",
        response1.getUpdatedDate());
  }

  @Test
  void updateIdNotFoundTest() {
    System.out.println(TestUtils.mapToJson(SnapshotBuilderTestData.SETTINGS));
    assertThrows(
        NotFoundException.class,
        () ->
            snapshotRequestDao.update(
                UUID.randomUUID(), SnapshotAccessRequestResponse.StatusEnum.APPROVED));
  }

  @Test
  void deleteTest() {
    SnapshotAccessRequestResponse response = create();
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.getById(response.getId()),
        equalTo(response));
    snapshotRequestDao.delete(response.getId());
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(response.getId()));
  }

  @Test
  void deleteNotFoundTest() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.delete(UUID.randomUUID()));
  }
}
