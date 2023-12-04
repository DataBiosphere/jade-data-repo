package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import bio.terra.common.EmbeddedDatabaseTest;
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
import java.util.Collections;
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

  private SnapshotAccessRequestResponse response;
  private String email;

  @BeforeEach
  void setUp() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);

    dataset = daoOperations.createMinimalDataset(billingProfile.getId(), projectId);

    snapshotAccessRequest = SnapshotBuilderTestData.ACCESS_REQUEST;
    email = "user@gmail.com";
    response = snapshotRequestDao.create(dataset.getId(), snapshotAccessRequest, email);
  }

  @Test
  void getById() {
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.getById(response.getId()),
        equalTo(response));
  }

  @Test
  void getByIdNotFound() {
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(UUID.randomUUID()));
  }

  @Test
  void enumerateByDatasetId() {
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.enumerateByDatasetId(dataset.getId()),
        equalTo(Collections.singletonList(response)));
  }

  @Test
  void enumerateByDatasetIdNotFound() {
    assertThat(
        "For a dataset id that does not exist nothing is returned",
        snapshotRequestDao.enumerateByDatasetId(UUID.randomUUID()).isEmpty());
  }

  @Test
  void create() {
    SnapshotAccessRequestResponse response1 =
        snapshotRequestDao.create(dataset.getId(), snapshotAccessRequest, email);
    Assertions.assertNotEquals(
        response1.getId(),
        response.getId(),
        "Snapshot Access Request Response should have unique request id");
    Assertions.assertNotEquals(
        response1.getCreateDate(),
        response.getCreateDate(),
        "Snapshot Access Request Response should have unique create date timestamp");
    assertThat(
        "Snapshot Access Request Response should contain the same datasetId as the example",
        response1.getDatasetId(),
        equalTo(response.getDatasetId()));
    assertThat(
        "Snapshot Access Request Response should contain the same name as the example",
        response1.getRequestName(),
        equalTo(response.getRequestName()));
    assertThat(
        "Snapshot Access Request Response should contain the same research purpose as the example",
        response1.getRequestResearchPurpose(),
        equalTo(response.getRequestResearchPurpose()));
    assertThat(
        "Snapshot Access Request Response should contain the same snapshot builder request as the example",
        response1.getRequest(),
        equalTo(response.getRequest()));
    assertThat(
        "Snapshot Access Request Response should contain the same user email as the example",
        response1.getUserEmail(),
        equalTo(response.getUserEmail()));
    assertThat(
        "New Snapshot Access Request Response should have submitted status",
        response1.getStatus(),
        equalTo(SnapshotAccessRequestResponse.StatusEnum.SUBMITTED));
  }

  @Test
  void createDatasetIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () -> snapshotRequestDao.create(UUID.randomUUID(), snapshotAccessRequest, email));
  }

  @Test
  void update() {
    Assert.assertNull("Response was never updated.", response.getUpdateDate());
    Assert.assertEquals(
        "New Snapshot Access Request Responses should have submitted status",
        response.getStatus(),
        SnapshotAccessRequestResponse.StatusEnum.SUBMITTED);

    SnapshotAccessRequestResponse response1 =
        snapshotRequestDao.update(
            response.getId(), SnapshotAccessRequestResponse.StatusEnum.APPROVED);

    Assertions.assertEquals(
        response1.getId(),
        response.getId(),
        "Updated Snapshot Access Request should have the same request id");
    Assertions.assertEquals(
        response1.getCreateDate(),
        response.getCreateDate(),
        "Updated Snapshot Access Request should maintain their create date timestamp");
    assertThat(
        "Updated Snapshot Access Request Response should maintain the same datasetId",
        response1.getDatasetId(),
        equalTo(response.getDatasetId()));
    assertThat(
        "Updates Snapshot Access Request Response should maintain the same name",
        response1.getRequestName(),
        equalTo(response.getRequestName()));
    assertThat(
        "Updated Snapshot Access Request Response should maintain the same research purpose",
        response1.getRequestResearchPurpose(),
        equalTo(response.getRequestResearchPurpose()));
    assertThat(
        "Updated Snapshot Access Request Response should contain the same snapshot builder request",
        response1.getRequest(),
        equalTo(response.getRequest()));
    assertThat(
        "Updated Snapshot Access Request Response should maintain the same user email",
        response1.getUserEmail(),
        equalTo(response.getUserEmail()));
    assertThat(
        "Updated Snapshot Access Request Response should have approved status",
        response1.getStatus(),
        equalTo(SnapshotAccessRequestResponse.StatusEnum.APPROVED));
    assertNotNull(
        "Updated Snapshot Access Request Response should have an update date",
        response1.getUpdateDate());
  }

  @Test
  void updateIdNotFound() {
    assertThrows(
        NotFoundException.class,
        () ->
            snapshotRequestDao.update(
                UUID.randomUUID(), SnapshotAccessRequestResponse.StatusEnum.APPROVED));
  }

  @Test
  void delete() {
    assertThat(
        "Snapshot Access Request should be the same as the example",
        snapshotRequestDao.getById(response.getId()),
        equalTo(response));
    snapshotRequestDao.delete(response.getId());
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.getById(response.getId()));
  }

  @Test
  void deleteNotFound() {
    snapshotRequestDao.delete(response.getId());
    assertThrows(NotFoundException.class, () -> snapshotRequestDao.delete(response.getId()));
  }
}
