package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class GoogleResourceDaoUnitTest {

  @Autowired private ProfileDao profileDao;

  @Autowired private DatasetDao datasetDao;

  @Autowired private GoogleResourceDao googleResourceDao;

  @Autowired private JsonLoader jsonLoader;

  private BillingProfileModel billingProfile;
  private List<GoogleProjectResource> projects;
  private List<UUID> projectResourceIds;
  private List<UUID> datasetIds;

  @Before
  public void setup() throws IOException, InterruptedException {
    // Initialize list;
    projects = new ArrayList<>();
    projectResourceIds = new ArrayList<>();
    datasetIds = new ArrayList<>();

    // One billing profile
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    // two google project resources
    GoogleProjectResource projectResource1 = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = googleResourceDao.createProject(projectResource1);
    projectResource1.id(projectId);
    projectResourceIds.add(projectId);
    projects.add(projectResource1);

    GoogleProjectResource projectResource2 = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId2 = googleResourceDao.createProject(projectResource2);
    projectResource2.id(projectId2);
    projectResourceIds.add(projectId2);
    projects.add(projectResource2);
  }

  @After
  public void teardown() {
    for (UUID datasetId : datasetIds) {
      datasetDao.delete(datasetId);
    }
    for (GoogleProjectResource project : projects) {
      googleResourceDao.deleteProject(project.getId());
    }
    profileDao.deleteBillingProfileById(billingProfile.getId());
  }

  @Test
  public void twoDatasetsTwoBillingProfilesTwoBuckets() {
    List<GoogleProjectResource> retrievedProjects =
        googleResourceDao.retrieveProjectsByBillingProfileId(billingProfile.getId());

    assertThat("Project Count should be 2", retrievedProjects.size(), equalTo(2));
  }

  @Test
  public void testMarkForDelete() {
    UUID project1Id = projects.get(0).getId();
    UUID project2Id = projects.get(1).getId();

    confirmNotFoundByDelete(project1Id);
    confirmNotFoundByDelete(project2Id);

    // mark the two projects for delete
    // They're not used for any resources, so this should succeed
    googleResourceDao.markUnusedProjectsForDelete(projectResourceIds);

    assertThat(
        "Should be able to retrieve the project 1 'for delete' since it was marked for delete",
        googleResourceDao.retrieveProjectByIdForDelete(project1Id).getId(),
        equalTo(project1Id));
    assertThat(
        "Should be able to retrieve the project 2 'for delete' since it was marked for delete",
        googleResourceDao.retrieveProjectByIdForDelete(project2Id).getId(),
        equalTo(project2Id));
  }

  @Test
  public void testMarkForDeleteWhenProjectInUse() throws IOException {
    GoogleProjectResource project1 = projects.get(0);
    GoogleProjectResource project2 = projects.get(1);
    createMinimalDataset(project1);
    googleResourceDao.markUnusedProjectsForDelete(projectResourceIds);

    // Since project1 is in use by dataset1, we should not mark it for delete
    confirmNotFoundByDelete(project1.getId());
    // Project 2 is not in use, so we should have still marked it for delete
    assertThat(
        "Should be able to retrieve the project 2 'for delete' since it was marked for delete",
        googleResourceDao.retrieveProjectByIdForDelete(project2.getId()).getId(),
        equalTo(project2.getId()));
  }

  private Dataset createMinimalDataset(GoogleProjectResource projectResource) throws IOException {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
    String newName = datasetRequest.getName() + UUID.randomUUID();
    datasetRequest
        .name(newName)
        .defaultProfileId(billingProfile.getId())
        .cloudPlatform(CloudPlatform.GCP);
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectResource.getId());
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    datasetIds.add(datasetId);
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);

    return dataset;
  }

  private void confirmNotFoundByDelete(UUID projectResourceId) {
    boolean errorCaught = false;
    try {
      googleResourceDao.retrieveProjectByIdForDelete(projectResourceId);
    } catch (GoogleResourceNotFoundException ex) {
      errorCaught = true;
    }
    assertThat(
        "Should not be able to retrieve project 'for delete' b/c it hasn't been marked for delete",
        errorCaught);
  }
}
