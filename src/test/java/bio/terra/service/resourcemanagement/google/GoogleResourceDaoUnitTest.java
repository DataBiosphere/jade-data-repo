package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.filedata.google.gcs.GcsConfiguration;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.exception.GoogleResourceNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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

  @Autowired private DaoOperations daoOperations;

  @Autowired private NamedParameterJdbcTemplate jdbcTemplate;
  @Mock private GcsConfiguration gcsConfiguration;
  @Mock private GoogleResourceConfiguration googleResourceConfiguration;
  private static final String TDR_SERVICE_ACCOUNT_EMAIL = "tdr-sa@a.com";
  private GoogleResourceDao googleResourceDao;

  private BillingProfileModel billingProfile;
  private List<UUID> projectResourceIds;
  private List<UUID> datasetIds;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @Before
  public void setup() throws IOException, InterruptedException, SQLException {
    googleResourceDao =
        new GoogleResourceDao(
            jdbcTemplate, gcsConfiguration, googleResourceConfiguration, TDR_SERVICE_ACCOUNT_EMAIL);

    // Initialize lists
    projectResourceIds = new ArrayList<>();
    datasetIds = new ArrayList<>();

    // One billing profile
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    // Two google project resources
    IntStream.range(0, 2)
        .forEach(
            i -> {
              GoogleProjectResource project =
                  ResourceFixtures.randomProjectResource(billingProfile);
              projectResourceIds.add(googleResourceDao.createProject(project));
            });
  }

  @After
  public void teardown() {
    for (UUID datasetId : datasetIds) {
      datasetDao.delete(datasetId);
    }
    for (UUID projectResourceId : projectResourceIds) {
      googleResourceDao.deleteProject(projectResourceId);
    }
    profileDao.deleteBillingProfileById(billingProfile.getId());
  }

  /* Helper method to create a minimal dataset and register its ID for cleanup */
  private Dataset createDataset(UUID projectResourceId) throws IOException {
    Dataset dataset =
        daoOperations.createMinimalDataset(billingProfile.getId(), projectResourceId, TEST_USER);
    datasetIds.add(dataset.getId());
    return dataset;
  }

  @Test
  public void twoDatasetsTwoBillingProfilesTwoBuckets() {
    List<GoogleProjectResource> retrievedProjects =
        googleResourceDao.retrieveProjectsByBillingProfileId(billingProfile.getId());

    assertThat("Project Count should be 2", retrievedProjects, hasSize(2));
    retrievedProjects.forEach(
        project ->
            assertFalse(
                "Projects by default use the general TDR SA",
                project.hasDedicatedServiceAccount()));

    UUID projectId1 = projectResourceIds.get(0);
    UUID projectId2 = projectResourceIds.get(1);
    String dedicatedSa = "dedicated-sa@gmail.com";
    googleResourceDao.updateProjectResourceServiceAccount(projectId1, dedicatedSa);
    assertTrue(
        "Dedicated service account is detected",
        googleResourceDao.retrieveProjectById(projectId1).hasDedicatedServiceAccount());
    assertFalse(
        "Unaltered project still uses the general TDR SA",
        googleResourceDao.retrieveProjectById(projectId2).hasDedicatedServiceAccount());

    googleResourceDao.updateProjectResourceServiceAccount(projectId1, TDR_SERVICE_ACCOUNT_EMAIL);
    assertFalse(
        "Project explicitly using general TDR SA is registered as such",
        googleResourceDao.retrieveProjectById(projectId1).hasDedicatedServiceAccount());
  }

  @Test
  public void testMarkForDelete() {
    projectResourceIds.forEach(this::confirmNotFoundByDelete);

    // mark the projects for delete
    // They're not used for any resources, so this should succeed
    googleResourceDao.markUnusedProjectsForDelete(projectResourceIds);

    projectResourceIds.forEach(
        projectResourceId ->
            assertThat(
                "Should be able to retrieve project 'for delete' since it was marked for delete",
                googleResourceDao.retrieveProjectByIdForDelete(projectResourceId).getId(),
                equalTo(projectResourceId)));
  }

  @Test
  public void testMarkForDeleteWhenProjectInUse() throws IOException {
    createDataset(projectResourceIds.get(0));
    googleResourceDao.markUnusedProjectsForDelete(projectResourceIds);

    // Since project1 is in use by dataset1, we should not mark it for delete
    confirmNotFoundByDelete(projectResourceIds.get(0));
    // Project 2 is not in use, so we should have still marked it for delete
    assertThat(
        "Should be able to retrieve the project 2 'for delete' since it was marked for delete",
        googleResourceDao.retrieveProjectByIdForDelete(projectResourceIds.get(1)).getId(),
        equalTo(projectResourceIds.get(1)));
  }

  private void confirmNotFoundByDelete(UUID projectResourceId) {
    assertThrows(
        "Should not be able to retrieve project 'for delete' b/c it hasn't been marked for delete",
        GoogleResourceNotFoundException.class,
        () -> googleResourceDao.retrieveProjectByIdForDelete(projectResourceId));
  }
}
