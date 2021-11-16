package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileDao;
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
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
@EmbeddedDatabaseTest
public class GoogleResourceDaoUnitTest {

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao googleResourceDao;

  private BillingProfileModel billingProfile;
  private List<GoogleProjectResource> projects;

  @Before
  public void setup() throws IOException, InterruptedException {
    // Initialize list;
    projects = new ArrayList<>();

    // One billing profile
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");

    // two google project resources
    GoogleProjectResource projectResource1 = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = googleResourceDao.createProject(projectResource1);
    projectResource1.id(projectId);
    projects.add(projectResource1);

    GoogleProjectResource projectResource2 = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId2 = googleResourceDao.createProject(projectResource2);
    projectResource2.id(projectId2);
    projects.add(projectResource2);
  }

  @After
  public void teardown() {
    for (GoogleProjectResource project : projects) {
      googleResourceDao.deleteProject(project.getId());
    }
    profileDao.deleteBillingProfileById(billingProfile.getId());
  }

  @Test
  public void twoDatasetsTwoBillingProfilesTwoBuckets() throws Exception {
    List<GoogleProjectResource> retrievedProjects =
        googleResourceDao.retrieveProjectsByBillingProfileId(billingProfile.getId());

    assertThat("Project Count should be 2", retrievedProjects.size(), equalTo(2));
  }
}
