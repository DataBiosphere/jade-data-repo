package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.category.OnDemand;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.BufferService;
import com.google.api.client.util.Lists;
import com.google.api.services.cloudresourcemanager.model.Project;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(OnDemand.class)
public class ResourceServiceConnectedTest {

  @Autowired private GoogleResourceConfiguration resourceConfiguration;
  @Autowired private GoogleProjectService projectService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private BufferService bufferService;

  private BillingProfileModel profile;

  @Before
  public void setup() throws Exception {
    profile = connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
  }

  @After
  public void teardown() throws Exception {
    connectedOperations.teardown();
  }

  @Test
  public void createAndDeleteProjectTest() throws Exception {
    // the project id can't be more than 30 characters
    HandoutRequestBody request =
        new HandoutRequestBody().handoutRequestId(UUID.randomUUID().toString());
    ResourceInfo resource = bufferService.handoutResource(request);
    String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();

    String role = "roles/bigquery.jobUser";
    String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
    List<String> stewardsGroupEmailList = Lists.newArrayList();
    stewardsGroupEmailList.add(stewardsGroupEmail);
    Map<String, List<String>> roleToStewardMap = new HashMap<>();
    roleToStewardMap.put(role, stewardsGroupEmailList);

    GoogleProjectResource projectResource =
        projectService.getOrInitializeProject(
            projectId, profile, roleToStewardMap, GoogleRegion.DEFAULT_GOOGLE_REGION);

    Project project = projectService.getProject(projectId);
    assertThat("the project is active", project.getLifecycleState(), equalTo("ACTIVE"));

    // TODO check to make sure a steward can complete a job in another test

    projectService.deleteGoogleProject(projectResource.getId());
    project = projectService.getProject(projectId);
    assertThat(
        "the project is not active after delete",
        project.getLifecycleState(),
        not(equalTo("ACTIVE")));
  }
}
