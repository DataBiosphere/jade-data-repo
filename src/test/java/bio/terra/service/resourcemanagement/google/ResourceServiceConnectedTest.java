package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.CollectionType;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.BufferService;
import com.google.api.client.util.Lists;
import com.google.api.services.cloudresourcemanager.model.Project;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
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
public class ResourceServiceConnectedTest {

  @Autowired private GoogleResourceConfiguration resourceConfiguration;
  @Autowired private GoogleProjectService projectService;
  @Autowired private GoogleResourceManagerService resourceManagerService;
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
    ResourceInfo resource = bufferService.handoutResource(false);
    String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();

    String role = "roles/bigquery.jobUser";
    String stewardsGroupEmail = "group:JadeStewards-dev@dev.test.firecloud.org";
    List<String> stewardsGroupEmailList = Lists.newArrayList();
    stewardsGroupEmailList.add(stewardsGroupEmail);
    Map<String, List<String>> roleToStewardMap = new HashMap<>();
    roleToStewardMap.put(role, stewardsGroupEmailList);

    GoogleProjectResource projectResource =
        projectService.initializeGoogleProject(
            projectId,
            profile,
            roleToStewardMap,
            GoogleRegion.DEFAULT_GOOGLE_REGION,
            Map.of("test-name", "resource-service-connected-test"),
            CollectionType.DATASET);

    Project project = resourceManagerService.getProject(projectId);
    assertThat("the project is active", project.getLifecycleState(), equalTo("ACTIVE"));
    assertThat(
        "the project has the correct label",
        project.getLabels(),
        hasEntry(equalTo("test-name"), equalTo("resource-service-connected-test")));
    assertThat(
        "the project has the correct name", project.getName(), equalTo("TDR Dataset Project"));

    // TODO check to make sure a steward can complete a job in another test

    projectService.deleteGoogleProject(projectResource.getId());
    project = resourceManagerService.getProject(projectId);
    assertThat(
        "the project is not active after delete",
        project.getLifecycleState(),
        not(equalTo("ACTIVE")));
  }
}
