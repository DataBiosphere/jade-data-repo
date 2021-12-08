package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSecurityClassification;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.BufferService;
import com.google.api.client.util.Lists;
import com.google.api.services.cloudresourcemanager.model.Project;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class ResourceServiceConnectedTest {

  @Autowired private GoogleResourceConfiguration resourceConfiguration;
  @Autowired private BufferService bufferService;
  @Autowired private GoogleProjectService projectService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private ConfigurationService configService;

  @MockBean private IamProviderInterface samService;

  private BillingProfileModel profile;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    configService.reset();
    profile = connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
  }

  @After
  public void teardown() throws Exception {
    connectedOperations.teardown();
  }

  @Test
  public void createAndDeleteProjectTest() throws Exception {
    ResourceInfo resource = bufferService.handoutResource(DatasetSecurityClassification.NONE);
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
            Map.of("test-name", "resource-service-connected-test"));

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
