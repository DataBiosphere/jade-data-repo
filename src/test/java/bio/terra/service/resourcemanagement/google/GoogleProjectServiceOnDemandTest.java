package bio.terra.service.resourcemanagement.google;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.OnDemand;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.resourcemanagement.BufferService;
import com.google.api.services.cloudresourcemanager.model.Project;
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
@Category(OnDemand.class)
@EmbeddedDatabaseTest
public class GoogleProjectServiceOnDemandTest {

  @Autowired private BufferService bufferService;
  @Autowired private GoogleResourceManagerService resourceManagerService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private GoogleProjectService googleProjectService;
  @MockBean private IamProviderInterface samService;

  private BillingProfileModel billingProfile;
  private GoogleRegion region = GoogleRegion.DEFAULT_GOOGLE_REGION;
  private GoogleProjectResource projectResource;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());
    ResourceInfo resource = bufferService.handoutResource(false);
    String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
    Project project = resourceManagerService.getProject(projectId);
    String googleProjectNumber = project.getProjectNumber().toString();
    String googleProjectId = project.getProjectId();
    projectResource =
        new GoogleProjectResource()
            .profileId(billingProfile.getId())
            .googleProjectId(googleProjectId)
            .googleProjectNumber(googleProjectNumber);
    region = GoogleRegion.DEFAULT_GOOGLE_REGION;
  }

  @Test
  public void enableServicesTest() throws InterruptedException {
    googleProjectService.enableServices(projectResource, region);
  }
}
