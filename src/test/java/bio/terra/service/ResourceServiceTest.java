package bio.terra.service;


import bio.terra.category.Connected;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectRequest;
import bio.terra.resourcemanagement.metadata.google.GoogleProjectResource;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import bio.terra.resourcemanagement.service.google.GoogleResourceService;
import bio.terra.service.dataproject.DataLocationService;
import com.google.api.services.cloudresourcemanager.model.Project;
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

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class ResourceServiceTest {

    @Autowired private GoogleResourceConfiguration resourceConfiguration;
    @Autowired private GoogleResourceService resourceService;
    @Autowired private ConnectedOperations connectedOperations;

    private BillingProfileModel profile;

    @Before
    public void setup() throws Exception {
        profile = connectedOperations.createProfileForAccount(resourceConfiguration.getCoreBillingAccount());
    }

    @After
    public void teardown() throws Exception {
        connectedOperations.teardown();
    }

    @Test
    public void createAndDeleteProjectTest() {
        // the project id can't be more than 30 characters
        String projectId = ("test-" + UUID.randomUUID().toString()).substring(0, 30);
        GoogleProjectRequest projectRequest = new GoogleProjectRequest()
            .projectId(projectId)
            .profileId(UUID.fromString(profile.getId()))
            .serviceIds(DataLocationService.DATA_PROJECT_SERVICE_IDS);
        GoogleProjectResource projectResource = resourceService.getOrCreateProject(projectRequest);
        Project project = resourceService.getProject(projectId);
        assertThat("the project is active",
            project.getLifecycleState(),
            equalTo("ACTIVE"));

        resourceService.deleteProjectResource(projectResource.getRepositoryId());
        project = resourceService.getProject(projectId);
        assertThat("the project is not active after delete",
            project.getLifecycleState(),
            not(equalTo("ACTIVE")));
    }
}
