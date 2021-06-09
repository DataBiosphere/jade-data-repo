package bio.terra.service.resourcemanagement;

import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import com.google.api.services.cloudresourcemanager.model.Project;
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

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class RBSConnectedTest {

    @Autowired private BufferService bufferService;
    @Autowired private GoogleProjectService projectService;

    @Test
    public void testProjectHandout() {
        String handoutRequestId = UUID.randomUUID().toString();
        HandoutRequestBody request = new HandoutRequestBody().handoutRequestId(handoutRequestId);
        ResourceInfo resource = bufferService.handoutResource(request);
        String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
        Project project = projectService.getProject(projectId);
        assertThat("The project requested from RBS is active",
                project.getLifecycleState(),
                equalTo("ACTIVE"));
    }
}
