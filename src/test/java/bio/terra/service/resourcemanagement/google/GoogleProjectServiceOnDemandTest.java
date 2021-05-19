package bio.terra.service.resourcemanagement.google;

import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.OnDemand;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@Category(OnDemand.class)
public class GoogleProjectServiceOnDemandTest {

    @Autowired
    private GoogleProjectService projectService;

    @Test
    public void testInitFirestore() throws InterruptedException {
        // Test the explicit activation of a Firestore DB in an empty project
        // Note, runner needs to populate in a project id and number before running
        projectService.enableServices(new GoogleProjectResource()
            .googleProjectId("")
            .googleProjectNumber(""),
            GoogleRegion.DEFAULT_GOOGLE_REGION);
    }
}
