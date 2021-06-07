package bio.terra.service.resourcemanagement;

import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.category.Connected;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class RBSConnectedTest {

    @Autowired private BufferService bufferService;

    @Test
    public void testProjectHandout() {
        String handoutRequestId = UUID.randomUUID().toString();
        HandoutRequestBody request = new HandoutRequestBody().handoutRequestId(handoutRequestId);
        ResourceInfo resource = bufferService.handoutResource(request);
        assertEquals(resource.getRequestHandoutId(), handoutRequestId);
    }
}
