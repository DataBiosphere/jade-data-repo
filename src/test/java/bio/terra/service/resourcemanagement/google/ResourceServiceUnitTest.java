package bio.terra.service.resourcemanagement.google;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.resourcemanagement.ResourceService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class ResourceServiceUnitTest {

    @Autowired
    @InjectMocks
    private ResourceService resourceService;

    @Mock
    private DatasetService datasetService;

    @Mock
    private GoogleProjectService googleProjectService;

    @Before
    public void setup() throws InterruptedException {
        when(googleProjectService.getOrCreateProject(any(), any(), any())).thenReturn(new GoogleProjectResource());
    }

    @Test
    public void testCreateBucketWhenNoneExists() throws Exception {
        BillingProfileModel profileModel = new BillingProfileModel();
        // create bucket
        resourceService.getOrCreateBucketForFile("datasetName", "datasetId", profileModel, "flightId");
    }
}
