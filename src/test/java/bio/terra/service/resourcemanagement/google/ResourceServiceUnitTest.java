package bio.terra.service.resourcemanagement.google;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.resourcemanagement.OneProjectPerProfileIdSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest
@ActiveProfiles({"google", "unittest", "terra"})
@Category(Unit.class)
public class ResourceServiceUnitTest {

    @Autowired
    @InjectMocks
    private ResourceService resourceService;

    @Mock
    private DatasetService datasetService;

    @Mock
    private GoogleProjectService googleProjectService;

    @Mock
    private DatasetBucketDao datasetBucketDao;

    @Mock
    private OneProjectPerProfileIdSelector oneProjectPerProfileIdSelector;

    @Mock
    private GoogleBucketService bucketService;

    @Mock
    private GoogleResourceDao resourceDao;

    @Mock
    private SamConfiguration samConfiguration;

    private final UUID billingProfileId = UUID.randomUUID();
    private final BillingProfileModel profileModel = new BillingProfileModel()
        .id(billingProfileId.toString());

    private final UUID datasetId = UUID.randomUUID();

    private final GoogleProjectResource projectResource = new GoogleProjectResource()
        .profileId(billingProfileId);

    private final UUID bucketId = UUID.randomUUID();
    private final GoogleBucketResource bucketResource = new GoogleBucketResource()
        .resourceId(bucketId)
        .name("bucketName")
        .projectResource(projectResource);

    private final List<UUID> bucketsForDataset = Collections.singletonList(bucketId);

    @Before
    public void setup() throws InterruptedException {
        MockitoAnnotations.initMocks(this);

        resourceService = new ResourceService(oneProjectPerProfileIdSelector, googleProjectService,
            bucketService, samConfiguration, datasetBucketDao, resourceDao);
    }

    @Test
    public void testUseExistingBucketWhenNewNameProduced() throws Exception {
        when(googleProjectService.getOrCreateProject(any(), any(), any()))
            .thenReturn(projectResource);
        when(datasetBucketDao.getBucketForDatasetId(datasetId)).thenReturn(bucketsForDataset);
        when(bucketService.getBucketResourceById(bucketId, true)).thenReturn(bucketResource);
        when(oneProjectPerProfileIdSelector.bucketForFile(datasetId.toString(), profileModel))
            .thenReturn("newBucketName");
        when(bucketService.getOrCreateBucket("bucketName", projectResource, "flightId"))
            .thenReturn(bucketResource);

        GoogleBucketResource foundBucket = resourceService
            .getOrCreateBucketForFile("datasetName", datasetId.toString(), profileModel,
                "flightId");
        Assert.assertEquals(bucketResource, foundBucket);
    }

    @Test
    public void testUseNewBucketIfNoneExists() throws Exception {
        GoogleBucketResource newBucket = new GoogleBucketResource().name("newBucketName");

        when(googleProjectService.getOrCreateProject(any(), any(), any()))
            .thenReturn(projectResource);
        when(datasetBucketDao.getBucketForDatasetId(datasetId)).thenReturn(new ArrayList<>());
        when(oneProjectPerProfileIdSelector.bucketForFile(datasetId.toString(), profileModel))
            .thenReturn("newBucketName");
        when(bucketService.getOrCreateBucket("newBucketName", projectResource, "flightId"))
            .thenReturn(newBucket);

        GoogleBucketResource foundBucket = resourceService
            .getOrCreateBucketForFile("datasetName", datasetId.toString(), profileModel,
                "flightId");
        Assert.assertEquals(newBucket, foundBucket);
    }
}
