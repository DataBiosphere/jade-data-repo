package bio.terra.service.resourcemanagement.google;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.StorageResource;
import bio.terra.service.resourcemanagement.OneProjectPerResourceSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@Category(Unit.class)
@RunWith(MockitoJUnitRunner.class)
public class ResourceServiceUnitTest {

    @InjectMocks
    private ResourceService resourceService;

    @Mock
    private GoogleProjectService googleProjectService;

    @Mock
    private DatasetBucketDao datasetBucketDao;

    @Mock
    private OneProjectPerResourceSelector oneProjectPerResourceSelector;

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

    private final DatasetSummary datasetSummary = new DatasetSummary().storage(List.of(
        new StorageResource().region(GoogleRegion.DEFAULT_GOOGLE_REGION).cloudResource(
            GoogleCloudResource.BUCKET),
        new StorageResource().region(GoogleRegion.DEFAULT_GOOGLE_REGION).cloudResource(
            GoogleCloudResource.FIRESTORE)
        ));
    private final Dataset dataset = new Dataset(datasetSummary).id(datasetId);

    private final GoogleProjectResource projectResource = new GoogleProjectResource()
        .profileId(billingProfileId);

    private final UUID bucketName = UUID.randomUUID();
    private final UUID bucketId = UUID.randomUUID();
    private final GoogleBucketResource bucketResource = new GoogleBucketResource()
        .resourceId(bucketId)
        .name(bucketName.toString())
        .projectResource(projectResource);

    private final List<UUID> bucketsForDataset = List.of(bucketId);

    public void setup() throws InterruptedException {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testUseExistingBucketWhenNewNameProduced() throws Exception {
        when(googleProjectService.getOrCreateProject(any(), any(), any(), any()))
            .thenReturn(projectResource);
        when(datasetBucketDao.getBucketResourceIdForDatasetId(datasetId)).thenReturn(bucketsForDataset);
        when(bucketService.getBucketResourceById(bucketId, true)).thenReturn(bucketResource);
        when(oneProjectPerResourceSelector.bucketForFile(projectResource.getGoogleProjectId()))
            .thenReturn("newBucketName");
        when(bucketService
            .getOrCreateBucket(bucketName.toString(), projectResource, GoogleRegion.DEFAULT_GOOGLE_REGION,
                "flightId"))
            .thenReturn(bucketResource);

        GoogleBucketResource foundBucket = resourceService
            .getOrCreateBucketForFile(dataset, projectResource, "flightId");
        Assert.assertEquals(bucketResource, foundBucket);
    }

    @Test
    public void testUseNewBucketIfNoneExists() throws Exception {
        UUID newName = UUID.randomUUID();
        GoogleBucketResource newBucket = new GoogleBucketResource().name(newName.toString());

        when(googleProjectService.getOrCreateProject(any(), any(), any(), any()))
            .thenReturn(projectResource);
        when(datasetBucketDao.getBucketResourceIdForDatasetId(datasetId)).thenReturn(new ArrayList<>());
        when(oneProjectPerResourceSelector.bucketForFile(projectResource.getGoogleProjectId()))
            .thenReturn("newBucketName");
        when(bucketService
            .getOrCreateBucket(newName.toString(), projectResource, GoogleRegion.DEFAULT_GOOGLE_REGION,
                "flightId"))
            .thenReturn(newBucket);

        GoogleBucketResource foundBucket = resourceService
            .getOrCreateBucketForFile(dataset, projectResource, "flightId");
        Assert.assertEquals(newBucket, foundBucket);
    }
}
