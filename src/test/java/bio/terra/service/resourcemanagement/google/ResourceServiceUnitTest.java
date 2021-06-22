package bio.terra.service.resourcemanagement.google;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.GoogleStorageResource;
import bio.terra.service.resourcemanagement.OneProjectPerResourceSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Category(Unit.class)
@RunWith(MockitoJUnitRunner.class)
public class ResourceServiceUnitTest {

    @InjectMocks
    private ResourceService resourceService;

    @Mock
    private GoogleBucketService bucketService;

    @Mock
    private OneProjectPerResourceSelector oneProjectPerResourceSelector;

    private final UUID billingProfileId = UUID.randomUUID();

    private final UUID datasetId = UUID.randomUUID();

    private final DatasetSummary datasetSummary = new DatasetSummary().storage(List.of(
        new GoogleStorageResource(datasetId, GoogleCloudResource.BUCKET, GoogleRegion.DEFAULT_GOOGLE_REGION),
        new GoogleStorageResource(datasetId, GoogleCloudResource.FIRESTORE, GoogleRegion.DEFAULT_GOOGLE_REGION)));
    private final Dataset dataset = new Dataset(datasetSummary).id(datasetId);

    private final GoogleProjectResource projectResource = new GoogleProjectResource()
        .profileId(billingProfileId)
        .googleProjectId("randProjectId");

    private final UUID bucketName = UUID.randomUUID();
    private final UUID bucketId = UUID.randomUUID();
    private final GoogleBucketResource bucketResource = new GoogleBucketResource()
        .resourceId(bucketId)
        .name(bucketName.toString())
        .projectResource(projectResource);

    public void setup() throws InterruptedException {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGrabBucket() throws Exception {
        when(bucketService
            .getOrCreateBucket(any(), any(), any(), any()))
            .thenReturn(bucketResource);

        GoogleBucketResource foundBucket = resourceService
            .getOrCreateBucketForFile(dataset, projectResource, "flightId");
        Assert.assertEquals(bucketResource, foundBucket);
    }
}
