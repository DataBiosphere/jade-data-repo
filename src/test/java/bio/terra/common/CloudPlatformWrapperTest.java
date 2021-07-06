package bio.terra.common;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.CloudResource;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.StorageResource;



import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import java.util.stream.Collectors;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;


@Category(Unit.class)
public class CloudPlatformWrapperTest {

    /**
     * Canary test to make sure we're only returning the CloudResources that are actually used for Azure datasets.
     * This will change as more and more of Azure is implemented and is only here to make sure our database is
     * somewhat clean during Azure implementation
     */
    @Test
    public void createGoogleResourcesForAzure() {

        var dummyDatasetRequestModel = new DatasetRequestModel()
            .region(AzureRegion.DEFAULT_AZURE_REGION.name());

        // Need to convert to Strings for Hamcrest Matchers to accept types.
        List<CloudResource> resourcesForAzure = CloudPlatformWrapper.of(CloudPlatform.AZURE)
            .createStorageResourceValues(dummyDatasetRequestModel)
            .stream().map(StorageResource::getCloudResource)
            .collect(Collectors.toList());

        assertThat("creating storage resources for Azure results in resources that are actually used",
            resourcesForAzure,
            contains(AzureCloudResource.APPLICATION_DEPLOYMENT,
                AzureCloudResource.STORAGE_ACCOUNT,
                GoogleCloudResource.BIGQUERY,
                GoogleCloudResource.FIRESTORE));

    }
}
