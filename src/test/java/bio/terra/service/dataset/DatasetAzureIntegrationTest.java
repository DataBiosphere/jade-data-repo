package bio.terra.service.dataset;

import bio.terra.app.model.AzureCloudResource;
import bio.terra.app.model.AzureRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.EnumerateDatasetModel;
import bio.terra.model.StorageResourceModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class DatasetAzureIntegrationTest extends UsersBase {
    private static final String omopDatasetName = "it_dataset_omop";
    private static final String omopDatasetDesc =
        "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki";
    private static final String omopDatasetRegionName = AzureRegion.DEFAULT_AZURE_REGION.toString();
    private static final String omopDatasetGcpRegionName = GoogleRegion.DEFAULT_GOOGLE_REGION.toString();
    private static Logger logger = LoggerFactory.getLogger(DatasetAzureIntegrationTest.class);

    @Autowired private DataRepoFixtures dataRepoFixtures;
    @Autowired private AuthService authService;

    private String stewardToken;
    private String datasetId;
    private String profileId;

    @Before
    public void setup() throws Exception {
        super.setup(false);
        stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
        dataRepoFixtures.resetConfig(steward());
        profileId = dataRepoFixtures.createAzureBillingProfile(steward()).getId();
        datasetId = null;
    }

    @After
    public void teardown() throws Exception {
        dataRepoFixtures.resetConfig(steward());
        if (datasetId != null) {
            dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
            datasetId = null;
        }

        if (profileId != null) {
            dataRepoFixtures.deleteProfileLog(steward(), profileId);
            profileId = null;
        }
    }

    @Test
    public void datasetsHappyPath() throws Exception {
        DatasetSummaryModel summaryModel =
            dataRepoFixtures.createDataset(steward(), profileId, "it-dataset-omop.json", CloudPlatform.AZURE);
        datasetId = summaryModel.getId();

        logger.info("dataset id is " + summaryModel.getId());
        assertThat(summaryModel.getName(), startsWith(omopDatasetName));
        assertThat(summaryModel.getDescription(), equalTo(omopDatasetDesc));

        DatasetModel datasetModel = dataRepoFixtures.getDataset(steward(), summaryModel.getId());

        assertThat(datasetModel.getName(), startsWith(omopDatasetName));
        assertThat(datasetModel.getDescription(), equalTo(omopDatasetDesc));

        // There is a delay from when a resource is created in SAM to when it is available in an enumerate call.
        boolean metExpectation = TestUtils.eventualExpect(5, 60, true, () -> {
            EnumerateDatasetModel enumerateDatasetModel = dataRepoFixtures.enumerateDatasets(steward());
            boolean found = false;
            for (DatasetSummaryModel oneDataset : enumerateDatasetModel.getItems()) {
                if (oneDataset.getId().equals(datasetModel.getId())) {
                    assertThat(oneDataset.getName(), startsWith(omopDatasetName));
                    assertThat(oneDataset.getDescription(), equalTo(omopDatasetDesc));
                    Map<String, StorageResourceModel> storageMap = datasetModel.getStorage().stream()
                        .collect(Collectors.toMap(StorageResourceModel::getCloudResource, Function.identity()));

                    AzureRegion omopDatasetRegion = AzureRegion.fromValue(omopDatasetRegionName);
                    GoogleRegion omopDatasetGoogleRegion = GoogleRegion.fromValue(omopDatasetGcpRegionName);
                    assertThat(omopDatasetRegion, notNullValue());
                    assertThat(omopDatasetGoogleRegion, notNullValue());

                    assertThat("Bucket storage matches",
                        storageMap.entrySet().stream().filter(e ->
                            e.getKey().equals(GoogleCloudResource.BUCKET.getValue()))
                            .findAny()
                            .map(e -> e.getValue().getRegion())
                            .orElseThrow(() -> new RuntimeException("Key not found")),
                        equalTo(omopDatasetGoogleRegion.getRegionOrFallbackBucketRegion().getValue()));

                    assertThat("Firestore storage matches",
                        storageMap.entrySet().stream().filter(e ->
                            e.getKey().equals(GoogleCloudResource.FIRESTORE.getValue()))
                            .findAny()
                            .map(e -> e.getValue().getRegion())
                            .orElseThrow(() -> new RuntimeException("Key not found")),
                        equalTo(omopDatasetGoogleRegion.getRegionOrFallbackFirestoreRegion().getValue()));

                    assertThat("Storage account storage matches",
                        storageMap.entrySet().stream().filter(e ->
                            e.getKey().equals(AzureCloudResource.STORAGE_ACCOUNT.getValue()))
                            .findAny()
                            .map(e -> e.getValue().getRegion())
                            .orElseThrow(() -> new RuntimeException("Key not found")),
                        equalTo(omopDatasetRegion.getValue()));

                    found = true;
                    break;
                }
            }
            return found;
        });

        assertTrue("dataset was found in enumeration", metExpectation);

        // This should fail since it currently has dataset storage account within
        assertThrows(AssertionError.class, () -> dataRepoFixtures.deleteProfile(steward(), profileId));

        // Create and delete a dataset and make sure that the profile still can't be deleted
        DatasetSummaryModel summaryModel2 =
            dataRepoFixtures.createDataset(steward(), profileId, "it-dataset-omop.json");
        dataRepoFixtures.deleteDataset(steward(), summaryModel2.getId());
        assertThat(
            "Original dataset is still there",
            dataRepoFixtures.getDatasetRaw(steward(), summaryModel.getId()).getStatusCode().is2xxSuccessful(),
            equalTo(true));
        assertThat(
            "New dataset was deleted",
            dataRepoFixtures.getDatasetRaw(steward(), summaryModel2.getId()).getStatusCode().value(),
            // TODO: fix bug where this shows up as a 401 and not a 404 since it's not longer in Sam
            equalTo(401));
        assertThrows(AssertionError.class, () -> dataRepoFixtures.deleteProfile(steward(), profileId));

        // Make sure that any failure in tearing down is presented as a test failure
        clearEnvironment();
    }

    private void clearEnvironment() throws Exception {
        if (datasetId != null) {
            dataRepoFixtures.deleteDataset(steward(), datasetId);
            datasetId = null;
        }

        if (profileId != null) {
            dataRepoFixtures.deleteProfile(steward(), profileId);
            profileId = null;
        }
    }

}
