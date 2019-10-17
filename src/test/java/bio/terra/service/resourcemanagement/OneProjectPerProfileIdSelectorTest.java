package bio.terra.service.resourcemanagement;

import bio.terra.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.iam.SamClientService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ActiveProfiles({"terra", "google"})
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Connected.class)
@Ignore
public class OneProjectPerProfileIdSelectorTest {
    @Autowired
    private GoogleResourceConfiguration resourceConfiguration;

    @Autowired
    private ProfileService profileService;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private SnapshotService snapshotService;

    @Autowired
    private OneProjectPerProfileIdSelector oneProjectPerProfileIdSelector;

    @Autowired
    private ConnectedOperations connectedOperations;

    @MockBean
    private SamClientService samClientService;

    @Before
    public void setup() throws ApiException {
        connectedOperations.stubOutSamCalls(samClientService);
    }

    @Test
    public void shouldGetCorrectIdForDataset() throws Exception {
        String coreBillingAccountId = resourceConfiguration.getCoreBillingAccount();
        String profileName = ProfileFixtures.randomHex(16);
        BillingProfileRequestModel billingProfileRequestModel = ProfileFixtures.randomBillingProfileRequest()
            .billingAccountId(coreBillingAccountId)
            .profileName(profileName);
        BillingProfileModel profile = profileService.createProfile(billingProfileRequestModel);

        DatasetSummaryModel datasetSummaryModel =
            connectedOperations.createDatasetWithFlight(profile, "dataset-minimal.json");
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetSummaryModel.getId()));

        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(dataset);
        String expectedProfileId = resourceConfiguration.getProjectId() + "-" + profileName;
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForDatasetWithSpecialChars() throws Exception {
        String coreBillingAccountId = resourceConfiguration.getCoreBillingAccount();
        String namePrefix = "chars  ";
        String hexDigits = ProfileFixtures.randomHex(8);
        String profileName = namePrefix + hexDigits;
        BillingProfileRequestModel billingProfileRequestModel = ProfileFixtures.randomBillingProfileRequest()
            .billingAccountId(coreBillingAccountId)
            .profileName(profileName);
        BillingProfileModel profile = profileService.createProfile(billingProfileRequestModel);

        DatasetSummaryModel datasetSummaryModel =
            connectedOperations.createDatasetWithFlight(profile, "dataset-minimal.json");
        Dataset dataset = datasetService.retrieve(UUID.fromString(datasetSummaryModel.getId()));

        String projectId = oneProjectPerProfileIdSelector.projectIdForDataset(dataset);
        String expectedProfileId = resourceConfiguration.getProjectId() + "-" + "chars--" + hexDigits;
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }

    @Test
    public void shouldGetCorrectIdForSnapshot() throws Exception {
        String coreBillingAccountId = resourceConfiguration.getCoreBillingAccount();
        String profileName = ProfileFixtures.randomHex(16);
        BillingProfileRequestModel billingProfileRequestModel = ProfileFixtures.randomBillingProfileRequest()
            .billingAccountId(coreBillingAccountId)
            .profileName(profileName);
        BillingProfileModel profile = profileService.createProfile(billingProfileRequestModel);

        DatasetSummaryModel datasetSummaryModel =
            connectedOperations.createDatasetWithFlight(profile, "snapshot-test-dataset.json");

        MockHttpServletResponse response =
            connectedOperations.launchCreateSnapshot(datasetSummaryModel, "snapshot-test-snapshot.json", "");
        SnapshotSummaryModel snapshotSummaryModel = connectedOperations.handleCreateSnapshotSuccessCase(response);
        SnapshotModel snapshotModel = connectedOperations.getSnapshot(snapshotSummaryModel.getId());
        Snapshot snapshot = snapshotService.retrieveSnapshot(UUID.fromString(snapshotModel.getId()));

        // TODO: we can test this once configuring firestore is programatic

        String projectId = oneProjectPerProfileIdSelector.projectIdForSnapshot(snapshot);
        String expectedProfileId = resourceConfiguration.getProjectId() + "-" + profileName;
        assertThat("Project ID is what we expect", projectId, equalTo(expectedProfileId));
    }
}
