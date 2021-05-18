package bio.terra.service.resourcemanagement;


import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.CloudPlatform;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.profile.ProfileDao;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.equalTo;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class ProfileConnectedTest {

    @Autowired private ProfileDao profileDao;
    @Autowired private ConnectedOperations connectedOperations;
    @Autowired private ConnectedTestConfiguration testConfig;
    @Autowired private ConfigurationService configService;

    @MockBean
    private IamProviderInterface samService;

    @Before
    public void setup() throws Exception {
        connectedOperations.stubOutSamCalls(samService);
        configService.reset();
    }

    @After
    public void tearDown() throws Exception {
        connectedOperations.teardown();
        configService.reset();
    }

    @Test
    public void testGoogleIsDefault() throws Exception {
        var requestModel = ProfileFixtures.randomBillingProfileRequest()
            .billingAccountId(testConfig.getGoogleBillingAccountId())
            .cloudPlatform(null);
        var profile = connectedOperations.createProfile(requestModel);
        var retrievedProfile = connectedOperations.getProfileById(profile.getId());
        assertThat("GCP is the default cloud provider",
            retrievedProfile.getCloudPlatform(), equalTo(CloudPlatform.GCP));
    }

    @Test
    public void testGoogleInvalidAzureParams() throws Exception {
        var gcpRequestModel = ProfileFixtures.randomBillingProfileRequest()
            .cloudPlatform(CloudPlatform.GCP)
            .tenant(UUID.randomUUID().toString())
            .subscription(UUID.randomUUID().toString())
            .resourceGroup(UUID.randomUUID().toString());

        var defaultRequestModel = ProfileFixtures.randomBillingProfileRequest()
            .tenant(UUID.randomUUID().toString())
            .subscription(UUID.randomUUID().toString())
            .resourceGroup(UUID.randomUUID().toString());

        for (var requestModel : List.of(gcpRequestModel, defaultRequestModel)) {
            var errorModel = connectedOperations.createProfileExpectError(requestModel, HttpStatus.BAD_REQUEST);

            assertThat("There are 3 errors returned", errorModel.getErrorDetail().size(), equalTo(3));

            assertThat("GCP request returns tenant error if supplied",
                errorModel.getErrorDetail(),
                hasItems(startsWith("tenant")));
            assertThat("GCP request returns subscription error if supplied",
                errorModel.getErrorDetail(),
                hasItems(startsWith("subscription")));
            assertThat("GCP request returns resourceGroup error if supplied",
                errorModel.getErrorDetail(),
                hasItems(startsWith("resourceGroup")));
        }
    }

    @Test
    public void testAzureInvalidMissingParams() throws Exception {
        var azureRequestModel = ProfileFixtures.randomBillingProfileRequest()
            .cloudPlatform(CloudPlatform.AZURE);
        var missingParams = connectedOperations
            .createProfileExpectError(azureRequestModel, HttpStatus.BAD_REQUEST);

        assertThat("There are 3 errors returned", missingParams.getErrorDetail().size(), equalTo(3));

        assertThat("Azure request returns tenant error if not supplied",
            missingParams.getErrorDetail(),
            hasItems(containsString("UUID `tenant`")));
        assertThat("Azure request returns subscription error if not supplied",
            missingParams.getErrorDetail(),
            hasItems(containsString("UUID `subscription`")));
        assertThat("Azure request returns resourceGroup error if not supplied",
            missingParams.getErrorDetail(),
            hasItems(containsString("UUID `resourceGroup`")));

        var invalidUuidRequest = ProfileFixtures.randomBillingProfileRequest()
            .cloudPlatform(CloudPlatform.AZURE)
            .tenant("not-a-valid-uuid")
            .subscription(UUID.randomUUID().toString())
            .resourceGroup(UUID.randomUUID().toString());

        var invalidUuid = connectedOperations
            .createProfileExpectError(invalidUuidRequest, HttpStatus.BAD_REQUEST);

        assertThat("There are 3 errors returned", invalidUuid.getErrorDetail().size(), equalTo(3));
        assertThat("The server rejects invalid UUID for tenant",
            invalidUuid.getErrorDetail(),
            hasItems(containsString("UUID `tenant`")));
        assertThat("The server rejects invalid UUID for subscription",
            invalidUuid.getErrorDetail(),
            hasItems(containsString("UUID `subscription`")));
        assertThat("The server rejects invalid UUID for resourceGroup",
            invalidUuid.getErrorDetail(),
            hasItems(containsString("UUID `resourceGroup`")));
    }
}
