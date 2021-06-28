package bio.terra.service.profile;

import bio.terra.common.category.Unit;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamService;
import bio.terra.service.job.JobBuilder;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.azure.AzureAuthzService;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Category(Unit.class)
public class ProfileServiceUnitTest {

    @Mock
    private ProfileDao profileDao;
    @Mock
    private IamService iamService;
    @Mock
    private JobService jobService;
    @Mock
    private GoogleBillingService googleBillingService;
    @Mock
    private AzureAuthzService azureAuthzService;

    private ProfileService profileService;


    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);

        profileService = new ProfileService(
                profileDao, iamService, jobService,
                googleBillingService, azureAuthzService
        );
    }

    @Test
    public void testCreateProfile() {
        var billingProfileRequestModel = new BillingProfileRequestModel();
        billingProfileRequestModel.setProfileName("name");

        var user = new AuthenticatedUserRequest();

        var jobBuilder = mock(JobBuilder.class);

        when(jobBuilder.submit()).thenReturn("id");

        when(jobService.newJob(eq("Create billing profile 'name'"),
                eq(ProfileCreateFlight.class), any(), any())).thenReturn(jobBuilder);

        profileService.createProfile(billingProfileRequestModel, user);
        verify(jobBuilder, times(1)).submit();
    }

    @Test
    public void testVerifyAccountHasAccess() {
        var user = new AuthenticatedUserRequest();
        String id = "id";

        when(googleBillingService.canAccess(any(), eq("id"))).thenReturn(true);

        try {
            profileService.verifyAccount(id, user);
        } catch (Exception e) {
            fail("No exception should've been thrown");
        }
    }

    @Test(expected = InaccessibleBillingAccountException.class)
    public void testVerifyAccountNoAccess() {
        var user = new AuthenticatedUserRequest();
        String id = "id";

        when(googleBillingService.canAccess(any(), eq("id"))).thenReturn(false);

        profileService.verifyAccount(id, user);
    }

}
