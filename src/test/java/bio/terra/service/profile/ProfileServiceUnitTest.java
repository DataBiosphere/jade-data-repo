package bio.terra.service.profile;

import bio.terra.common.category.Unit;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.iam.exception.IamForbiddenException;
import bio.terra.service.job.JobBuilder;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.azure.AzureAuthzService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.service.profile.flight.update.ProfileUpdateFlight;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
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

        when(jobService.newJob(any(), eq(ProfileCreateFlight.class), any(), any())).thenReturn(jobBuilder);

        String result = profileService.createProfile(billingProfileRequestModel, user);
        verify(jobBuilder, times(1)).submit();
        assertEquals(result, "id");
    }

    @Test
    public void testUpdateProfile() {
        var billingProfileUpdateModel = new BillingProfileUpdateModel();
        billingProfileUpdateModel.setId(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));

        var user = new AuthenticatedUserRequest();

        var jobBuilder = mock(JobBuilder.class);

        when(jobBuilder.submit()).thenReturn("id");

        when(jobService.newJob(any(), eq(ProfileUpdateFlight.class), any(), any())).thenReturn(jobBuilder);

        String result = profileService.updateProfile(billingProfileUpdateModel, user);
        verify(iamService, times(1))
                .verifyAuthorization(
                        eq(user),
                        eq(IamResourceType.SPEND_PROFILE),
                        any(),
                        eq(IamAction.UPDATE_BILLING_ACCOUNT)
                );
        verify(jobBuilder, times(1)).submit();
        assertEquals(result, "id");
    }

    @Test(expected = IamForbiddenException.class)
    public void testUpdateProfileNoAccess() {
        var user = new AuthenticatedUserRequest();
        doThrow(IamForbiddenException.class).when(iamService).verifyAuthorization(
                eq(user),
                eq(IamResourceType.SPEND_PROFILE),
                any(),
                eq(IamAction.UPDATE_BILLING_ACCOUNT)
        );
        var billingProfileUpdateModel = new BillingProfileUpdateModel();
        profileService.updateProfile(billingProfileUpdateModel, user);
    }

    @Test
    public void testDeleteProfile() {
        var jobBuilder = mock(JobBuilder.class);

        when(jobBuilder.submit()).thenReturn("id");
        when(jobBuilder.addParameter(eq(ProfileMapKeys.PROFILE_ID), eq("name"))).thenReturn(jobBuilder);

        when(jobService.newJob(any(), eq(ProfileDeleteFlight.class), any(), any())).thenReturn(jobBuilder);

        var user = new AuthenticatedUserRequest();
        String result = profileService.deleteProfile(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), user);
        verify(iamService, times(1))
                .verifyAuthorization(
                        eq(user),
                        eq(IamResourceType.SPEND_PROFILE),
                        any(),
                        eq(IamAction.DELETE)
                );
        verify(jobBuilder, times(1)).submit();
        assertEquals(result, "id");
    }

    @Test(expected = IamForbiddenException.class)
    public void testDeleteProfileNoAccess() {
        var user = new AuthenticatedUserRequest();
        doThrow(IamForbiddenException.class).when(iamService).verifyAuthorization(
                eq(user),
                eq(IamResourceType.SPEND_PROFILE),
                any(),
                eq(IamAction.DELETE)
        );
        profileService.deleteProfile(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), user);
    }

    @Test
    public void testVerifyAccountHasAccess() {
        var user = new AuthenticatedUserRequest();
        String id = "id";

        when(googleBillingService.canAccess(any(), eq(id))).thenReturn(true);

        profileService.verifyAccount(id, user);
    }

    @Test(expected = InaccessibleBillingAccountException.class)
    public void testVerifyAccountNoAccess() {
        var user = new AuthenticatedUserRequest();
        String id = "id";

        when(googleBillingService.canAccess(any(), eq(id))).thenReturn(false);

        profileService.verifyAccount(id, user);
    }
}
