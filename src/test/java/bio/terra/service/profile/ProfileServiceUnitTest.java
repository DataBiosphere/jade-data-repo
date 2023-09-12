package bio.terra.service.profile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.CloudPlatform;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.job.JobBuilder;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.azure.AzureAuthzService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.service.profile.flight.update.ProfileUpdateFlight;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class ProfileServiceUnitTest {

  @Mock private ProfileDao profileDao;
  @Mock private IamService iamService;
  @Mock private JobService jobService;
  @Mock private GoogleBillingService googleBillingService;
  @Mock private AzureAuthzService azureAuthzService;
  @Mock private ApplicationConfiguration applicationConfiguration;

  private ProfileService profileService;
  private AuthenticatedUserRequest user;
  private static final UUID PROFILE_ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

  @BeforeEach
  void setup() {
    profileService =
        new ProfileService(
            profileDao, iamService, jobService, googleBillingService, azureAuthzService);
    user =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DatasetUnit")
            .setEmail("dataset@unit.com")
            .setToken("token")
            .build();
  }

  @Test
  void testCreateProfile() {
    var jobBuilder = mock(JobBuilder.class);
    String jobId = "jobId";
    when(jobBuilder.submit()).thenReturn(jobId);

    var billingProfileRequestModel = new BillingProfileRequestModel();
    billingProfileRequestModel.setProfileName("name");

    when(jobService.newJob(
            anyString(), eq(ProfileCreateFlight.class), eq(billingProfileRequestModel), eq(user)))
        .thenReturn(jobBuilder);

    String result = profileService.createProfile(billingProfileRequestModel, user);
    verify(jobBuilder, times(1)).submit();
    assertEquals(result, jobId);
  }

  @Test
  void testUpdateProfile() {
    var billingProfileUpdateModel = new BillingProfileUpdateModel();
    UUID updateId = PROFILE_ID;
    billingProfileUpdateModel.setId(updateId);

    var jobBuilder = mock(JobBuilder.class);
    when(jobBuilder.addParameter(
            eq(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName()), eq(IamResourceType.SPEND_PROFILE)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(
            eq(JobMapKeys.IAM_RESOURCE_ID.getKeyName()), eq(updateId.toString())))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(
            eq(JobMapKeys.IAM_ACTION.getKeyName()), eq(IamAction.UPDATE_BILLING_ACCOUNT)))
        .thenReturn(jobBuilder);

    String jobId = "jobId";
    when(jobBuilder.submit()).thenReturn(jobId);

    when(jobService.newJob(
            anyString(), eq(ProfileUpdateFlight.class), eq(billingProfileUpdateModel), eq(user)))
        .thenReturn(jobBuilder);

    String result = profileService.updateProfile(billingProfileUpdateModel, user);

    verify(jobBuilder, times(1)).submit();
    assertEquals(result, jobId);
  }

  @Test
  void testDeleteProfile() {
    var jobBuilder = mock(JobBuilder.class);

    String jobId = "id";
    when(jobBuilder.submit()).thenReturn(jobId);
    UUID deleteId = PROFILE_ID;
    when(jobBuilder.addParameter(eq(ProfileMapKeys.PROFILE_ID), eq(deleteId)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(
            eq(JobMapKeys.CLOUD_PLATFORM.getKeyName()), eq(CloudPlatform.GCP.name())))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(
            eq(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName()), eq(IamResourceType.SPEND_PROFILE)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(eq(JobMapKeys.IAM_RESOURCE_ID.getKeyName()), eq(deleteId)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(eq(JobMapKeys.IAM_ACTION.getKeyName()), eq(IamAction.DELETE)))
        .thenReturn(jobBuilder);
    when(jobBuilder.addParameter(eq(JobMapKeys.DELETE_CLOUD_RESOURCES.getKeyName()), eq(false)))
        .thenReturn(jobBuilder);

    var billingProfileModel = new BillingProfileModel();
    billingProfileModel.setCloudPlatform(CloudPlatform.GCP);
    when(profileDao.getBillingProfileById(deleteId)).thenReturn(billingProfileModel);

    when(jobService.newJob(anyString(), eq(ProfileDeleteFlight.class), eq(null), eq(user)))
        .thenReturn(jobBuilder);

    String result = profileService.deleteProfile(deleteId, false, user);
    verify(jobBuilder, times(1)).submit();
    assertEquals(result, jobId);
  }

  @Test
  void testDeleteProfileNoAccess() {
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(
            eq(user), eq(IamResourceType.SPEND_PROFILE), any(), eq(IamAction.DELETE));
    assertThrows(
        IamForbiddenException.class,
        () ->
            profileService.deleteProfile(
                UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), false, user));
  }

  @Test
  void testVerifyAccountHasAccess() {
    String id = "id";

    when(googleBillingService.canAccess(any(), eq(id))).thenReturn(true);

    profileService.verifyAccount(id, user);
  }

  @Test
  void testVerifyAccountNoAccess() {
    String id = "id";

    when(googleBillingService.canAccess(any(), eq(id))).thenReturn(false);

    assertThrows(
        InaccessibleBillingAccountException.class, () -> profileService.verifyAccount(id, user));
  }
}
