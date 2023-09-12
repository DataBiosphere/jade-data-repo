package bio.terra.service.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.PolicyResponse;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class ProfileAPIControllerTest {

  @Mock private ObjectMapper objectMapper;
  @Mock private HttpServletRequest request;
  @Mock private ProfileService profileService;
  @Mock private ProfileRequestValidator billingProfileRequestValidator;
  @Mock private ProfileUpdateRequestValidator profileUpdateRequestValidator;
  @Mock private PolicyMemberValidator policyMemberValidator;
  @Mock private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @Mock private JobService jobService;

  @Mock private IamService iamService;
  @Mock private ApplicationConfiguration applicationConfiguration;

  private ProfileApiController apiController;
  private AuthenticatedUserRequest user;

  @Before
  public void setup() throws Exception {
    apiController =
        new ProfileApiController(
            objectMapper,
            request,
            profileService,
            billingProfileRequestValidator,
            profileUpdateRequestValidator,
            policyMemberValidator,
            jobService,
            authenticatedUserRequestFactory,
            iamService,
            applicationConfiguration);
    user =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DatasetUnit")
            .setEmail("dataset@unit.com")
            .setToken("token")
            .build();
  }

  @Test
  public void testCreateProfile() {
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    var billingProfileRequestModel = new BillingProfileRequestModel();
    String jobId = "jobId";
    when(profileService.createProfile(eq(billingProfileRequestModel), eq(user)))
        .thenReturn("jobId");

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(eq(jobId), eq(user))).thenReturn(jobModel);

    ResponseEntity entity = apiController.createProfile(billingProfileRequestModel);
    verify(profileService, times(1)).createProfile(eq(billingProfileRequestModel), eq(user));
    assertNotNull(entity);
  }

  @Test
  public void testUpdateProfile() {
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(UUID.randomUUID());
    String jobId = "jobId";
    when(profileService.updateProfile(eq(billingProfileUpdateModel), eq(user))).thenReturn(jobId);

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(eq(jobId), eq(user))).thenReturn(jobModel);

    ResponseEntity entity = apiController.updateProfile(billingProfileUpdateModel);
    verify(profileService, times(1)).updateProfile(eq(billingProfileUpdateModel), eq(user));
    assertNotNull(entity);
  }

  @Test
  public void testUpdateProfileNotFound() {
    UUID profileId = UUID.randomUUID();
    doThrow(ProfileNotFoundException.class).when(profileService).getProfileByIdNoCheck(profileId);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(profileId);
    assertThrows(
        ProfileNotFoundException.class,
        () -> apiController.updateProfile(billingProfileUpdateModel));
    verifyNoInteractions(iamService);
    verify(profileService, times(0)).updateProfile(eq(billingProfileUpdateModel), eq(user));
  }

  @Test
  public void testUpdateProfileForbidden() {
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.UPDATE_BILLING_ACCOUNT);
    var billingProfileUpdateModel = new BillingProfileUpdateModel().id(profileId);
    assertThrows(
        IamForbiddenException.class, () -> apiController.updateProfile(billingProfileUpdateModel));
    verify(profileService, times(0)).updateProfile(eq(billingProfileUpdateModel), eq(user));
  }

  @Test
  public void testDeleteProfile() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(user);
    UUID deleteId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String jobId = "jobId";
    when(profileService.deleteProfile(eq(deleteId), eq(false), eq(user))).thenReturn(jobId);

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(eq(jobId), eq(user))).thenReturn(jobModel);

    ResponseEntity entity = apiController.deleteProfile(deleteId, false);
    verify(profileService, times(1)).deleteProfile(eq(deleteId), eq(false), eq(user));
    assertNotNull(entity);
  }

  @Test
  public void testDeleteProfileNotFound() {
    UUID profileId = UUID.randomUUID();
    doThrow(ProfileNotFoundException.class).when(profileService).getProfileByIdNoCheck(profileId);
    assertThrows(
        ProfileNotFoundException.class, () -> apiController.deleteProfile(profileId, false));
    verifyNoInteractions(iamService);
    verify(profileService, times(0)).deleteProfile(eq(profileId), eq(false), eq(user));
  }

  @Test
  public void testDeleteProfileForbidden() {
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    UUID profileId = UUID.randomUUID();
    when(profileService.getProfileByIdNoCheck(profileId))
        .thenReturn(new BillingProfileModel().id(profileId));
    mockProfileForbidden(profileId, IamAction.DELETE);
    assertThrows(IamForbiddenException.class, () -> apiController.deleteProfile(profileId, false));
    verify(profileService, times(0)).deleteProfile(eq(profileId), eq(false), eq(user));
  }

  @Test
  public void testAddProfilePolicyMember() {
    when(authenticatedUserRequestFactory.from(any())).thenReturn(user);

    UUID id = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String policyName = "policyName";
    var policyMemberRequest = new PolicyMemberRequest();
    var policyModel = new PolicyModel();
    when(profileService.addProfilePolicyMember(
            eq(id), eq(policyName), eq(policyMemberRequest), eq(user)))
        .thenReturn(policyModel);

    ResponseEntity<PolicyResponse> response =
        apiController.addProfilePolicyMember(id, policyName, policyMemberRequest);

    assertTrue(response.getBody().getPolicies().contains(policyModel));
    assertEquals(HttpStatus.OK, response.getStatusCode());
  }

  private void mockProfileForbidden(UUID profileId, IamAction action) {
    doThrow(IamForbiddenException.class)
        .when(iamService)
        .verifyAuthorization(user, IamResourceType.SPEND_PROFILE, profileId.toString(), action);
  }
}
