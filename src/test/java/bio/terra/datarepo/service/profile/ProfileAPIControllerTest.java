package bio.terra.datarepo.service.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.model.BillingProfileUpdateModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.JobModel.JobStatusEnum;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyModel;
import bio.terra.datarepo.model.PolicyResponse;
import bio.terra.datarepo.service.iam.AuthenticatedUserRequest;
import bio.terra.datarepo.service.iam.AuthenticatedUserRequestFactory;
import bio.terra.datarepo.service.iam.PolicyMemberValidator;
import bio.terra.datarepo.service.job.JobService;
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

@RunWith(MockitoJUnitRunner.StrictStubs.class)
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

  private ProfileApiController apiController;

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
            authenticatedUserRequestFactory);
  }

  @Test
  public void testCreateProfile() {
    var user = new AuthenticatedUserRequest();
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
    var user = new AuthenticatedUserRequest();
    when(authenticatedUserRequestFactory.from(eq(request))).thenReturn(user);
    var billingProfileUpdateModel = new BillingProfileUpdateModel();
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
  public void testDeleteProfile() {
    var user = new AuthenticatedUserRequest();
    when(authenticatedUserRequestFactory.from(any())).thenReturn(user);
    UUID deleteId = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    String jobId = "jobId";
    when(profileService.deleteProfile(eq(deleteId), eq(user))).thenReturn(jobId);

    var jobModel = new JobModel();
    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    when(jobService.retrieveJob(eq(jobId), eq(user))).thenReturn(jobModel);

    ResponseEntity entity = apiController.deleteProfile(deleteId);
    verify(profileService, times(1)).deleteProfile(eq(deleteId), eq(user));
    assertNotNull(entity);
  }

  @Test
  public void testAddProfilePolicyMember() {
    var user = new AuthenticatedUserRequest();
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
}
