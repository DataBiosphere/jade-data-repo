package bio.terra.service.profile;

import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.AuthenticatedUserRequestFactory;
import bio.terra.service.iam.PolicyMemberValidator;
import bio.terra.service.job.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

import javax.servlet.http.HttpServletRequest;

@Category(Unit.class)
public class ProfileAPIControllerTest {

    @Mock
    private ObjectMapper objectMapper;
    @Mock
    private HttpServletRequest request;
    @Mock
    private ProfileService profileService;
    @Mock
    private ProfileRequestValidator billingProfileRequestValidator;
    @Mock
    private ProfileUpdateRequestValidator profileUpdateRequestValidator;
    @Mock
    private PolicyMemberValidator policyMemberValidator;
    @Mock
    private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
    @Mock
    private JobService jobService;

    private ProfileApiController apiController;

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.openMocks(this);

        apiController = new ProfileApiController(
                objectMapper,
                request,
                profileService,
                billingProfileRequestValidator,
                profileUpdateRequestValidator,
                policyMemberValidator,
                jobService,
                authenticatedUserRequestFactory
        );
    }

    @Test
    public void testCreateProfile() {
        AuthenticatedUserRequest user = mock(AuthenticatedUserRequest.class);
        when(authenticatedUserRequestFactory.from(any())).thenReturn(user);
        when(profileService.createProfile(any(), any())).thenReturn("jobId");

        var jobModel = new JobModel();
        jobModel.setJobStatus(JobStatusEnum.RUNNING);
        when(jobService.retrieveJob(any(), any())).thenReturn(jobModel);

        ResponseEntity entity = apiController.createProfile(new BillingProfileRequestModel());
        assertNotNull(entity);
    }


}
