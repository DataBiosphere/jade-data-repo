package bio.terra.service.policy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.PolicyServiceConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.policy.api.PublicApi;
import bio.terra.policy.client.ApiException;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.policy.exception.PolicyServiceApiException;
import bio.terra.service.policy.exception.PolicyServiceAuthorizationException;
import bio.terra.service.policy.exception.PolicyServiceDuplicateException;
import bio.terra.service.policy.exception.PolicyServiceNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class PolicyServiceTest {

  @Mock private PublicApi publicApi;
  @Mock private PolicyServiceConfiguration policyServiceConfiguration;
  private PolicyService policyService;

  @Before
  public void setup() throws Exception {
    when(policyServiceConfiguration.getEnabled()).thenReturn(true);
    when(policyServiceConfiguration.getBasePath())
        .thenReturn("https://tps.dsde-dev.broadinstitute.org");
    policyService = new PolicyService(policyServiceConfiguration);
  }

  @Test
  public void testConvertApiException() {
    var unauthorizedException = new ApiException(HttpStatus.UNAUTHORIZED.value(), "unauthorized");
    assertThat(
        PolicyService.convertApiException(unauthorizedException),
        instanceOf(PolicyServiceAuthorizationException.class));

    var notFoundException = new ApiException(HttpStatus.NOT_FOUND.value(), "not found");
    assertThat(
        PolicyService.convertApiException(notFoundException),
        instanceOf(PolicyServiceNotFoundException.class));

    var badRequestException = new ApiException(HttpStatus.BAD_REQUEST.value(), "duplicate object");
    assertThat(
        PolicyService.convertApiException(badRequestException),
        instanceOf(PolicyServiceDuplicateException.class));

    var conflictException = new ApiException(HttpStatus.CONFLICT.value(), "conflict");
    assertThat(
        PolicyService.convertApiException(conflictException),
        instanceOf(PolicyConflictException.class));

    var generalException = new ApiException(HttpStatus.I_AM_A_TEAPOT.value(), "error");
    assertThat(
        PolicyService.convertApiException(generalException),
        instanceOf(PolicyServiceApiException.class));
  }
}
