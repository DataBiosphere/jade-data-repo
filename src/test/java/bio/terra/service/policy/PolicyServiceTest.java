package bio.terra.service.policy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.PolicyServiceConfiguration;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.policy.api.PublicApi;
import bio.terra.policy.api.TpsApi;
import bio.terra.policy.client.ApiException;
import bio.terra.policy.model.TpsComponent;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoCreateRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.policy.exception.PolicyServiceApiException;
import bio.terra.service.policy.exception.PolicyServiceAuthorizationException;
import bio.terra.service.policy.exception.PolicyServiceDuplicateException;
import bio.terra.service.policy.exception.PolicyServiceNotFoundException;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;

@ExtendWith(MockitoExtension.class)
@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
public class PolicyServiceTest {

  @Mock private PolicyServiceConfiguration policyServiceConfiguration;
  @Mock private PolicyApiService policyApiService;
  @Mock private TpsApi tpsApi;
  @Mock private PublicApi tpsUnauthApi;
  private PolicyService policyService;

  @BeforeEach
  public void setup() throws Exception {
    policyService = new PolicyService(policyServiceConfiguration, policyApiService);
  }

  private void mockPolicyServiceConfiguration() {
    when(policyServiceConfiguration.getEnabled()).thenReturn(true);
  }

  private void mockPolicyApi() {
    when(policyApiService.getPolicyApi()).thenReturn(tpsApi);
  }

  private void mockUnauthPolicyApi() {
    when(policyApiService.getUnauthPolicyApi()).thenReturn(tpsUnauthApi);
  }

  @Test
  void testGetProtectedDataPolicyInput() {
    TpsPolicyInput actualPolicy = PolicyService.getProtectedDataPolicyInput();
    TpsPolicyInput expectedPolicy =
        new TpsPolicyInput()
            .namespace(PolicyService.POLICY_NAMESPACE)
            .name(PolicyService.PROTECTED_DATA_POLICY_NAME);
    assertEquals(actualPolicy, expectedPolicy);
  }

  @Test
  void testCreateSnapshotDao() throws Exception {
    mockPolicyApi();
    UUID snapshotId = UUID.randomUUID();
    TpsPolicyInput policy = new TpsPolicyInput().namespace("terra").name("protected-data");
    TpsPolicyInputs policies = new TpsPolicyInputs().addInputsItem(policy);
    policyService.createSnapshotPao(snapshotId, policies);
    verify(tpsApi)
        .createPao(
            new TpsPaoCreateRequest()
                .objectId(snapshotId)
                .component(TpsComponent.TDR)
                .objectType(TpsObjectType.SNAPSHOT)
                .attributes(policies));
  }

  @Test
  void testConvertApiException() {
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

  @Test
  void testStatusOk() {
    mockPolicyServiceConfiguration();
    mockUnauthPolicyApi();
    RepositoryStatusModelSystems status = policyService.status();
    assertTrue(status.isOk());
    assertThat(status.isCritical(), equalTo(policyServiceConfiguration.getEnabled()));
    assertThat(status.getMessage(), containsString("Terra Policy Service status ok"));
  }

  @Test
  void testStatusNotOk() throws Exception {
    mockPolicyServiceConfiguration();
    mockUnauthPolicyApi();
    var exception = new ApiException(HttpStatus.INTERNAL_SERVER_ERROR.value(), "TPS error");
    doThrow(exception).when(tpsUnauthApi).getStatus();
    RepositoryStatusModelSystems status = policyService.status();
    assertFalse(status.isOk());
    assertThat(status.isCritical(), equalTo(policyServiceConfiguration.getEnabled()));
    assertThat(status.getMessage(), containsString(exception.getMessage()));
  }
}
