package bio.terra.pact.consumer;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTest;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import bio.terra.app.configuration.PolicyServiceConfiguration;
import bio.terra.policy.api.TpsApi;
import bio.terra.policy.client.ApiException;
import bio.terra.policy.model.TpsComponent;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoCreateRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyApiService;
import bio.terra.service.policy.PolicyService;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ActiveProfiles;

@Tag("pact-test")
// @Tag(bio.terra.common.category.Pact.TAG)
@PactConsumerTest
@ActiveProfiles(bio.terra.common.category.Pact.PROFILE)
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = "tps", pactVersion = PactSpecVersion.V3)
class TpsPactTest {

  private TpsApi tps;
  private final UUID snapshotId = UUID.randomUUID();
  private final TpsPolicyInput protectedDataPolicy =
      new TpsPolicyInput()
          .namespace(PolicyService.POLICY_NAMESPACE)
          .name(PolicyService.PROTECTED_DATA_POLICY_NAME);
  private final TpsPolicyInputs policies = new TpsPolicyInputs().addInputsItem(protectedDataPolicy);

  static Map<String, String> contentTypeJsonHeader = Map.of("Content-Type", "application/json");

  @BeforeEach
  void setup(MockServer mockServer) throws Exception {
    var tpsConfig = mock(PolicyServiceConfiguration.class);
    when(tpsConfig.getAccessToken()).thenReturn("dummyToken");
    when(tpsConfig.getBasePath()).thenReturn(mockServer.getUrl());
    PolicyApiService policyApiService = new PolicyApiService(tpsConfig);
    tps = policyApiService.getPolicyApi();
  }

  @Pact(consumer = "datarepo")
  RequestResponsePact createPao(PactDslWithProvider builder) {
    String snapshotId = UUID.randomUUID().toString();
    return builder
        .given("default")
        .uponReceiving("create PAO with ID <uuid>")
        .method("POST")
        .path("/api/policy/v1alpha1/pao")
        .body(
            newJsonBody(
                    object -> {
                      object.stringType("objectId", snapshotId);
                      object.stringType("component", TpsComponent.TDR.getValue());
                      object.stringType("objectType", TpsObjectType.SNAPSHOT.getValue());
                      object.object(
                          "attributes",
                          (attributes) ->
                              attributes.array(
                                  "inputs",
                                  (inputs) ->
                                      inputs.object(
                                          i -> {
                                            i.stringType(
                                                "namespace", PolicyService.POLICY_NAMESPACE);
                                            i.stringType(
                                                "name", PolicyService.PROTECTED_DATA_POLICY_NAME);
                                          })));
                    })
                .build())
        .headers(contentTypeJsonHeader)
        .willRespondWith()
        .status(204)
        .toPact();
  }

  @Pact(consumer = "datarepo")
  RequestResponsePact deletePaoThatDoesNotExist(PactDslWithProvider builder) {
    return builder
        .given("default")
        .uponReceiving("create PAO with ID <uuid>")
        .method("DELETE")
        .path("/api/policy/v1alpha1/pao/" + snapshotId)
        .willRespondWith()
        .status(404)
        .toPact();
  }

  // create snapshot protected data policy - success
  @Test
  @PactTestFor(pactMethod = "createPao")
  void createPaoSuccess(MockServer mockServer) throws ApiException {
    // call createPao with the snapshot id
    tps.createPao(
        new TpsPaoCreateRequest()
            .objectId(snapshotId)
            .component(TpsComponent.TDR)
            .objectType(TpsObjectType.SNAPSHOT)
            .attributes(policies));
  }

  // create snapshot group policy - success

  // create snapshot policy -- conflict error

  // update existing policy (already protected data, update with group policy)
  // state requires policy with this id to already exist

  // delete a policy (exists - requires TPS state to have this policy)

  // delete a policy (does not exist)
  @Test
  @PactTestFor(pactMethod = "deletePaoThatDoesNotExist")
  void deletePaoThatDoesNotExist(MockServer mockServer) {
    assertThrows(
        ApiException.class,
        () -> tps.deletePao(snapshotId),
        "nonexistent policy should return 404");
  }
}
