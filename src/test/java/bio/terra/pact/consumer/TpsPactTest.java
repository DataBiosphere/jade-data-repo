package bio.terra.pact.consumer;

import static au.com.dius.pact.consumer.dsl.LambdaDsl.newJsonBody;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.DslPart;
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
import bio.terra.policy.model.TpsPaoUpdateRequest;
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

  TpsPaoCreateRequest createPAORequest =
      new TpsPaoCreateRequest()
          .objectId(snapshotId)
          .component(TpsComponent.TDR)
          .objectType(TpsObjectType.SNAPSHOT)
          .attributes(new TpsPolicyInputs().addInputsItem(protectedDataPolicy));

  private final String groupName = "testGroup";
  private final TpsPolicyInput groupConstraintPolicy =
      PolicyService.getGroupConstraintPolicyInput(groupName);
  TpsPaoUpdateRequest updatePAORequest =
      new TpsPaoUpdateRequest()
          .updateMode(PolicyService.UPDATE_MODE)
          .addAttributes(new TpsPolicyInputs().addInputsItem(groupConstraintPolicy));

  static Map<String, String> contentTypeJsonHeader = Map.of("Content-Type", "application/json");

  DslPart createPaoJsonBody =
      newJsonBody(
              object -> {
                object.stringType("objectId", snapshotId.toString());
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
                                      i.stringType("namespace", PolicyService.POLICY_NAMESPACE);
                                      i.stringType(
                                          "name", PolicyService.PROTECTED_DATA_POLICY_NAME);
                                    })));
              })
          .build();

  DslPart updatePaoJsonBody =
      newJsonBody(
              object -> {
                object.stringType("updateMode", PolicyService.UPDATE_MODE.getValue());
                object.object(
                    "addAttributes",
                    (attributes) ->
                        attributes.array(
                            "inputs",
                            (inputs) ->
                                inputs.object(
                                    i -> {
                                      i.stringType("namespace", PolicyService.POLICY_NAMESPACE);
                                      i.stringType(
                                          "name", PolicyService.GROUP_CONSTRAINT_POLICY_NAME);
                                      i.array(
                                          "additionalData",
                                          (additionalData) ->
                                              additionalData.object(
                                                  data -> {
                                                    data.stringType(
                                                        "key",
                                                        PolicyService.GROUP_CONSTRAINT_KEY_NAME);
                                                    data.stringType("value", "testGroup");
                                                  }));
                                    })));
              })
          .build();

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
    return builder
        .given("a PAO with this id does not exist")
        .uponReceiving("create PAO for TDR snapshot")
        .method("POST")
        .path("/api/policy/v1alpha1/pao")
        .body(createPaoJsonBody)
        .headers(contentTypeJsonHeader)
        .willRespondWith()
        .status(204)
        .toPact();
  }

  @Pact(consumer = "datarepo")
  RequestResponsePact createPaoConflict(PactDslWithProvider builder) {
    return builder
        .given("a PAO with this id exists")
        .uponReceiving("create PAO for TDR snapshot throws conflict error")
        .method("POST")
        .path("/api/policy/v1alpha1/pao")
        .body(createPaoJsonBody)
        .headers(contentTypeJsonHeader)
        .willRespondWith()
        .status(409)
        .toPact();
  }

  @Pact(consumer = "datarepo")
  RequestResponsePact updatePao(PactDslWithProvider builder) {
    return builder
        .given("a PAO with a protected-data policy exists for this snapshot")
        .uponReceiving("update snapshot PAO with group constraint policy")
        .method("PATCH")
        .path("/api/policy/v1alpha1/pao/" + snapshotId)
        .body(updatePaoJsonBody)
        .headers(contentTypeJsonHeader)
        .willRespondWith()
        .status(200)
        .headers(contentTypeJsonHeader)
        .toPact();
  }

  @Pact(consumer = "datarepo")
  RequestResponsePact updatePaoConflict(PactDslWithProvider builder) {
    return builder
        .given("a PAO with a group constraint policy exists for this snapshot")
        .uponReceiving("update snapshot PAO with duplicate group constraint policy")
        .method("PATCH")
        .path("/api/policy/v1alpha1/pao/" + snapshotId)
        .body(updatePaoJsonBody)
        .headers(contentTypeJsonHeader)
        .willRespondWith()
        .status(409)
        .toPact();
  }

  @Pact(consumer = "datarepo")
  RequestResponsePact deletePao(PactDslWithProvider builder) {
    return builder
        .given("a PAO with this id exists")
        .uponReceiving("delete PAO")
        .method("DELETE")
        .path("/api/policy/v1alpha1/pao/" + snapshotId)
        .willRespondWith()
        .status(200)
        .toPact();
  }

  @Pact(consumer = "datarepo")
  RequestResponsePact deletePaoThatDoesNotExist(PactDslWithProvider builder) {
    return builder
        .given("a PAO with this id does not exist")
        .uponReceiving("delete non-existent PAO")
        .method("DELETE")
        .path("/api/policy/v1alpha1/pao/" + snapshotId)
        .willRespondWith()
        .status(404)
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "createPao")
  void createPaoSuccess(MockServer mockServer) throws ApiException {
    tps.createPao(createPAORequest);
  }

  @Test
  @PactTestFor(pactMethod = "createPaoConflict")
  void createPaoConflictError(MockServer mockServer) throws ApiException {
    assertThrows(
        ApiException.class,
        () -> tps.createPao(createPAORequest),
        "creating a policy should return 409 if one already exists");
  }

  @Test
  @PactTestFor(pactMethod = "updatePao")
  void updatePaoSuccess(MockServer mockServer) throws ApiException {
    tps.updatePao(updatePAORequest, snapshotId);
  }

  @Test
  @PactTestFor(pactMethod = "updatePaoConflict")
  void updatePaoWithDuplicatePolicy(MockServer mockServer) {
    assertThrows(
        ApiException.class,
        () -> tps.updatePao(updatePAORequest, snapshotId),
        "updating pao with duplicate policy should return 409");
  }

  @Test
  @PactTestFor(pactMethod = "deletePao")
  void deletePaoSuccess(MockServer mockServer) throws ApiException {
    tps.deletePao(snapshotId);
  }

  @Test
  @PactTestFor(pactMethod = "deletePaoThatDoesNotExist")
  void deletePaoThatDoesNotExist(MockServer mockServer) {
    assertThrows(
        ApiException.class,
        () -> tps.deletePao(snapshotId),
        "nonexistent policy should return 404");
  }
}
