package bio.terra.service.rawls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.RawlsConfiguration;
import bio.terra.common.HttpEntityUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class RawlsClientTest {
  @Mock private RestTemplate restTemplate;
  private RawlsClient rawlsClient;

  private static final String BASE_PATH = "rawls.base.path";
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID WORKSPACE_ID = UUID.randomUUID();
  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";

  @BeforeEach
  void beforeEach() {
    var rawlsConfiguration = new RawlsConfiguration(BASE_PATH);
    rawlsClient = new RawlsClient(rawlsConfiguration, restTemplate);
  }

  @Test
  void getWorkspace() {
    var expectedBody =
        new WorkspaceResponse(new WorkspaceDetails(WORKSPACE_ID.toString(), NAMESPACE, NAME));
    when(restTemplate.exchange(
            eq(rawlsClient.getWorkspaceEndpoint(WORKSPACE_ID)),
            eq(HttpMethod.GET),
            argThat(RawlsClientTest::hasUserToken),
            eq(WorkspaceResponse.class)))
        .thenReturn(ResponseEntity.ok(expectedBody));

    assertThat(
        "An accessible workspace with the given ID is returned",
        rawlsClient.getWorkspace(WORKSPACE_ID, TEST_USER),
        equalTo(expectedBody));
  }

  @Test
  void getWorkspace_NotFound() {
    var expectedEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    when(restTemplate.exchange(
            eq(rawlsClient.getWorkspaceEndpoint(WORKSPACE_ID)),
            eq(HttpMethod.GET),
            argThat(RawlsClientTest::hasUserToken),
            eq(WorkspaceResponse.class)))
        .thenThrow(expectedEx);

    HttpClientErrorException thrown =
        assertThrows(
            HttpClientErrorException.class,
            () -> rawlsClient.getWorkspace(WORKSPACE_ID, TEST_USER));
    assertThat(thrown, equalTo(expectedEx));
  }

  private static <T> boolean hasUserToken(HttpEntity<T> httpEntity) {
    return HttpEntityUtils.hasToken(httpEntity, TEST_USER.getToken());
  }
}
