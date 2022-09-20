package bio.terra.app.usermetrics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.RestTemplate;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@SuppressWarnings("rawtypes")
public class BardClientTest {

  @SpyBean UserMetricsConfiguration userMetricsConfiguration;

  private AuthenticatedUserRequest user1;

  private AuthenticatedUserRequest user2;

  private String SYNC_PATH;

  private String API_PATH;

  @Before
  public void setup() {
    SYNC_PATH = userMetricsConfiguration.getBardBasePath() + "/api/syncProfile";
    API_PATH = userMetricsConfiguration.getBardBasePath() + "/api/event";
    user1 =
        AuthenticatedUserRequest.builder()
            .setSubjectId("Bob")
            .setEmail("bob")
            .setToken("bob token")
            .build();

    user2 =
        AuthenticatedUserRequest.builder()
            .setSubjectId("alice")
            .setEmail("alice")
            .setToken("alice token")
            .build();
  }

  @Test
  public void testBardClientLogEvent() {
    BardClient bardClient =
        mock(
            BardClient.class,
            withSettings()
                .useConstructor(userMetricsConfiguration)
                .defaultAnswer(CALLS_REAL_METHODS));
    RestTemplate apiRestTemplate = mock(RestTemplate.class);
    RestTemplate syncRestTemplate = mock(RestTemplate.class);
    ResponseEntity responseEntity = mock(ResponseEntity.class);
    when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
    when(apiRestTemplate.exchange(
            eq(API_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(responseEntity);
    when(syncRestTemplate.exchange(
            eq(SYNC_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(responseEntity);
    doReturn(apiRestTemplate).when(bardClient).getApiRestTemplate();
    doReturn(syncRestTemplate).when(bardClient).getSyncRestTemplate();

    logEventForUser(bardClient, user1);

    verifyRestTemplatePathAndCount(syncRestTemplate, SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(apiRestTemplate, API_PATH, 1);

    logEventForUser(bardClient, user1);

    verifyRestTemplatePathAndCount(syncRestTemplate, SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(apiRestTemplate, API_PATH, 2);
  }

  @Test
  public void testBardClientSyncProfile_happy() {
    BardClient bardClient =
        mock(
            BardClient.class,
            withSettings()
                .useConstructor(userMetricsConfiguration)
                .defaultAnswer(CALLS_REAL_METHODS));
    RestTemplate restTemplate = mock(RestTemplate.class);
    ResponseEntity responseEntity = mock(ResponseEntity.class);
    when(responseEntity.getStatusCode()).thenReturn(HttpStatus.OK);
    when(restTemplate.exchange(
            anyString(), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(responseEntity);
    doReturn(restTemplate).when(bardClient).getSyncRestTemplate();

    // new users should be added to the cache.
    assertTrue(bardClient.syncUser(user1));
    assertTrue(bardClient.syncUser(user2));

    // if the users are in the cache, they shouldn't be added again.
    assertFalse(bardClient.syncUser(user1));
    assertFalse(bardClient.syncUser(user2));
  }

  @Test
  public void testBardClientSyncProfile_sad() {
    BardClient bardClient =
        mock(
            BardClient.class,
            withSettings()
                .useConstructor(userMetricsConfiguration)
                .defaultAnswer(CALLS_REAL_METHODS));
    RestTemplate restTemplate = mock(RestTemplate.class);
    ResponseEntity responseEntity = mock(ResponseEntity.class);
    when(responseEntity.getStatusCode()).thenReturn(HttpStatus.FORBIDDEN);
    when(restTemplate.exchange(
            anyString(), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(responseEntity);
    doReturn(restTemplate).when(bardClient).getSyncRestTemplate();

    // if the remote service returns something but a 2XX error, call sync on each request.
    assertFalse(bardClient.syncUser(user1));
    assertFalse(bardClient.syncUser(user2));

    // even when the calls continue to fail.
    assertFalse(bardClient.syncUser(user1));
    assertFalse(bardClient.syncUser(user2));
  }

  private void verifyRestTemplatePathAndCount(RestTemplate template, String path, int count) {
    verify(template, Mockito.times(count))
        .exchange(eq(path), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class));
  }

  private void logEventForUser(BardClient bardClient, AuthenticatedUserRequest user) {
    bardClient.logEvent(
        user,
        new BardEvent(
            UserMetricsInterceptor.API_EVENT_NAME,
            Map.of(
                BardEventProperties.METHOD_FIELD_NAME, "POST",
                BardEventProperties.PATH_FIELD_NAME, "/foo/bar"),
            "test app",
            "some.dns.entry.org"));
  }
}
