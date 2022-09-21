package bio.terra.app.usermetrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  public void testBardClientLogEvent_happy() {
    BardClient bardClient = spy(new BardClient(userMetricsConfiguration));
    RestTemplate restTemplate = mock(RestTemplate.class);
    when(restTemplate.exchange(
            eq(API_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(new ResponseEntity(HttpStatus.OK));
    when(restTemplate.exchange(
            eq(SYNC_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(new ResponseEntity(HttpStatus.OK));
    when(bardClient.getRestTemplate()).thenReturn(restTemplate);

    logEventForUser(bardClient, user1);

    verifyRestTemplatePathAndCount(restTemplate, SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(restTemplate, API_PATH, 1);

    logEventForUser(bardClient, user1);

    verifyRestTemplatePathAndCount(restTemplate, SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(restTemplate, API_PATH, 2);

    logEventForUser(bardClient, user2);

    verifyRestTemplatePathAndCount(restTemplate, SYNC_PATH, 2);
    verifyRestTemplatePathAndCount(restTemplate, API_PATH, 3);

    logEventForUser(bardClient, user2);

    verifyRestTemplatePathAndCount(restTemplate, SYNC_PATH, 2);
    verifyRestTemplatePathAndCount(restTemplate, API_PATH, 4);
  }

  @Test
  public void testBardClientLogEvent_sad_sync() {
    BardClient bardClient = spy(new BardClient(userMetricsConfiguration));
    RestTemplate restTemplate = mock(RestTemplate.class);
    when(restTemplate.exchange(
            eq(API_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(new ResponseEntity(HttpStatus.OK));
    when(restTemplate.exchange(
            eq(SYNC_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(new ResponseEntity(HttpStatus.FORBIDDEN));
    when(bardClient.getRestTemplate()).thenReturn(restTemplate);

    logEventForUser(bardClient, user1);

    verifyRestTemplatePathAndCount(restTemplate, SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(restTemplate, API_PATH, 1);

    logEventForUser(bardClient, user1);

    verifyRestTemplatePathAndCount(restTemplate, SYNC_PATH, 2);
    verifyRestTemplatePathAndCount(restTemplate, API_PATH, 2);
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
