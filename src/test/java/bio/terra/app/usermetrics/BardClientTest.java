package bio.terra.app.usermetrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.Map;
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
import org.springframework.web.client.RestTemplate;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class BardClientTest {

  @Mock private UserMetricsConfiguration userMetricsConfiguration;
  @Mock private RestTemplate restTemplate;
  private BardClient bardClient;

  private static AuthenticatedUserRequest USER_1 = AuthenticationFixtures.randomUserRequest();
  private static AuthenticatedUserRequest USER_2 = AuthenticationFixtures.randomUserRequest();

  private String SYNC_PATH;
  private String API_PATH;

  @BeforeEach
  void setup() {
    when(userMetricsConfiguration.getSyncRefreshIntervalSeconds()).thenReturn(1000);

    bardClient = new BardClient(userMetricsConfiguration, restTemplate);

    when(restTemplate.exchange(
            eq(bardClient.getApiUrl()), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(new ResponseEntity(HttpStatus.OK));

    API_PATH = bardClient.getApiUrl();
    SYNC_PATH = bardClient.getSyncPathUrl();
  }

  @Test
  void testBardClientLogEvent_happy() {
    when(restTemplate.exchange(
            eq(SYNC_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(new ResponseEntity(HttpStatus.OK));

    logEventForUser(bardClient, USER_1);

    verifyRestTemplatePathAndCount(SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(API_PATH, 1);

    logEventForUser(bardClient, USER_1);

    verifyRestTemplatePathAndCount(SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(API_PATH, 2);

    logEventForUser(bardClient, USER_2);

    verifyRestTemplatePathAndCount(SYNC_PATH, 2);
    verifyRestTemplatePathAndCount(API_PATH, 3);

    logEventForUser(bardClient, USER_2);

    verifyRestTemplatePathAndCount(SYNC_PATH, 2);
    verifyRestTemplatePathAndCount(API_PATH, 4);
  }

  @Test
  void testBardClientLogEvent_sad_sync() {
    when(restTemplate.exchange(
            eq(SYNC_PATH), eq(HttpMethod.POST), any(HttpEntity.class), eq(Void.class)))
        .thenReturn(new ResponseEntity(HttpStatus.FORBIDDEN));

    logEventForUser(bardClient, USER_1);

    verifyRestTemplatePathAndCount(SYNC_PATH, 1);
    verifyRestTemplatePathAndCount(API_PATH, 1);

    logEventForUser(bardClient, USER_1);

    verifyRestTemplatePathAndCount(SYNC_PATH, 2);
    verifyRestTemplatePathAndCount(API_PATH, 2);
  }

  private void verifyRestTemplatePathAndCount(String path, int count) {
    verify(restTemplate, times(count))
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
