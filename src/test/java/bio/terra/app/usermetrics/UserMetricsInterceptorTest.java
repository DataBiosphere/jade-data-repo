package bio.terra.app.usermetrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class UserMetricsInterceptorTest {
  @SpyBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private BardClient bardClient;
  @Autowired private UserLoggingMetrics loggingMetrics;
  @Autowired private UserMetricsConfiguration metricsConfig;
  @Captor private ArgumentCaptor<AuthenticatedUserRequest> authCaptor;
  @SpyBean private UserMetricsInterceptor metricsInterceptor;
  @Mock HttpServletRequest request;
  @Mock HttpServletResponse response;

  @Before
  public void setUp() throws Exception {
    metricsConfig.setIgnorePaths(List.of());
    when(request.getMethod()).thenReturn("post");
    when(request.getRequestURI()).thenReturn("/foo/bar");
  }

  @Test
  public void testSendEvent() throws Exception {
    mockRequestAuth(request);

    runAnWait(request, response);

    verify(bardClient, times(1))
        .logEvent(
            authCaptor.capture(),
            eq(
                new BardEvent(
                    UserMetricsInterceptor.API_EVENT_NAME,
                    Map.of(
                        BardEventProperties.METHOD_FIELD_NAME, "POST",
                        BardEventProperties.PATH_FIELD_NAME, "/foo/bar"),
                    "testapp",
                    "some.dnsname.org")));

    assertThat("token is correct", authCaptor.getValue().getToken(), equalTo("footoken"));
  }

  @Test
  public void testSendEventWithBillingProfileId() throws Exception {
    String billingProfileId = UUID.randomUUID().toString();
    loggingMetrics.set(BardEventProperties.BILLING_PROFILE_ID_FIELD_NAME, billingProfileId);

    mockRequestAuth(request);

    runAnWait(request, response);

    verify(bardClient, times(1))
        .logEvent(
            authCaptor.capture(),
            eq(
                new BardEvent(
                    UserMetricsInterceptor.API_EVENT_NAME,
                    Map.of(
                        BardEventProperties.METHOD_FIELD_NAME, "POST",
                        BardEventProperties.PATH_FIELD_NAME, "/foo/bar",
                        BardEventProperties.BILLING_PROFILE_ID_FIELD_NAME, billingProfileId),
                    "testapp",
                    "some.dnsname.org")));

    assertThat("token is correct", authCaptor.getValue().getToken(), equalTo("footoken"));
  }

  @Test
  public void testSendEventNotFiredWithNoToken() throws Exception {
    doThrow(new UnauthorizedException("Building AuthenticatedUserRequest failed"))
        .when(authenticatedUserRequestFactory)
        .from(any());

    runAnWait(request, response);

    verify(bardClient, times(0)).logEvent(any(), any());
  }

  @Test
  public void testSendEventNotFiredWithIgnoredUrl() throws Exception {
    metricsConfig.setIgnorePaths(List.of("/foo/bar"));
    mockRequestAuth(request);

    runAnWait(request, response);

    verify(bardClient, times(0)).logEvent(any(), any());
  }

  @Test
  public void testSendEventNotFiredWithIgnoredWildcardUrl() throws Exception {
    metricsConfig.setIgnorePaths(List.of("/foo/*"));
    mockRequestAuth(request);

    runAnWait(request, response);

    verify(bardClient, times(0)).logEvent(any(), any());
  }

  private void mockRequestAuth(HttpServletRequest request) {
    when(request.getHeader("Authorization")).thenReturn("Bearer footoken");
    when(request.getHeader("From")).thenReturn("me@me.me");
  }

  private void runAnWait(HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    metricsInterceptor.afterCompletion(request, response, new Object(), null);
    // This waits for the threadpool to wrap up so that we can examine the results
    metricsConfig.metricsPerformanceThreadpool().awaitTermination(100, TimeUnit.MILLISECONDS);
  }
}
