package bio.terra.app.usermetrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class UserMetricsInterceptorTest {
  @MockBean private BardClient bardClient;
  @Autowired private UserMetricsConfiguration metricsConfig;
  @Captor private ArgumentCaptor<AuthenticatedUserRequest> authCaptor;
  @Autowired private UserMetricsInterceptor metricsInterceptor;

  @Before
  public void setUp() throws Exception {
    metricsConfig.setIgnorePaths(List.of());
  }

  @Test
  public void testSendEvent() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getMethod()).thenReturn("post");
    when(request.getRequestURI()).thenReturn("/foo/bar");
    mockRequestAuth(request);

    runAnWait(request, response);

    verify(bardClient, times(1))
        .logEvent(
            authCaptor.capture(),
            eq(
                new BardEvent(
                    UserMetricsInterceptor.API_EVENT_NAME,
                    Map.of(
                        UserMetricsInterceptor.METHOD_FIELD_NAME, "POST",
                        UserMetricsInterceptor.PATH_FIELD_NAME, "/foo/bar"),
                    "testapp",
                    "some.dnsname.org")));

    assertThat("token is correct", authCaptor.getValue().getToken(), equalTo("footoken"));
  }

  @Test
  public void testSendEventNotFiredWithNoToken() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getMethod()).thenReturn("post");
    when(request.getRequestURI()).thenReturn("/foo/bar");

    runAnWait(request, response);

    verify(bardClient, times(0)).logEvent(any(), any());
  }

  @Test
  public void testSendEventNotFiredWithIgnoredUrl() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getMethod()).thenReturn("post");
    when(request.getRequestURI()).thenReturn("/foo/bar");
    metricsConfig.setIgnorePaths(List.of("/foo/bar"));
    mockRequestAuth(request);

    runAnWait(request, response);

    verify(bardClient, times(0)).logEvent(any(), any());
  }

  @Test
  public void testSendEventNotFiredWithIgnoredWildcardUrl() throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(request.getMethod()).thenReturn("post");
    when(request.getRequestURI()).thenReturn("/foo/bar");
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
    metricsInterceptor.afterCompletion(request, response, null, null);
    // This waits for the threadpool to wrap up so that we can examine the results
    metricsConfig.metricsPerformanceThreadpool().awaitTermination(100, TimeUnit.MILLISECONDS);
  }
}
