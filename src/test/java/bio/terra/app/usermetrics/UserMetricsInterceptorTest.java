package bio.terra.app.usermetrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.model.CloudPlatform;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(classes = UserLoggingMetrics.class)
@SpringBootTest
@Tag(Unit.TAG)
class UserMetricsInterceptorTest {
  @Autowired private UserLoggingMetrics eventProperties;
  @MockBean private BardClient bardClient;
  @MockBean private AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  @MockBean private ApplicationConfiguration applicationConfiguration;
  @MockBean private UserMetricsConfiguration metricsConfig;

  private ExecutorService metricsPerformanceThreadpool;
  private UserMetricsInterceptor userMetricsInterceptor;

  @Captor private ArgumentCaptor<AuthenticatedUserRequest> authCaptor;
  @Mock HttpServletRequest request;
  @Mock HttpServletResponse response;

  private static final String APP_ID = "testapp";
  private static final String BARD_BASE_PATH = "https://bard.com";
  private static final String TOKEN = "footoken";
  private static final String DNS_NAME = "some.dnsname.org";
  private static final String METHOD = "post";
  private static final String REQUEST_URI = "/foo/bar";
  private static final Integer NUM_EXECUTOR_THREADS = 3;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.userRequest(TOKEN);

  @BeforeEach
  void setUp() {
    eventProperties.get().clear();
    when(metricsConfig.ignorePaths()).thenReturn(List.of());
    when(metricsConfig.appId()).thenReturn(APP_ID);
    when(metricsConfig.bardBasePath()).thenReturn(BARD_BASE_PATH);

    when(applicationConfiguration.getDnsName()).thenReturn(DNS_NAME);

    when(request.getMethod()).thenReturn(METHOD.toLowerCase());
    when(request.getRequestURI()).thenReturn(REQUEST_URI);

    metricsPerformanceThreadpool = Executors.newFixedThreadPool(NUM_EXECUTOR_THREADS);

    userMetricsInterceptor =
        new UserMetricsInterceptor(
            bardClient,
            authenticatedUserRequestFactory,
            applicationConfiguration,
            metricsConfig,
            eventProperties,
            metricsPerformanceThreadpool);
  }

  @AfterEach
  void tearDown() {
    if (metricsPerformanceThreadpool != null) {
      metricsPerformanceThreadpool.shutdownNow();
    }
  }

  private void mockRequestAuth(HttpServletRequest request) {
    when(authenticatedUserRequestFactory.from(request)).thenReturn(TEST_USER);
  }

  @Test
  void testSendEvent() throws Exception {
    mockRequestAuth(request);
    runAndWait();

    verify(bardClient)
        .logEvent(
            authCaptor.capture(),
            eq(
                new BardEvent(
                    UserMetricsInterceptor.API_EVENT_NAME,
                    Map.of(
                        BardEventProperties.METHOD_FIELD_NAME,
                        METHOD.toUpperCase(),
                        BardEventProperties.PATH_FIELD_NAME,
                        REQUEST_URI),
                    APP_ID,
                    DNS_NAME)));

    assertThat("token is correct", authCaptor.getValue().getToken(), equalTo(TOKEN));
  }

  @Test
  void testSendEventWithAdditionalProperties() throws Exception {
    UUID datasetId = UUID.randomUUID();
    String datasetName = "datasetName";
    UUID snapshotId = UUID.randomUUID();
    String snapshotName = "snapshotName";
    String billingProfileId = UUID.randomUUID().toString();
    CloudPlatform cloudPlatform = CloudPlatform.GCP;

    eventProperties.set(BardEventProperties.DATASET_ID_FIELD_NAME, datasetId);
    eventProperties.set(BardEventProperties.DATASET_NAME_FIELD_NAME, datasetName);
    eventProperties.set(BardEventProperties.SNAPSHOT_ID_FIELD_NAME, snapshotId);
    eventProperties.set(BardEventProperties.SNAPSHOT_NAME_FIELD_NAME, snapshotName);
    eventProperties.set(BardEventProperties.BILLING_PROFILE_ID_FIELD_NAME, billingProfileId);
    eventProperties.set(BardEventProperties.CLOUD_PLATFORM_FIELD_NAME, cloudPlatform);

    mockRequestAuth(request);

    runAndWait();

    verify(bardClient)
        .logEvent(
            authCaptor.capture(),
            eq(
                new BardEvent(
                    UserMetricsInterceptor.API_EVENT_NAME,
                    Map.of(
                        BardEventProperties.METHOD_FIELD_NAME, METHOD.toUpperCase(),
                        BardEventProperties.PATH_FIELD_NAME, REQUEST_URI,
                        BardEventProperties.DATASET_ID_FIELD_NAME, datasetId,
                        BardEventProperties.DATASET_NAME_FIELD_NAME, datasetName,
                        BardEventProperties.SNAPSHOT_ID_FIELD_NAME, snapshotId,
                        BardEventProperties.SNAPSHOT_NAME_FIELD_NAME, snapshotName,
                        BardEventProperties.BILLING_PROFILE_ID_FIELD_NAME, billingProfileId,
                        BardEventProperties.CLOUD_PLATFORM_FIELD_NAME, cloudPlatform),
                    APP_ID,
                    DNS_NAME)));

    assertThat("token is correct", authCaptor.getValue().getToken(), equalTo(TOKEN));
  }

  @Test
  void testSendEventWithTransactionIdHeader() throws Exception {
    String transactionId = UUID.randomUUID().toString();
    when(request.getHeader("X-Transaction-Id")).thenReturn(transactionId);
    mockRequestAuth(request);

    runAndWait();

    verify(bardClient)
        .logEvent(
            authCaptor.capture(),
            eq(
                new BardEvent(
                    UserMetricsInterceptor.API_EVENT_NAME,
                    Map.of(
                        BardEventProperties.METHOD_FIELD_NAME,
                        METHOD.toUpperCase(),
                        BardEventProperties.PATH_FIELD_NAME,
                        REQUEST_URI,
                        BardEventProperties.TRANSACTION_ID_FIELD_NAME,
                        transactionId),
                    APP_ID,
                    DNS_NAME)));

    assertThat("token is correct", authCaptor.getValue().getToken(), equalTo(TOKEN));
  }

  @Test
  void testSendEventNotFiredWithNoBardBasePath() throws Exception {
    when(metricsConfig.bardBasePath()).thenReturn(null);

    runAndWait();

    verify(bardClient, never()).logEvent(any(), any());
  }

  @Test
  void testSendEventNotFiredWithNoToken() throws Exception {
    when(authenticatedUserRequestFactory.from(any()))
        .thenThrow(new UnauthorizedException("Building AuthenticatedUserRequest failed"));

    runAndWait();

    verify(bardClient, never()).logEvent(any(), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {REQUEST_URI, "/foo/*"})
  void testSendEventNotFiredWithIgnoredUrl(String ignorePath) throws Exception {
    when(metricsConfig.ignorePaths()).thenReturn(List.of(ignorePath));
    mockRequestAuth(request);

    runAndWait();

    verify(bardClient, never()).logEvent(any(), any());
  }

  private void runAndWait() throws Exception {
    userMetricsInterceptor.afterCompletion(request, response, new Object(), null);
    // This waits for the threadpool to wrap up so that we can examine the results
    metricsPerformanceThreadpool.awaitTermination(100, TimeUnit.MILLISECONDS);
  }
}
