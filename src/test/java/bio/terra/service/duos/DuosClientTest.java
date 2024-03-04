package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.DuosConfiguration;
import bio.terra.common.HttpEntityUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.oauth2.GoogleCredentialsService;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import bio.terra.service.duos.exception.DuosInternalServerErrorException;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
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
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DuosClientTest {

  @Mock private DuosConfiguration duosConfiguration;
  @Mock private RestTemplate restTemplate;
  @Mock private GoogleCredentialsService googleCredentialsService;

  private static final String BASE_PATH = "https://consent.dsde-dev.broadinstitute.org";
  private static final String DUOS_ID = "DUOS-123456";
  private static final Integer DUOS_DATASET_ID = 1;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String TDR_SA_TOKEN = UUID.randomUUID().toString();

  private DuosClient duosClient;

  @BeforeEach
  void setup() {
    duosClient = new DuosClient(duosConfiguration, restTemplate, googleCredentialsService);
  }

  private void mockDuosConfigurationBasePath() {
    when(duosConfiguration.basePath()).thenReturn(BASE_PATH);
  }

  private void mockGoogleCredentialsService() {
    when(googleCredentialsService.getApplicationDefaultAccessToken(DuosClient.SCOPES))
        .thenReturn(TDR_SA_TOKEN);
  }

  @Test
  void testStatusSucceeds() {
    mockDuosConfigurationBasePath();
    var expectedBody = new SystemStatus(true, false, Map.of());
    when(restTemplate.exchange(
            eq(duosClient.getStatusUrl()),
            eq(HttpMethod.GET),
            argThat(HttpEntityUtils::isUnauthenticated),
            eq(SystemStatus.class)))
        .thenReturn(ResponseEntity.ok(expectedBody));

    assertThat("Successful system status returned", duosClient.status(), equalTo(expectedBody));
  }

  @Test
  void testStatusThrows() {
    mockDuosConfigurationBasePath();
    var expectedException = new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
    when(restTemplate.exchange(
            eq(duosClient.getStatusUrl()),
            eq(HttpMethod.GET),
            argThat(HttpEntityUtils::isUnauthenticated),
            eq(SystemStatus.class)))
        .thenThrow(expectedException);

    assertThat(
        "System status exception rethrown",
        assertThrows(HttpServerErrorException.class, () -> duosClient.status()),
        equalTo(expectedException));
  }

  @Test
  void testGetDatasetSucceeds() {
    mockDuosConfigurationBasePath();
    var expectedBody = new DuosDataset(DUOS_DATASET_ID);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(DuosClientTest::hasUserToken),
            eq(DuosDataset.class)))
        .thenReturn(ResponseEntity.ok(expectedBody));

    assertThat(
        "A DUOS dataset that exists for the supplied ID is returned",
        duosClient.getDataset(DUOS_ID, TEST_USER),
        equalTo(expectedBody));
  }

  @Test
  void testGetDatasetThrowsWhenNotFound() {
    mockDuosConfigurationBasePath();
    var expectedEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(DuosClientTest::hasUserToken),
            eq(DuosDataset.class)))
        .thenThrow(expectedEx);

    DuosDatasetNotFoundException thrown =
        assertThrows(
            DuosDatasetNotFoundException.class, () -> duosClient.getDataset(DUOS_ID, TEST_USER));
    assertThat(
        "Exception thrown when DUOS dataset not found for the supplied ID",
        thrown.getCause(),
        equalTo(expectedEx));
  }

  @Test
  void testGetDatasetThrowsWhenIdMalformed() {
    mockDuosConfigurationBasePath();
    var expectedEx = new HttpClientErrorException(HttpStatus.BAD_REQUEST);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(DuosClientTest::hasUserToken),
            eq(DuosDataset.class)))
        .thenThrow(expectedEx);

    DuosDatasetBadRequestException thrown =
        assertThrows(
            DuosDatasetBadRequestException.class, () -> duosClient.getDataset(DUOS_ID, TEST_USER));
    assertThat(
        "Exception thrown when supplied DUOS ID is malformed",
        thrown.getCause(),
        equalTo(expectedEx));
  }

  @Test
  void testGetApprovedUsers() {
    mockDuosConfigurationBasePath();
    mockGoogleCredentialsService();

    var expectedBody =
        new DuosDatasetApprovedUsers(
            Stream.of("a", "b", "c").map(DuosDatasetApprovedUser::new).toList());
    when(restTemplate.exchange(
            eq(duosClient.getApprovedUsersUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(DuosClientTest::hasTdrSaToken),
            eq(DuosDatasetApprovedUsers.class)))
        .thenReturn(ResponseEntity.ok(expectedBody));

    assertThat(
        "Authorized users are returned",
        duosClient.getApprovedUsers(DUOS_ID),
        equalTo(expectedBody));
  }

  @Test
  void testGetApprovedUsersThrows() {
    mockDuosConfigurationBasePath();
    mockGoogleCredentialsService();

    var expectedEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    when(restTemplate.exchange(
            eq(duosClient.getApprovedUsersUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(DuosClientTest::hasTdrSaToken),
            eq(DuosDatasetApprovedUsers.class)))
        .thenThrow(expectedEx);

    DuosDatasetNotFoundException thrown =
        assertThrows(
            DuosDatasetNotFoundException.class, () -> duosClient.getApprovedUsers(DUOS_ID));
    assertThat(
        "Exception thrown when DUOS dataset not found for the supplied ID",
        thrown.getCause(),
        equalTo(expectedEx));
  }

  @Test
  void testConvertDuosExToDataRepoEx() {
    var badRequestEx = new HttpClientErrorException(HttpStatus.BAD_REQUEST);
    assertThat(
        DuosClient.convertToDataRepoException(badRequestEx, DUOS_ID),
        instanceOf(DuosDatasetBadRequestException.class));

    var notFoundEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    assertThat(
        DuosClient.convertToDataRepoException(notFoundEx, DUOS_ID),
        instanceOf(DuosDatasetNotFoundException.class));

    var unexpectedEx = new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT);
    assertThat(
        DuosClient.convertToDataRepoException(unexpectedEx, DUOS_ID),
        instanceOf(DuosInternalServerErrorException.class));

    var internalServerErrorEx = new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(
        DuosClient.convertToDataRepoException(internalServerErrorEx, DUOS_ID),
        instanceOf(DuosInternalServerErrorException.class));
  }

  // Utilities for examining HttpEntity parameters
  private static <T> boolean hasUserToken(HttpEntity<T> httpEntity) {
    return HttpEntityUtils.hasToken(httpEntity, TEST_USER.getToken());
  }

  private static <T> boolean hasTdrSaToken(HttpEntity<T> httpEntity) {
    return HttpEntityUtils.hasToken(httpEntity, TDR_SA_TOKEN);
  }
}
