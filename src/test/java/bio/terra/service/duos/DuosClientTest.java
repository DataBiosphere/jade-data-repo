package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.DuosConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.oauth2.GoogleCredentialsService;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import bio.terra.service.duos.exception.DuosInternalServerErrorException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.http.HttpHeaders;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DuosClientTest {

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

  @Before
  public void setup() {
    when(googleCredentialsService.scopes(DuosClient.DUOS_SCOPES))
        .thenReturn(googleCredentialsService);
    when(googleCredentialsService.getApplicationDefaultAccessToken()).thenReturn(TDR_SA_TOKEN);

    when(duosConfiguration.getBasePath()).thenReturn(BASE_PATH);

    duosClient = new DuosClient(duosConfiguration, restTemplate, googleCredentialsService);
  }

  @Test
  public void testStatusSucceeds() {
    var expectedBody = new SystemStatus(true, false, Map.of());
    when(restTemplate.exchange(
            eq(duosClient.getStatusUrl()),
            eq(HttpMethod.GET),
            argThat(this::isUnauthenticated),
            eq(SystemStatus.class)))
        .thenReturn(ResponseEntity.ok(expectedBody));

    assertThat("Successful system status returned", duosClient.status(), equalTo(expectedBody));
  }

  @Test
  public void testStatusThrows() {
    var expectedException = new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
    when(restTemplate.exchange(
            eq(duosClient.getStatusUrl()),
            eq(HttpMethod.GET),
            argThat(this::isUnauthenticated),
            eq(SystemStatus.class)))
        .thenThrow(expectedException);

    assertThat(
        "System status exception rethrown",
        assertThrows(HttpServerErrorException.class, () -> duosClient.status()),
        equalTo(expectedException));
  }

  @Test
  public void testGetDatasetSucceeds() {
    var expectedBody = new DuosDataset(DUOS_DATASET_ID);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(this::hasUserToken),
            eq(DuosDataset.class)))
        .thenReturn(ResponseEntity.ok(expectedBody));

    assertThat(
        "A DUOS dataset that exists for the supplied ID is returned",
        duosClient.getDataset(DUOS_ID, TEST_USER),
        equalTo(expectedBody));
  }

  @Test
  public void testGetDatasetThrowsWhenNotFound() {
    var expectedEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(this::hasUserToken),
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
  public void testGetDatasetThrowsWhenIdMalformed() {
    var expectedEx = new HttpClientErrorException(HttpStatus.BAD_REQUEST);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(this::hasUserToken),
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
  public void testGetApprovedUsers() {
    var expectedBody =
        new DuosDatasetApprovedUsers(
            List.of("a", "b", "c").stream().map(DuosDatasetApprovedUser::new).toList());
    when(restTemplate.exchange(
            eq(duosClient.getApprovedUsersUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(this::hasTdrSaToken),
            eq(DuosDatasetApprovedUsers.class)))
        .thenReturn(ResponseEntity.ok(expectedBody));

    assertThat(
        "Authorized users are returned",
        duosClient.getApprovedUsers(DUOS_ID),
        equalTo(expectedBody));
  }

  @Test
  public void testGetApprovedUsersThrows() {
    var expectedEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    when(restTemplate.exchange(
            eq(duosClient.getApprovedUsersUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            argThat(this::hasTdrSaToken),
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
  public void testConvertDuosExToDataRepoEx() {
    var badRequestEx = new HttpClientErrorException(HttpStatus.BAD_REQUEST);
    assertThat(
        DuosClient.convertDuosExToDataRepoEx(badRequestEx, DUOS_ID),
        instanceOf(DuosDatasetBadRequestException.class));

    var notFoundEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    assertThat(
        DuosClient.convertDuosExToDataRepoEx(notFoundEx, DUOS_ID),
        instanceOf(DuosDatasetNotFoundException.class));

    var unexpectedEx = new HttpClientErrorException(HttpStatus.I_AM_A_TEAPOT);
    assertThat(
        DuosClient.convertDuosExToDataRepoEx(unexpectedEx, DUOS_ID),
        instanceOf(DuosInternalServerErrorException.class));

    var internalServerErrorEx = new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
    assertThat(
        DuosClient.convertDuosExToDataRepoEx(internalServerErrorEx, DUOS_ID),
        instanceOf(DuosInternalServerErrorException.class));
  }

  // Utilities for examining HttpEntity parameters
  private List<String> getAuthorizations(HttpEntity httpEntity) {
    return Optional.ofNullable(httpEntity.getHeaders().get(HttpHeaders.AUTHORIZATION))
        .orElse(List.of());
  }

  private boolean isUnauthenticated(HttpEntity httpEntity) {
    return getAuthorizations(httpEntity).isEmpty();
  }

  private boolean hasUserToken(HttpEntity httpEntity) {
    String bearerHeader = "Bearer %s".formatted(TEST_USER.getToken());
    List<String> authorizations = getAuthorizations(httpEntity);
    return authorizations.size() == 1 && authorizations.get(0).equals(bearerHeader);
  }

  private boolean hasTdrSaToken(HttpEntity httpEntity) {
    String bearerHeader = "Bearer %s".formatted(TDR_SA_TOKEN);
    List<String> authorizations = getAuthorizations(httpEntity);
    return authorizations.size() == 1 && authorizations.get(0).equals(bearerHeader);
  }
}
