package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.DuosConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import java.util.List;
import java.util.Map;
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

  private static final String BASE_PATH = "https://consent.dsde-dev.broadinstitute.org";
  private static final String DUOS_ID = "DUOS-123456";
  private static final Integer DUOS_DATASET_ID = 1;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private DuosClient duosClient;

  @Before
  public void setup() {
    duosClient = new DuosClient(duosConfiguration, restTemplate);

    when(duosConfiguration.getBasePath()).thenReturn(BASE_PATH);
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

  private boolean isUnauthenticated(HttpEntity httpEntity) {
    List<String> authorizations = httpEntity.getHeaders().get(HttpHeaders.AUTHORIZATION);
    return authorizations == null || authorizations.isEmpty();
  }

  private boolean hasUserToken(HttpEntity httpEntity) {
    String bearerHeader = "Bearer %s".formatted(TEST_USER.getToken());
    List<String> authorizations = httpEntity.getHeaders().get(HttpHeaders.AUTHORIZATION);
    return authorizations.size() == 1 && authorizations.get(0).equals(bearerHeader);
  }
}
