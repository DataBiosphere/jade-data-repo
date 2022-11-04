package bio.terra.service.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.DuosConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class DuosClientTest {

  @MockBean private DuosConfiguration duosConfiguration;
  @Mock private RestTemplate restTemplate;

  private static final String BASE_PATH = "https://consent.dsde-dev.broadinstitute.org";
  private static final String DUOS_ID = "DUOS-123456";
  private static final Integer DUOS_DATASET_ID = 1;

  private DuosClient duosClient;
  private AuthenticatedUserRequest userReq;

  @Before
  public void setup() {
    duosClient = new DuosClient(duosConfiguration, restTemplate);

    when(duosConfiguration.getBasePath()).thenReturn(BASE_PATH);

    userReq =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DuosClientTest")
            .setEmail("DuosClientTest@unit.com")
            .setToken("token")
            .build();
  }

  @Test
  public void testGetDatasetSucceeds() {
    var expectedBody = new DuosDataset(DUOS_DATASET_ID);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(DuosDataset.class)))
        .thenReturn(new ResponseEntity<>(expectedBody, HttpStatus.OK));

    assertThat(
        "A DUOS dataset that exists for the supplied ID is returned",
        duosClient.getDataset(DUOS_ID, userReq),
        equalTo(expectedBody));
  }

  @Test
  public void testGetDatasetThrowsWhenNotFound() {
    var expectedEx = new HttpClientErrorException(HttpStatus.NOT_FOUND);
    when(restTemplate.exchange(
            eq(duosClient.getDatasetUrl(DUOS_ID)),
            eq(HttpMethod.GET),
            any(HttpEntity.class),
            eq(DuosDataset.class)))
        .thenThrow(expectedEx);

    DuosDatasetNotFoundException thrown =
        assertThrows(
            DuosDatasetNotFoundException.class, () -> duosClient.getDataset(DUOS_ID, userReq));
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
            any(HttpEntity.class),
            eq(DuosDataset.class)))
        .thenThrow(expectedEx);

    DuosDatasetBadRequestException thrown =
        assertThrows(
            DuosDatasetBadRequestException.class, () -> duosClient.getDataset(DUOS_ID, userReq));
    assertThat(
        "Exception thrown when supplied DUOS ID is malformed",
        thrown.getCause(),
        equalTo(expectedEx));
  }
}
