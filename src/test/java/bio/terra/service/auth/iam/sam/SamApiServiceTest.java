package bio.terra.service.auth.iam.sam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import okhttp3.ConnectionPool;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class SamApiServiceTest {

  @Mock private SamConfiguration samConfig;
  @Mock private ConfigurationService configurationService;
  private SamApiService samApiService;
  private Map<String, ApiClient> unauthorizedApiClients;
  private Map<String, ApiClient> authorizedApiClients;

  private static final String TOKEN = "some-access-token";
  private static final int OPERATION_TIMEOUT_SECONDS = 123;
  private static final int OPERATION_TIMEOUT_MILLIS = OPERATION_TIMEOUT_SECONDS * 1000;

  @BeforeEach
  void setUp() {
    when(configurationService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS))
        .thenReturn(OPERATION_TIMEOUT_SECONDS);
    samApiService = new SamApiService(samConfig, configurationService);
    unauthorizedApiClients = Map.of("StatusApi", samApiService.statusApi().getApiClient());
    authorizedApiClients =
        Map.of(
            "ResourcesApi", samApiService.resourcesApi(TOKEN).getApiClient(),
            "GoogleApi", samApiService.googleApi(TOKEN).getApiClient(),
            "UsersApi", samApiService.usersApi(TOKEN).getApiClient(),
            "TermsOfServiceApi", samApiService.termsOfServiceApi(TOKEN).getApiClient(),
            "GroupApi", samApiService.groupApi(TOKEN).getApiClient());
  }

  @Test
  void testSamApiClientReadTimeouts() {
    unauthorizedApiClients.forEach(
        (name, client) ->
            assertThat(
                name + " keeps default read timeout",
                client.getReadTimeout(),
                equalTo(new ApiClient().getReadTimeout())));
    authorizedApiClients.forEach(
        (name, client) ->
            assertThat(
                name + " has expected read timeout",
                client.getReadTimeout(),
                equalTo(OPERATION_TIMEOUT_MILLIS)));
  }

  @Test
  void testSamApiClientsShareHttpClient() {
    List<ApiClient> apiClients = new ArrayList<>();
    apiClients.addAll(unauthorizedApiClients.values());
    apiClients.addAll(authorizedApiClients.values());

    // Our intention is that all Sam ApiClients share a common HttpClient, but under the covers
    // we could have many HttpClients built to support different configuration that all reuse the
    // same connection pool.
    // As a proxy check, we make sure that the same connection pool is reused.
    List<ConnectionPool> connectionPools =
        apiClients.stream().map(ac -> ac.getHttpClient().connectionPool()).distinct().toList();
    assertThat("All Sam ApiClients share a connection pool", connectionPools, hasSize(1));
  }
}
