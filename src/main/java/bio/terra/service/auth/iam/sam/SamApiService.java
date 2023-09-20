package bio.terra.service.auth.iam.sam;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.GroupApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.api.TermsOfServiceApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SamApiService {

  private final SamConfiguration samConfig;
  private final ConfigurationService configurationService;
  /** OkHttpClients should be shared among requests to reduce latency and save memory * */
  private final OkHttpClient sharedHttpClient;

  @Autowired
  public SamApiService(SamConfiguration samConfig, ConfigurationService configurationService) {
    this.samConfig = samConfig;
    this.configurationService = configurationService;
    this.sharedHttpClient = new ApiClient().getHttpClient();
  }

  public ResourcesApi resourcesApi(String accessToken) {
    return new ResourcesApi(createApiClient(accessToken));
  }

  public StatusApi statusApi() {
    return new StatusApi(createUnauthApiClient());
  }

  public GoogleApi googleApi(String accessToken) {
    return new GoogleApi(createApiClient(accessToken));
  }

  public UsersApi usersApi(String accessToken) {
    return new UsersApi(createApiClient(accessToken));
  }

  public TermsOfServiceApi termsOfServiceApi(String accessToken) {
    return new TermsOfServiceApi(createApiClient(accessToken));
  }

  public GroupApi groupApi(String accessToken) {
    return new GroupApi(createApiClient(accessToken));
  }

  private ApiClient createUnauthApiClient() {
    return new ApiClient()
        .setHttpClient(sharedHttpClient)
        .setUserAgent("OpenAPI-Generator/1.0.0 java") // only logs an error in sam
        .setBasePath(samConfig.basePath());
  }

  private ApiClient createApiClient(String accessToken) {
    ApiClient apiClient = createUnauthApiClient();
    apiClient.setAccessToken(accessToken);

    // Sometimes Sam calls can take longer than the OkHttp default of 10 seconds to return a
    // response.  In those cases, we can see socket timeout exceptions despite the underlying Sam
    // call continuing to execute and possibly succeeding.
    int operationTimeoutSeconds =
        configurationService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS);
    apiClient.setReadTimeout((int) TimeUnit.SECONDS.toMillis(operationTimeoutSeconds));

    return apiClient;
  }
}
