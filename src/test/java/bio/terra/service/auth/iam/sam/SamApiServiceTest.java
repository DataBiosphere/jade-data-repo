package bio.terra.service.auth.iam.sam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import java.util.Map;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class SamApiServiceTest {

  @Mock private SamConfiguration samConfig;
  @Mock private ConfigurationService configurationService;
  private SamApiService samApiService;

  private static final String TOKEN = "some-access-token";
  private static final int OPERATION_TIMEOUT_SECONDS = 123;
  private static final int OPERATION_TIMEOUT_MILLIS = OPERATION_TIMEOUT_SECONDS * 1000;

  @Before
  public void setUp() throws Exception {
    samApiService = new SamApiService(samConfig, configurationService);

    when(configurationService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS))
        .thenReturn(OPERATION_TIMEOUT_SECONDS);
  }

  @Test
  public void testSamApiClientReadTimeouts() {
    // We only have one unauthorized Sam API -- StatusApi -- and this is unlikely to change.
    assertThat(
        "StatusApi keeps default read timeout",
        samApiService.statusApi().getApiClient().getReadTimeout(),
        equalTo(new ApiClient().getReadTimeout()));

    var authorizedApiClients =
        Map.of(
            "ResourcesApi", samApiService.resourcesApi(TOKEN).getApiClient(),
            "GoogleApi", samApiService.googleApi(TOKEN).getApiClient(),
            "UsersApi", samApiService.usersApi(TOKEN).getApiClient(),
            "TermsOfServiceApi", samApiService.termsOfServiceApi(TOKEN).getApiClient(),
            "GroupApi", samApiService.groupApi(TOKEN).getApiClient());
    authorizedApiClients.forEach(
        (name, client) ->
            assertThat(
                name + " has expected read timeout",
                client.getReadTimeout(),
                equalTo(OPERATION_TIMEOUT_MILLIS)));
  }
}
