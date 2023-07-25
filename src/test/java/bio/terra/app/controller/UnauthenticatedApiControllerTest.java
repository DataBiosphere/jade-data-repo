package bio.terra.app.controller;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.OauthConfiguration;
import bio.terra.app.configuration.OpenIDConnectConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.configuration.TerraConfiguration;
import bio.terra.model.RepositoryStatusModel;
import bio.terra.service.job.JobService;
import bio.terra.service.status.StatusService;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(classes = UnauthenticatedApiController.class)
@Tag("bio.terra.common.category.Unit")
@WebMvcTest
public class UnauthenticatedApiControllerTest {

  @Autowired private MockMvc mvc;
  @MockBean private OauthConfiguration oauthConfig;
  @MockBean private OpenIDConnectConfiguration openIDConnectConfiguration;
  @MockBean private JobService jobService;
  @MockBean private StatusService statusService;
  @MockBean private TerraConfiguration terraConfiguration;
  @MockBean private SamConfiguration samConfiguration;

  private void mockGetStatus(boolean ok) {
    when(statusService.getStatus()).thenReturn(new RepositoryStatusModel().ok(ok));
  }

  @Test
  void testStatusDown() throws Exception {
    mockGetStatus(false);
    mvc.perform(get("/status")).andExpect(status().is5xxServerError());
  }

  @Test
  void testStatusUp() throws Exception {
    mockGetStatus(true);
    mvc.perform(get("/status")).andExpect(status().isOk());
  }

  @Test
  void testRetrieveRepositoryConfig() throws Exception {
    String oauthClientId = "oauthClientId";
    String oidcClientId = "oidcClientId";
    String terraUrl = "terra.base.url";
    String samUrl = "sam.base.url";
    String oidcAuthorityEndpoint = "/oidc/authority/endpoint";

    when(oauthConfig.clientId()).thenReturn(oauthClientId);
    when(openIDConnectConfiguration.getClientId()).thenReturn(oidcClientId);
    when(terraConfiguration.getBasePath()).thenReturn(terraUrl);
    when(samConfiguration.basePath()).thenReturn(samUrl);
    when(openIDConnectConfiguration.getAuthorityEndpoint()).thenReturn(oidcAuthorityEndpoint);

    mvc.perform(get("/configuration"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.clientId").value(oauthClientId))
        .andExpect(jsonPath("$.oidcClientId").value(oidcClientId))
        .andExpect(jsonPath("$.terraUrl").value(terraUrl))
        .andExpect(jsonPath("$.samUrl").value(samUrl))
        .andExpect(jsonPath("$.authorityEndpoint").value(oidcAuthorityEndpoint));
  }
}
