package bio.terra.app.controller;

import static bio.terra.app.controller.Oauth2ApiController.AUTHORIZE_ENDPOINT;
import static bio.terra.app.controller.Oauth2ApiController.TOKEN_REFRESH_ENDPOINT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.OpenIDConnectConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestTemplate;

@ContextConfiguration(classes = Oauth2ApiController.class)
@Tag("bio.terra.common.category.Unit")
@WebMvcTest
public class Oauth2ApiControllerTest {
  @Autowired private MockMvc mvc;
  @MockBean private RestTemplate restTemplate;
  @MockBean private OpenIDConnectConfiguration openIDConnectConfiguration;
  @Autowired private ObjectMapper objectMapper;

  @Captor private ArgumentCaptor<HttpEntity<String>> postCaptor;

  private static final String OIDC_AUTH_ENDPOINT = "oidc/config/auth/endpoint";

  @BeforeEach
  void setUp() {
    when(openIDConnectConfiguration.getAuthorizationEndpoint()).thenReturn(OIDC_AUTH_ENDPOINT);
    when(openIDConnectConfiguration.isAddClientIdToScope()).thenReturn(false);
  }

  @Test
  void testForwardingLogic() throws Exception {
    mvc.perform(get(AUTHORIZE_ENDPOINT + "?id=client_idwith\"fun'characters&scope=foo bar"))
        .andExpect(status().is3xxRedirection())
        .andExpect(
            redirectedUrl(
                OIDC_AUTH_ENDPOINT + "?id=client_idwith%22fun%27characters&scope=foo+bar"));
  }

  @Test
  void testForwardingLogicWithClientIdInjected() throws Exception {
    when(openIDConnectConfiguration.isAddClientIdToScope()).thenReturn(true);
    when(openIDConnectConfiguration.getClientId()).thenReturn("my_id");

    mvc.perform(get(AUTHORIZE_ENDPOINT + "?id=client_id&scope=foo bar"))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrl(OIDC_AUTH_ENDPOINT + "?id=client_id&scope=foo+bar+my_id"));
  }

  @Test
  void testForwardingLogicWithClientIdInjectedAndNewParameters() throws Exception {
    when(openIDConnectConfiguration.isAddClientIdToScope()).thenReturn(true);
    when(openIDConnectConfiguration.getClientId()).thenReturn("my_id");
    when(openIDConnectConfiguration.getExtraAuthParams()).thenReturn("foo=1&bar='2'");

    mvc.perform(get(AUTHORIZE_ENDPOINT + "?id=client_id&scope=foo bar"))
        .andExpect(status().is3xxRedirection())
        .andExpect(
            redirectedUrl(
                OIDC_AUTH_ENDPOINT + "?id=client_id&scope=foo+bar+my_id&foo=1&bar=%272%27"));
  }

  @Test
  void testProxyTokenLogic() throws Exception {
    testProxyTokenLogic(null);
  }

  @Test
  void testProxyTokenLogicWithSecret() throws Exception {
    testProxyTokenLogic("supersecret");
  }

  private void testProxyTokenLogic(String clientSecret) throws Exception {
    String tokenEndpoint = "http://foo.com/token";
    String requestBody = "access_token=foo";
    JsonNode returnNode = objectMapper.readValue("{\"access_token\": \"tkn\"}", JsonNode.class);

    when(openIDConnectConfiguration.getTokenEndpoint()).thenReturn(tokenEndpoint);
    when(openIDConnectConfiguration.getClientSecret()).thenReturn(clientSecret);
    when(restTemplate.exchange(
            anyString(), eq(HttpMethod.POST), any(HttpEntity.class), eq(JsonNode.class)))
        .thenReturn(ResponseEntity.ok().body(returnNode));

    mvc.perform(
            post(TOKEN_REFRESH_ENDPOINT)
                .content(requestBody)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.access_token").value("tkn"));

    verify(restTemplate)
        .exchange(eq(tokenEndpoint), eq(HttpMethod.POST), postCaptor.capture(), eq(JsonNode.class));

    String expectedBody =
        clientSecret == null ? requestBody : requestBody + "&" + "client_secret=" + clientSecret;
    assertThat(
        "proxy call body is passed through",
        postCaptor.getValue().getBody(),
        equalTo(expectedBody));
  }
}
