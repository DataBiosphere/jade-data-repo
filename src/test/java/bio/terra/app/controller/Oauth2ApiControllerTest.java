package bio.terra.app.controller;

import static bio.terra.app.controller.Oauth2ApiController.AUTHORIZE_ENDPOINT;
import static bio.terra.app.controller.Oauth2ApiController.TOKEN_REFRESH_ENDPOINT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import bio.terra.app.configuration.OpenIDConnectConfiguration;
import bio.terra.common.category.Unit;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestTemplate;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class Oauth2ApiControllerTest {

  @SpyBean private OpenIDConnectConfiguration openIDConnectConfiguration;
  @SpyBean private Oauth2ApiController controller;
  @Autowired private ObjectMapper objectMapper;
  @Autowired private MockMvc mvc;

  @Captor private ArgumentCaptor<HttpEntity<String>> postCaptor;

  @Before
  public void setUp() throws Exception {
    openIDConnectConfiguration.setAddClientIdToScope(false);
    openIDConnectConfiguration.setExtraAuthParams(null);
  }

  @Test
  public void testForwardingLogic() throws Exception {
    openIDConnectConfiguration.setAddClientIdToScope(false);
    mvc.perform(get(AUTHORIZE_ENDPOINT + "?id=client_idwith\"fun'characters&scope=foo bar"))
        .andExpect(status().is3xxRedirection())
        .andExpect(
            redirectedUrl(
                openIDConnectConfiguration.getAuthorizationEndpoint()
                    + "?id=client_idwith%22fun%27characters&scope=foo+bar"));
  }

  @Test
  public void testForwardingLogicWithClientIdInjected() throws Exception {
    openIDConnectConfiguration.setClientId("my_id");
    openIDConnectConfiguration.setAddClientIdToScope(true);
    mvc.perform(get(AUTHORIZE_ENDPOINT + "?id=client_id&scope=foo bar"))
        .andExpect(status().is3xxRedirection())
        .andExpect(
            redirectedUrl(
                openIDConnectConfiguration.getAuthorizationEndpoint()
                    + "?id=client_id&scope=foo+bar+my_id"));
  }

  @Test
  public void testForwardingLogicWithClientIdInjectedAndNewParameters() throws Exception {
    openIDConnectConfiguration.setClientId("my_id");
    openIDConnectConfiguration.setAddClientIdToScope(true);
    openIDConnectConfiguration.setExtraAuthParams("foo=1&bar='2'");
    mvc.perform(get(AUTHORIZE_ENDPOINT + "?id=client_id&scope=foo bar"))
        .andExpect(status().is3xxRedirection())
        .andExpect(
            redirectedUrl(
                openIDConnectConfiguration.getAuthorizationEndpoint()
                    + "?id=client_id&scope=foo+bar+my_id&foo=1&bar=%272%27"));
  }

  @Test
  public void testProxyTokenLogic() throws Exception {
    testProxyTokenLogic(null);
  }

  @Test
  public void testProxyTokenLogicWithSecret() throws Exception {
    testProxyTokenLogic("supersecret");
  }

  private void testProxyTokenLogic(String clientSecret) throws Exception {
    RestTemplate restTemplate = mock(RestTemplate.class);
    String tokenEndpoint = "http://foo.com/token";
    String requestBody = "access_token=foo";
    JsonNode returnNode = objectMapper.readValue("{\"access_token\": \"tkn\"}", JsonNode.class);
    when(openIDConnectConfiguration.getTokenEndpoint()).thenReturn(tokenEndpoint);
    openIDConnectConfiguration.setClientSecret(clientSecret);
    when(restTemplate.exchange(
            anyString(), eq(HttpMethod.POST), any(HttpEntity.class), eq(JsonNode.class)))
        .thenReturn(ResponseEntity.ok().body(returnNode));
    when(controller.getRestTemplate()).thenReturn(restTemplate);

    mvc.perform(
            post(TOKEN_REFRESH_ENDPOINT)
                .content(requestBody)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.access_token").value("tkn"));

    verify(restTemplate, times(1))
        .exchange(eq(tokenEndpoint), eq(HttpMethod.POST), postCaptor.capture(), eq(JsonNode.class));

    String expectedBody =
        clientSecret == null ? requestBody : requestBody + "&" + "client_secret=" + clientSecret;
    assertThat(
        "proxy call body is passed through",
        postCaptor.getValue().getBody(),
        equalTo(expectedBody));
  }
}
