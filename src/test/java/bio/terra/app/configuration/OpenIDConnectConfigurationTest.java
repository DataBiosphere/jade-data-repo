package bio.terra.app.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class OpenIDConnectConfigurationTest {

  @Test
  public void testInitializeWithGoogleOidc() {
    OpenIDConnectConfiguration openIDConnectConfiguration = new OpenIDConnectConfiguration();

    // Test will read the Oauth config from Google's Oauth endpoint
    openIDConnectConfiguration.setAuthorityEndpoint("https://accounts.google.com");

    openIDConnectConfiguration.init();

    assertThat(
        "there is a authorization endpoint",
        openIDConnectConfiguration.getAuthorizationEndpoint(),
        equalTo("https://accounts.google.com/o/oauth2/v2/auth"));

    assertThat(
        "there is a authorization endpoint",
        openIDConnectConfiguration.getTokenEndpoint(),
        equalTo("https://oauth2.googleapis.com/token"));
  }
}
