package bio.terra.app.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
class OpenIDConnectConfigurationTest {

  @Test
  void testInitializeWithGoogleOidc() {
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

  @Test
  void testGetOIDCMetadata() {
    String authorityEndpoint = "https://oauth-proxy.dsp-eng-tools.broadinstitute.org/b2c";
    String profileName = "b2c_1a_signup_signin_tdr_dev";
    String expectedOidcMetadata =
        "https://oauth-proxy.dsp-eng-tools.broadinstitute.org/b2c/.well-known/openid-configuration?p=b2c_1a_signup_signin_tdr_dev";

    OpenIDConnectConfiguration openIDConnectConfiguration = new OpenIDConnectConfiguration();
    openIDConnectConfiguration.setProfileParam(profileName);
    openIDConnectConfiguration.setAuthorityEndpoint(authorityEndpoint);
    openIDConnectConfiguration.init();

    assertThat(
        "Profile Param is correctly returned",
        openIDConnectConfiguration.getProfileParam(),
        equalTo(profileName));

    assertThat(
        "Expected oidcMetadataUrl is returned",
        openIDConnectConfiguration.getOidcMetadataUrl(),
        equalTo(expectedOidcMetadata));
  }
}
