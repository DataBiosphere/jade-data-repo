package bio.terra.service.auth.oauth2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
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
public class GoogleCredentialsServiceTest {

  @Mock private GoogleCredentials credentials;

  private GoogleCredentialsService service;

  private static final List<String> SCOPES = List.of("openid", "email", "profile");
  private static final String TOKEN_VALUE = UUID.randomUUID().toString();
  private static final AccessToken ACCESS_TOKEN = new AccessToken(TOKEN_VALUE, null);

  @Before
  public void setup() throws IOException {
    service = new GoogleCredentialsService();

    when(credentials.createScopedRequired()).thenReturn(false);
    when(credentials.createScoped(anyList())).thenReturn(credentials);
    when(credentials.refreshAccessToken()).thenReturn(ACCESS_TOKEN);
  }

  @Test
  public void testServiceScopes() {
    assertThat("Service scopes default to empty list", service.getScopes(), equalTo(List.of()));
    assertThat("Service scopes can be set", service.scopes(SCOPES).getScopes(), equalTo(SCOPES));
    assertThat("Set service scopes persist on the service", service.getScopes(), equalTo(SCOPES));
  }

  @Test
  public void testGetAccessTokenScopesNotRequired() {
    // Without scopes
    assertThat(service.getAccessToken(credentials), equalTo(TOKEN_VALUE));
    verify(credentials, never()).createScoped(anyList());

    // With scopes
    service.scopes(SCOPES);
    assertThat(service.getAccessToken(credentials), equalTo(TOKEN_VALUE));
    verify(credentials, never()).createScoped(anyList());
  }

  @Test
  public void testGetAccessTokenScopesRequired() {
    when(credentials.createScopedRequired()).thenReturn(true);

    // Without scopes
    assertThat(service.getAccessToken(credentials), equalTo(TOKEN_VALUE));
    verify(credentials).createScoped(List.of());

    // With scopes
    service.scopes(SCOPES);
    assertThat(service.getAccessToken(credentials), equalTo(TOKEN_VALUE));
    verify(credentials).createScoped(SCOPES);
  }

  @Test
  public void testGetAccessTokenThrows() throws IOException {
    var expectedEx = new IOException("Cannot refresh token");
    when(credentials.refreshAccessToken()).thenThrow(expectedEx);

    GoogleResourceException thrown =
        assertThrows(GoogleResourceException.class, () -> service.getAccessToken(credentials));
    assertThat(
        "Exception thrown when refreshing access token fails",
        thrown.getCause(),
        equalTo(expectedEx));
  }
}
