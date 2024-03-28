package bio.terra.service.profile.google;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class GoogelBillingServiceUnitTest {
  @Test
  public void testGetIdpTokenFromJwtNotAJwt() {
    var user =
        AuthenticatedUserRequest.builder()
            .setToken("not a jwt")
            .setEmail("")
            .setSubjectId("")
            .build();
    assertThat(GoogleBillingService.getIdpAccessTokenFromJwt(user), is(Optional.empty()));
  }

  @Test
  public void testGetIdpTokenFromJwtNoIdpAccessToken() {
    var jwt = new PlainJWT(new JWTClaimsSet.Builder().build());
    var user =
        AuthenticatedUserRequest.builder()
            .setToken(jwt.serialize())
            .setEmail("")
            .setSubjectId("")
            .build();
    assertThat(GoogleBillingService.getIdpAccessTokenFromJwt(user), is(Optional.empty()));
  }

  @Test
  public void testGetIdpTokenFromJwtWithIdpAccessToken() {
    var idpAccessToken = UUID.randomUUID().toString();
    var jwt =
        new PlainJWT(
            new JWTClaimsSet.Builder()
                .claim(GoogleBillingService.IDP_ACCESS_TOKEN_CLAIM, idpAccessToken)
                .build());
    var user =
        AuthenticatedUserRequest.builder()
            .setToken(jwt.serialize())
            .setEmail("")
            .setSubjectId("")
            .build();
    assertThat(
        GoogleBillingService.getIdpAccessTokenFromJwt(user), is(Optional.of(idpAccessToken)));
  }
}
