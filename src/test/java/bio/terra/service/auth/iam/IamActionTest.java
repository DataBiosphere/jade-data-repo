package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class IamActionTest {

  @ParameterizedTest
  @EnumSource(IamAction.class)
  void testFromValue(IamAction action) {
    assertThat(
        action + " maps back to itself via IamAction.fromValue",
        IamAction.fromValue(action.toString()),
        equalTo(action));
    assertThat(
        action + " lowercased maps back to itself via IamAction.fromValue",
        IamAction.fromValue(action.toString().toLowerCase()),
        equalTo(action));
    assertThat(
        action + " uppercased maps back to itself via IamAction.fromValue",
        IamAction.fromValue(action.toString().toUpperCase()),
        equalTo(action));
  }
}
