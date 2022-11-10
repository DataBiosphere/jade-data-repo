package bio.terra.service.auth.iam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IamActionTest {

  @Test
  public void testFromValue() {
    for (IamAction action : IamAction.values()) {
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
}
