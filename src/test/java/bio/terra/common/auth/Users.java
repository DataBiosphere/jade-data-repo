package bio.terra.common.auth;

import bio.terra.common.configuration.TestConfiguration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Users {

  private Map<String, List<TestConfiguration.User>> usersByRole = new HashMap<>();
  private Map<String, TestConfiguration.User> userByName = new HashMap<>();

  @Autowired
  public Users(TestConfiguration testConfig) {
    buildUsersByRole(testConfig.getUsers());
  }

  private void buildUsersByRole(List<TestConfiguration.User> users) {
    users.stream()
        .forEach(
            user -> {
              String role = user.getRole();
              List<TestConfiguration.User> newList = new ArrayList<>();

              if (usersByRole.containsKey(role)) newList = usersByRole.get(role);
              newList.add(user);
              usersByRole.put(role, newList);
              userByName.put(user.getName(), user);
            });
  }

  public TestConfiguration.User getUserForRole(String role) {
    return getUserForRole(role, true);
  }

  public TestConfiguration.User getUserForRole(String role, boolean shuffle) {
    return getUsersForRole(role, 1, shuffle).get(0);
  }

  public TestConfiguration.User getUser(String name) {
    return userByName.get(name);
  }

  public List<TestConfiguration.User> getUsersForRole(String role, int numUsers, boolean shuffle) {
    if (role == null) {
      throw new RuntimeException("Role not specified");
    }
    List<TestConfiguration.User> usersList = usersByRole.get(role);
    if (usersList.size() < numUsers) {
      throw new RuntimeException("not enough users for " + role);
    }
    if (shuffle) {
      Collections.shuffle(usersList);
    }
    return usersList.subList(0, numUsers);
  }
}
