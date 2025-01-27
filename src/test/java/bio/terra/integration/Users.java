package bio.terra.integration;

import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.configuration.TestConfiguration.User;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Users {
  private static final Logger logger = LoggerFactory.getLogger(Users.class);

  private static final String ADMIN_ROLE = "admin";
  private static final String STEWARD_ROLE = "steward";
  private static final String CUSTODIAN_ROLE = "custodian";
  private static final String READER_ROLE = "reader";
  private static final String DISCOVERER_ROLE = "discoverer";

  private final Map<String, List<User>> usersByRole;

  public record TestUsers(User admin, User steward, User custodian, User reader, User discoverer) {}

  @Autowired
  public Users(TestConfiguration testConfig) {
    usersByRole = testConfig.users().stream().collect(Collectors.groupingBy(User::role));
  }

  private User getUserForRole(String name, String role) {
    var usersForRole = usersByRole.get(role);
    return usersForRole.stream()
        .filter(u -> u.name().equals(name))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "User %s with role %s was not found.  Available are: [%s]",
                        name,
                        role,
                        usersForRole.stream().map(User::name).collect(Collectors.joining(", ")))));
  }

  private User getUserForRole(String role) {
    var users = usersByRole.get(role);
    return users.get(new Random().nextInt(users.size()));
  }

  public User admin() {
    return getUserForRole(ADMIN_ROLE);
  }

  public User admin(String name) {
    return getUserForRole(name, ADMIN_ROLE);
  }

  public User steward() {
    return getUserForRole(STEWARD_ROLE);
  }

  public User steward(String name) {
    return getUserForRole(name, STEWARD_ROLE);
  }

  public User custodian() {
    return getUserForRole(CUSTODIAN_ROLE);
  }

  public User custodian(String name) {
    return getUserForRole(name, CUSTODIAN_ROLE);
  }

  public User reader() {
    return getUserForRole(READER_ROLE);
  }

  public User reader(String name) {
    return getUserForRole(name, READER_ROLE);
  }

  public User discoverer() {
    return getUserForRole(DISCOVERER_ROLE);
  }

  public User discoverer(String name) {
    return getUserForRole(name, DISCOVERER_ROLE);
  }

  public TestUsers testUsers() {
    TestUsers testUsers = new TestUsers(admin(), steward(), custodian(), reader(), discoverer());
    logger.info("admin: {}; steward: {}; custodian: {}; reader: {}; discoverer: {}",
        testUsers.admin().name(), testUsers.steward().name(), testUsers.custodian().name(),
        testUsers.reader().name(), testUsers.discoverer().name());
    return testUsers;
  }
}
