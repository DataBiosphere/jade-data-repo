package bio.terra.integration;

import bio.terra.common.auth.AuthService;
import bio.terra.common.auth.Users;
import bio.terra.common.configuration.TestConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class UsersBase {
  private static final String ADMIN_ROLE = "admin";
  private static final String STEWARD_ROLE = "steward";
  private static final String CUSTODIAN_ROLE = "custodian";
  private static final String READER_ROLE = "reader";
  private static final String DISCOVERER_ROLE = "discoverer";

  @Autowired private Users users;

  @Autowired private AuthService authService;

  private static Logger logger = LoggerFactory.getLogger(UsersBase.class);
  private TestConfiguration.User admin;
  private TestConfiguration.User steward;
  private TestConfiguration.User custodian;
  private TestConfiguration.User reader;
  private TestConfiguration.User discoverer;

  public TestConfiguration.User admin() {
    return admin;
  }

  public TestConfiguration.User admin(String name) {
    return users.getUserForRole(name, ADMIN_ROLE);
  }

  public TestConfiguration.User steward() {
    return steward;
  }

  public TestConfiguration.User steward(String name) {
    return users.getUserForRole(name, STEWARD_ROLE);
  }

  public TestConfiguration.User custodian() {
    return custodian;
  }

  public TestConfiguration.User custodian(String name) {
    return users.getUserForRole(name, CUSTODIAN_ROLE);
  }

  public TestConfiguration.User reader() {
    return reader;
  }

  public TestConfiguration.User reader(String name) {
    return users.getUserForRole(name, READER_ROLE);
  }

  public TestConfiguration.User discoverer() {
    return discoverer;
  }

  public TestConfiguration.User discoverer(String name) {
    return users.getUserForRole(name, DISCOVERER_ROLE);
  }

  protected void setup() throws Exception {
    setup(true);
  }

  protected void setup(boolean shuffle) throws Exception {
    admin = users.getUserForRole(ADMIN_ROLE, shuffle);
    steward = users.getUserForRole(STEWARD_ROLE, shuffle);
    custodian = users.getUserForRole(CUSTODIAN_ROLE, shuffle);
    reader = users.getUserForRole(READER_ROLE, shuffle);
    discoverer = users.getUserForRole(DISCOVERER_ROLE, shuffle);
    logger.info(
        "admin: "
            + admin.getName()
            + "; steward: "
            + steward.getName()
            + "; custodian: "
            + custodian.getName()
            + "; reader: "
            + reader.getName()
            + "; discoverer: "
            + discoverer.getName());
  }
}
