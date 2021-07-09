package bio.terra.datarepo.integration;

import bio.terra.datarepo.common.auth.AuthService;
import bio.terra.datarepo.common.auth.Users;
import bio.terra.datarepo.common.configuration.TestConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class UsersBase {

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

  public TestConfiguration.User steward() {
    return steward;
  }

  public TestConfiguration.User custodian() {
    return custodian;
  }

  public TestConfiguration.User reader() {
    return reader;
  }

  public TestConfiguration.User discoverer() {
    return discoverer;
  }

  protected void setup() throws Exception {
    setup(true);
  }

  protected void setup(boolean shuffle) throws Exception {
    admin = users.getUserForRole("admin", shuffle);
    steward = users.getUserForRole("steward", shuffle);
    custodian = users.getUserForRole("custodian", shuffle);
    reader = users.getUserForRole("reader", shuffle);
    discoverer = users.getUserForRole("discoverer", shuffle);
    logger.info(
        "steward: "
            + steward.getName()
            + "; custodian: "
            + custodian.getName()
            + "; reader: "
            + reader.getName()
            + "; discoverer: "
            + discoverer.getName());
  }
}
