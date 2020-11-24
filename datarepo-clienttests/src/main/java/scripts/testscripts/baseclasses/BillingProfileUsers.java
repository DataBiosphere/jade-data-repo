package scripts.testscripts.baseclasses;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;

public class BillingProfileUsers extends runner.TestScript {
  private static final Logger logger = LoggerFactory.getLogger(BillingProfileUsers.class);

  protected TestUserSpecification ownerUser1; // able to create a profile
  protected TestUserSpecification ownerUser2; // able to create a profile and able to create dataset
  protected TestUserSpecification userUser; // able to create a dataset

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    // We require that the order of users be:
    // testUsers[0] = ownerUser1
    // testUsers[1] = ownerUser2
    // testUsers[2] = userUser
    if (testUsers.size() != 3) {
      throw new IllegalArgumentException("BillingProfileUsers requires 3 users");
    }
    ownerUser1 = testUsers.get(0);
    ownerUser2 = testUsers.get(1);
    userUser = testUsers.get(2);
  }
}
