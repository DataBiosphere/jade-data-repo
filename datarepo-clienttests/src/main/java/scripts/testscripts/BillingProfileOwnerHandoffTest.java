package scripts.testscripts;

import bio.terra.datarepo.model.BillingProfileModel;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.BillingProfileUsers;
import scripts.utils.tdrwrapper.DataRepoWrap;

public class BillingProfileOwnerHandoffTest extends BillingProfileUsers {
  private static final Logger logger =
      LoggerFactory.getLogger(BillingProfileOwnerHandoffTest.class);
  private String stewardsEmail;

  /** Public constructor so that this class can be instantiated via reflection. */
  public BillingProfileOwnerHandoffTest() {
    super();
  }

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    super.setup(testUsers);
  }

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      stewardsEmail = parameters.get(0);
    }
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    BillingProfileModel profile = null;
    DataRepoWrap ownerUser1Api = DataRepoWrap.wrapFactory(ownerUser1, server);
    DataRepoWrap ownerUser2Api = DataRepoWrap.wrapFactory(ownerUser2, server);
    DataRepoWrap userUserApi = DataRepoWrap.wrapFactory(userUser, server);

    try {
      profile = ownerUser1Api.createProfile(billingAccount, "profile_permission_test", true);
      UUID profileId = profile.getId();

      ownerUser1Api.addProfilePolicyMember(profileId, "owner", userUser.userEmail);
      userUserApi.deleteProfilePolicyMember(profileId, "owner", ownerUser2.userEmail);
      // Make sure removed owner can perform none of the operations
      testOperations(ownerUser2Api, RoleState.NONE, profileId);

      userUserApi.addProfilePolicyMember(profileId, "owner", ownerUser2.userEmail);
      testOperations(ownerUser2Api, RoleState.OWNER, profileId);
      ownerUser2Api.deleteProfile(profileId);
      profile = null;
    } catch (Exception e) {
      logger.error("Error in journey", e);
      e.printStackTrace();
      throw e;
    } finally {
      if (profile != null) {
        ownerUser1Api.deleteProfile(profile.getId());
      }
    }
  }
}
