package scripts.testscripts;

import bio.terra.datarepo.model.BillingProfileModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.BillingProfileUsers;
import scripts.utils.tdrwrapper.DataRepoWrap;

import java.util.List;

public class BillingProfileOwnerHandoffTest extends BillingProfileUsers {
  private static final Logger logger =
      LoggerFactory.getLogger(BillingProfileOwnerHandoffTest.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public BillingProfileOwnerHandoffTest() {
    super();
  }

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    super.setup(testUsers);
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    BillingProfileModel profile = null;
    DataRepoWrap ownerUser1Api = DataRepoWrap.wrapFactory(ownerUser1, server);
    DataRepoWrap ownerUser2Api = DataRepoWrap.wrapFactory(ownerUser2, server);
    DataRepoWrap userUserApi = DataRepoWrap.wrapFactory(userUser, server);

    try {
      profile = ownerUser1Api.createProfile(billingAccount, "profile_permission_test", true);
      String profileId = profile.getId();

      ownerUser1Api.addProfilePolicyMember(ownerUser2.userEmail, profileId, "owner");
      ownerUser2Api.deleteProfilePolicyMember(profileId, "owner", ownerUser1.userEmail);
      // Make sure removed owner can perform none of the operations
      testOperations(ownerUser1Api, RoleState.NONE, profileId);

      ownerUser2Api.addProfilePolicyMember(userUser.userEmail, profileId, "owner");
      testOperations(userUserApi, RoleState.OWNER, profileId);
      userUserApi.deleteProfile(profileId);
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
