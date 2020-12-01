package scripts.testscripts;

import bio.terra.datarepo.model.BillingProfileModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.BillingProfileUsers;
import scripts.utils.tdrwrapper.DataRepoWrap;

import java.util.List;

public class BillingProfileRoleAccessTest extends BillingProfileUsers {
  private static final Logger logger = LoggerFactory.getLogger(BillingProfileRoleAccessTest.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public BillingProfileRoleAccessTest() {
    super();
  }

  private BillingProfileModel profile;

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    super.setup(testUsers);
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    // TODO: two other tests
    //  test owner trade off
    //  test can't delete bill profile if in use

    DataRepoWrap ownerUser1Api = DataRepoWrap.wrapFactory(ownerUser1, server); // dumbledore
    DataRepoWrap ownerUser2Api = DataRepoWrap.wrapFactory(ownerUser2, server); // voldemort

    try {
      profile = ownerUser1Api.createProfile(billingAccount, "profile_permission_test", true);
      String profileId = profile.getId();
      logger.info("ProfileId: {}", profileId);
      // Make sure the owner can perform all operations
      testOperations(ownerUser1Api, RoleState.OWNER, profileId);

      // Make sure an unpermissioned user can perform none of the operations
      testOperations(ownerUser2Api, RoleState.NONE, profileId);

      // Make sure someone with user role can do some of the operations
      ownerUser1Api.addProfilePolicyMember(profileId, "user", ownerUser2.userEmail);
      dumpPolicies(ownerUser1Api, profileId);
      testOperations(ownerUser2Api, RoleState.USER, profileId);

      // Remove permissions and make sure they are truly gone
      ownerUser1Api.deleteProfilePolicyMember(profileId, "user", ownerUser2.userEmail);
      dumpPolicies(ownerUser1Api, profileId);
      testOperations(ownerUser2Api, RoleState.NONE, profileId);

      // Make sure another owner can do all of the operations
      ownerUser1Api.addProfilePolicyMember(profileId, "owner", ownerUser2.userEmail);
      testOperations(ownerUser2Api, RoleState.OWNER, profileId);

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
