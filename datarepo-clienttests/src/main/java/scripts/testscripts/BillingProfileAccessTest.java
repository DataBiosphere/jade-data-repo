package scripts.testscripts;

import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.BillingProfileUsers;
import scripts.utils.tdrwrapper.DataRepoWrap;
import scripts.utils.tdrwrapper.exception.DataRepoNotFoundClientException;
import scripts.utils.tdrwrapper.exception.DataRepoUnauthorizedClientException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class BillingProfileAccessTest extends BillingProfileUsers {
  private static final Logger logger = LoggerFactory.getLogger(BillingProfileAccessTest.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public BillingProfileAccessTest() {
    super();
  }

  private BillingProfileModel profile;

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    super.setup(testUsers);
  }

  public void setParameters(List<String> parameters) throws Exception {
    /* maybe error if any parameters are provided?
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a number of files to load in the parameters list");
    } else {
      filesToLoad = Integer.parseInt(parameters.get(0));
    }

       */
  }

  private enum RoleState {
    NONE,
    USER,
    OWNER;
  }

  // Run is sequence of operations collecting their results. At the end compare the results
  // to what is expected for their role state.
  private void testOperations(DataRepoWrap wrap, RoleState role, String profileId)
      throws Exception {

    // Test for ability to enumerate the profile
    boolean foundEnumeration = false;
    EnumerateBillingProfileModel enumeration = wrap.enumerateProfiles(0, 1000);
    for (BillingProfileModel profile : enumeration.getItems()) {
      if (StringUtils.equals(profileId, profile.getId())) {
        foundEnumeration = true;
      }
    }
    logger.info("Role {} enumeration {}", role, foundEnumeration);

    // Test for ability to retrieve the profile
    boolean retrieveSuccess;
    try {
      wrap.retrieveProfile(profileId);
      retrieveSuccess = true;
    } catch (DataRepoUnauthorizedClientException ex) {
      retrieveSuccess = false;
    }
    logger.info("Role {} retrieve {}", role, retrieveSuccess);

    // Test for ability to use the profile to create a dataset
    boolean datasetSuccess;
    try {
      DatasetSummaryModel datasetSummaryModel =
          wrap.createDataset(profileId, "dataset-simple.json", true);
      wrap.deleteDataset(datasetSummaryModel.getId());
      datasetSuccess = true;
    } catch (DataRepoUnauthorizedClientException ex) {
      datasetSuccess = false;
    }
    logger.info("Role {} dataset {}", role, datasetSuccess);

    // Test for ability to add a user to the user role of the profile
    boolean addUserSuccess;
    try {
      wrap.addProfilePolicyMember(profileId, "user", userUser.userEmail);
      addUserSuccess = true;
    } catch (DataRepoNotFoundClientException | DataRepoUnauthorizedClientException ex) {
      addUserSuccess = false;
    }
    logger.info("Role {} addUser {}", role, addUserSuccess);

    // Test for ability to delete a user from the user role of the profile
    boolean deleteUserSuccess = addUserSuccess;
    if (addUserSuccess) {
      try {
        wrap.deleteProfilePolicyMember(profileId, "user", userUser.userEmail);
        deleteUserSuccess = true;
      } catch (DataRepoNotFoundClientException | DataRepoUnauthorizedClientException ex) {
        deleteUserSuccess = false;
      }
    }
    logger.info("Role {} deleteUser {}", role, deleteUserSuccess);

    // Test for ability to retrieved the policies of a profile
    boolean retrievePolicySuccess;
    try {
      wrap.retrieveProfilePolicies(profileId);
      retrievePolicySuccess = true;
    } catch (DataRepoNotFoundClientException | DataRepoUnauthorizedClientException ex) {
      retrievePolicySuccess = false;
    }
    logger.info("Role {} retievePolicies {}", role, retrievePolicySuccess);

    switch (role) {
      case NONE:
        // all operations should fail
        assertThat(
            "NONE all operations should fail",
            !(foundEnumeration
                || retrieveSuccess
                || datasetSuccess
                || addUserSuccess
                || deleteUserSuccess
                || retrievePolicySuccess));
        break;

      case USER:
        assertThat(
            "USER operations that should succeed",
            (foundEnumeration && retrieveSuccess && datasetSuccess));
        assertThat(
            "USER operations that should fail",
            !(addUserSuccess || deleteUserSuccess || retrievePolicySuccess));
        break;

      case OWNER:
        assertThat(
            "OWNER all operations should succeed",
            (foundEnumeration
                && retrieveSuccess
                && datasetSuccess
                && addUserSuccess
                && deleteUserSuccess
                && retrievePolicySuccess));
        break;
    }
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    // 1. ownerUser1: create profile P1
    // 2. ownerUser2: test all of the operations ownerUser2 should not be able to do
    // 3. ownerUser1: add ownerUser2 as user of P1
    // 4. ownerUser2: test operations again; someone will work
    // 5. ownerUser1: remove ownerUser2 as user of P1
    // 6. ownerUser2: test all of the operations ownerUser2 should not be able to do
    // 7. ownerUser1: add ownerUser2 as owner
    // 8. ownerUser2: test all the operations ownerUser2 should be able to do
    // TODO: two other tests
    //  test owner trade off
    //  test can't delete bill profile if in use

    DataRepoWrap ownerUser1Api = DataRepoWrap.wrapFactory(ownerUser1, server);
    DataRepoWrap ownerUser2Api = DataRepoWrap.wrapFactory(ownerUser2, server);

    try {
      profile = ownerUser1Api.createProfile(billingAccount, "profile_permission_test", true);
      String profileId = profile.getId();
      testOperations(ownerUser1Api, RoleState.OWNER, profileId);

      testOperations(ownerUser2Api, RoleState.NONE, profileId);

      ownerUser1Api.addProfilePolicyMember(ownerUser2.userEmail, profileId, "user");
      testOperations(ownerUser2Api, RoleState.USER, profileId);

      ownerUser1Api.deleteProfilePolicyMember(profileId, "user", ownerUser2.userEmail);
      testOperations(ownerUser2Api, RoleState.NONE, profileId);

      ownerUser1Api.addProfilePolicyMember(ownerUser2.userEmail, profileId, "owner");
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
