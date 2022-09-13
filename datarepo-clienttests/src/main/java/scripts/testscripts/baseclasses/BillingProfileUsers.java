package scripts.testscripts.baseclasses;

import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;
import bio.terra.datarepo.model.PolicyModel;
import bio.terra.datarepo.model.PolicyResponse;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.utils.tdrwrapper.DataRepoWrap;
import scripts.utils.tdrwrapper.exception.DataRepoForbiddenClientException;
import scripts.utils.tdrwrapper.exception.DataRepoNotFoundClientException;

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

  protected enum RoleState {
    NONE,
    USER,
    OWNER;
  }

  // Run is sequence of operations collecting their results. At the end compare the results
  // to what is expected for their role state.
  protected void testOperations(DataRepoWrap wrap, RoleState role, UUID profileId)
      throws Exception {

    // Test for ability to enumerate the profile
    boolean foundEnumeration = false;
    EnumerateBillingProfileModel enumeration = wrap.enumerateProfiles(0, 1000);
    for (BillingProfileModel profile : enumeration.getItems()) {
      if (profileId.equals(profile.getId())) {
        foundEnumeration = true;
      }
    }
    logger.info("Role {} enumeration {}", role, foundEnumeration);

    // Test for ability to retrieve the profile
    boolean retrieveSuccess;
    try {
      wrap.retrieveProfile(profileId);
      retrieveSuccess = true;
    } catch (DataRepoForbiddenClientException ex) {
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
    } catch (DataRepoForbiddenClientException ex) {
      datasetSuccess = false;
    }
    logger.info("Role {} dataset {}", role, datasetSuccess);

    // Test for ability to add a user to the user role of the profile
    boolean addUserSuccess;
    try {
      wrap.addProfilePolicyMember(profileId, "user", userUser.userEmail);
      addUserSuccess = true;
    } catch (DataRepoNotFoundClientException | DataRepoForbiddenClientException ex) {
      addUserSuccess = false;
    }
    logger.info("Role {} addUser {}", role, addUserSuccess);

    // Test for ability to delete a user from the user role of the profile
    boolean deleteUserSuccess = addUserSuccess;
    if (addUserSuccess) {
      try {
        wrap.deleteProfilePolicyMember(profileId, "user", userUser.userEmail);
        deleteUserSuccess = true;
      } catch (DataRepoNotFoundClientException | DataRepoForbiddenClientException ex) {
        deleteUserSuccess = false;
      }
    }
    logger.info("Role {} deleteUser {}", role, deleteUserSuccess);

    // Test for ability to retrieved the policies of a profile
    boolean retrievePolicySuccess;
    try {
      wrap.retrieveProfilePolicies(profileId);
      retrievePolicySuccess = true;
    } catch (DataRepoNotFoundClientException | DataRepoForbiddenClientException ex) {
      retrievePolicySuccess = false;
    }
    logger.info("Role {} retrievePolicies {}", role, retrievePolicySuccess);
    switch (role) {
      case NONE:
        // all operations should fail
        if (foundEnumeration
            || retrieveSuccess
            || datasetSuccess
            || addUserSuccess
            || deleteUserSuccess
            || retrievePolicySuccess) {
          throw new IllegalStateException("NONE all operations should fail; some were successful");
        }
        break;

      case USER:
        if (!(foundEnumeration && retrieveSuccess && datasetSuccess)) {
          throw new IllegalStateException("Some USER operations that should succeed failed");
        }
        if (addUserSuccess || deleteUserSuccess || retrievePolicySuccess) {
          throw new IllegalStateException("some USER operations that should fail succeeded");
        }
        break;

      case OWNER:
        if (!(foundEnumeration
            && retrieveSuccess
            && datasetSuccess
            && addUserSuccess
            && deleteUserSuccess
            && retrievePolicySuccess)) {
          throw new IllegalStateException("OWNER all operations should succeed; some failed");
        }
        break;
    }
  }

  protected void dumpPolicies(DataRepoWrap wrap, UUID profileId) {
    PolicyResponse policies = wrap.retrieveProfilePolicies(profileId);
    for (PolicyModel policy : policies.getPolicies()) {
      logger.info(
          "Policy {} members {}", policy.getName(), StringUtils.join(policy.getMembers(), ", "));
    }
  }
}
