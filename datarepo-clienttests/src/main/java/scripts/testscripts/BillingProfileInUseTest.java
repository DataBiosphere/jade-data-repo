package scripts.testscripts;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.SnapshotSummaryModel;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.BillingProfileUsers;
import scripts.utils.tdrwrapper.DataRepoWrap;
import scripts.utils.tdrwrapper.exception.DataRepoBadRequestClientException;

public class BillingProfileInUseTest extends BillingProfileUsers {
  private static final Logger logger = LoggerFactory.getLogger(BillingProfileInUseTest.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public BillingProfileInUseTest() {
    super();
  }

  public void setup(List<TestUserSpecification> testUsers) throws Exception {
    super.setup(testUsers);
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    DataRepoWrap ownerUser1Api = DataRepoWrap.wrapFactory(ownerUser1, server);
    DataRepoWrap ownerUser2Api = DataRepoWrap.wrapFactory(ownerUser2, server);
    DataRepoWrap userUserApi = DataRepoWrap.wrapFactory(userUser, server);

    BillingProfileModel profile = null;
    DatasetSummaryModel dataset = null;
    SnapshotSummaryModel snapshot = null;

    try {
      // owner1 creates the profile and grants ownerUser2 and userUser the "user" role
      profile = ownerUser1Api.createProfile(billingAccount, "profile_permission_test", true);
      String profileId = profile.getId();
      ownerUser1Api.addProfilePolicyMember(profileId, "user", ownerUser2.userEmail);
      ownerUser1Api.addProfilePolicyMember(profileId, "user", userUser.userEmail);

      // owner2 creates a dataset and grants userUser the "custodian" role
      dataset = ownerUser2Api.createDataset(profileId, "dataset-simple.json", true);
      ownerUser2Api.addDatasetPolicyMember(dataset.getId(), "custodian", userUser.userEmail);
      // user creates a snapshot
      snapshot = userUserApi.createSnapshot(dataset.getId(), "snapshot-simple.json", true);

      // attempt to delete profile should fail due to dataset and snapshot dependency
      tryDeleteProfile(ownerUser1Api, profileId, false);

      userUserApi.deleteSnapshot(snapshot.getId());
      snapshot = null;

      // attempt to delete profile should fail due to dataset dependency
      tryDeleteProfile(ownerUser1Api, profileId, false);

      ownerUser2Api.deleteDataset(dataset.getId());
      dataset = null;

      // attempt to delete profile should succeed
      tryDeleteProfile(ownerUser1Api, profileId, true);
      profile = null;
    } catch (Exception e) {
      logger.error("Error in journey", e);
      e.printStackTrace();
      throw e;
    } finally {
      if (snapshot != null) {
        ownerUser2Api.deleteSnapshot(snapshot.getId());
      }
      if (dataset != null) {
        ownerUser2Api.deleteDataset(dataset.getId());
      }
      if (profile != null) {
        ownerUser1Api.deleteProfile(profile.getId());
      }
    }
  }

  private void tryDeleteProfile(DataRepoWrap wrap, String id, boolean expectSuccess)
      throws Exception {
    boolean success;
    try {
      wrap.deleteProfile(id);
      success = true;
    } catch (DataRepoBadRequestClientException ex) {
      success = false;
    }
    assertThat("success meets expectations", success, equalTo(expectSuccess));
  }
}
