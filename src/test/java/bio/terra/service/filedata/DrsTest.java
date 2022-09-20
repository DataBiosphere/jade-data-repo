package bio.terra.service.filedata;

import static bio.terra.service.resourcemanagement.ResourceService.BQ_JOB_USER_ROLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoClient;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessMethod.TypeEnum;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetModel;
import bio.terra.model.FileModel;
import bio.terra.model.SnapshotModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.google.firestore.EncodeFixture;
import com.google.api.services.cloudresourcemanager.model.Binding;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Acl;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

/*
 * WARNING: if making any changes to these tests make sure to notify the #dsp-batch channel! Describe the change and
 * any consequences downstream to DRS clients.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class DrsTest extends UsersBase {

  private static final Logger logger = LoggerFactory.getLogger(DrsTest.class);

  @Autowired private DataRepoClient dataRepoClient;
  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private EncodeFixture encodeFixture;
  @Autowired private AuthService authService;
  @Autowired private IamService iamService;
  @Autowired private ConfigurationService configurationService;
  @Rule @Autowired public TestJobWatcher testWatcher;

  private String custodianToken;
  private DatasetModel datasetModel;
  private SnapshotModel snapshotModel;
  private UUID profileId;
  private UUID datasetId;
  private Map<IamRole, String> datasetIamRoles;
  private Map<IamRole, String> snapshotIamRoles;
  private AuthenticatedUserRequest authenticatedStewardRequest;
  private AuthenticatedUserRequest authenticatedCustodianRequest;

  @Before
  public void setup() throws Exception {
    super.setup();
    custodianToken = authService.getDirectAccessAuthToken(custodian().getEmail());
    String stewardToken = authService.getDirectAccessAuthToken(steward().getEmail());
    EncodeFixture.SetupResult setupResult =
        encodeFixture.setupEncode(steward(), custodian(), reader());
    datasetModel = dataRepoFixtures.getDataset(steward(), setupResult.getDatasetId());
    snapshotModel =
        dataRepoFixtures.getSnapshot(steward(), setupResult.getSummaryModel().getId(), null);
    profileId = setupResult.getProfileId();
    datasetId = setupResult.getDatasetId();
    authenticatedStewardRequest =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DRSIntegration")
            .setEmail(steward().getEmail())
            .setToken(stewardToken)
            .build();
    authenticatedCustodianRequest =
        AuthenticatedUserRequest.builder()
            .setSubjectId("DRSIntegration")
            .setEmail(custodian().getEmail())
            .setToken(custodianToken)
            .build();
    datasetIamRoles =
        iamService.retrievePolicyEmails(
            authenticatedStewardRequest, IamResourceType.DATASET, datasetId);
    snapshotIamRoles =
        iamService.retrievePolicyEmails(
            authenticatedCustodianRequest, IamResourceType.DATASNAPSHOT, snapshotModel.getId());
    logger.info("setup complete");
  }

  @After
  public void teardown() throws Exception {
    try {
      dataRepoFixtures.deleteSnapshotLog(custodian(), snapshotModel.getId());
    } catch (Throwable e) {
      // Already ran if everything was successful so skipping
      logger.info("Snapshot already deleted");
    }
    try {
      dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    } catch (Throwable e) {
      // Already ran if everything was successful so skipping
      logger.info("Dataset already deleted");
    }
    if (profileId != null) {
      dataRepoFixtures.deleteProfileLog(steward(), profileId);
    }
    dataRepoFixtures.resetConfig(steward());
  }

  @Test
  @Ignore
  public void drsHackyTest() throws Exception {
    // Get a DRS ID from the dataset using the custodianToken.
    // Note: the reader does not have permission to run big query jobs anywhere.
    BigQuery bigQueryCustodian =
        BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), custodianToken);
    String drsObjectId =
        BigQueryFixtures.queryForDrsId(bigQueryCustodian, snapshotModel, "file", "file_ref");

    // DRS lookup the file and validate
    logger.info("DRS Object Id - file: {}", drsObjectId);
    final DRSObject drsObjectFile = dataRepoFixtures.drsGetObject(reader(), drsObjectId);
    validateDrsObject(drsObjectFile, drsObjectId);
    assertNull("Contents of file is null", drsObjectFile.getContents());

    TestUtils.validateDrsAccessMethods(
        drsObjectFile.getAccessMethods(),
        authService.getDirectAccessAuthToken(steward().getEmail()));

    Map<String, List<Acl>> preDeleteAcls =
        TestUtils.readDrsGCSAcls(drsObjectFile.getAccessMethods());
    validateContainsAcls(preDeleteAcls.values().iterator().next());
    validateBQJobUserRolePresent(
        datasetModel.getDataProject(),
        Arrays.asList(
            datasetIamRoles.get(IamRole.STEWARD),
            datasetIamRoles.get(IamRole.CUSTODIAN),
            datasetIamRoles.get(IamRole.SNAPSHOT_CREATOR)));
    // Make sure that the snapshot BigQuery Job User permission role is present for the Steward and
    // Reader
    validateBQJobUserRolePresent(
        snapshotModel.getDataProject(),
        Arrays.asList(snapshotIamRoles.get(IamRole.STEWARD), snapshotIamRoles.get(IamRole.READER)));

    Optional<DRSAccessMethod> drsAccessMethod =
        drsObjectFile.getAccessMethods().stream()
            .filter(accessMethod -> accessMethod.getType().equals(TypeEnum.GS))
            .findFirst();

    assertThat("DRS access method is present", drsAccessMethod.isPresent(), equalTo(true));

    String drsAccessId = drsAccessMethod.get().getAccessId();
    DrsResponse<bio.terra.model.DRSAccessURL> drsAccessUrlResponse =
        dataRepoFixtures.getObjectAccessUrl(custodian(), drsObjectId, drsAccessId);

    if (drsAccessUrlResponse.getResponseObject().isEmpty()) {
      Assert.fail("Access URL response object is empty");
    }

    bio.terra.model.DRSAccessURL drsAccessURL = drsAccessUrlResponse.getResponseObject().get();

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpUriRequest request = new HttpHead(drsAccessURL.getUrl());
      try (CloseableHttpResponse response = client.execute(request); ) {
        assertThat(
            "Drs signed URL is accessible",
            response.getStatusLine().getStatusCode(),
            equalTo(HttpStatus.OK.value()));
      }
    }

    // We don't have a DRS URI for a directory, so we back into it by computing the parent path
    // and using the non-DRS interface to get that file. Then we use that to build the
    // DRS object id of the directory. Only then do we get to do a directory lookup.
    assertThat("One alias", drsObjectFile.getAliases().size(), equalTo(1));
    String filePath = drsObjectFile.getAliases().get(0);
    String dirPath = StringUtils.prependIfMissing(getDirectoryPath(filePath), "/");

    FileModel fsObject =
        dataRepoFixtures.getSnapshotFileByName(steward(), snapshotModel.getId(), dirPath);
    String dirObjectId = "v1_" + snapshotModel.getId() + "_" + fsObject.getFileId();

    final DRSObject drsObjectDirectory = dataRepoFixtures.drsGetObject(reader(), dirObjectId);
    logger.info("DRS Object Id - dir: {}", dirObjectId);

    validateDrsObject(drsObjectDirectory, dirObjectId);
    assertNotNull("Contents of directory is not null", drsObjectDirectory.getContents());
    assertNull("Access method of directory is null", drsObjectDirectory.getAccessMethods());

    // When all is done, delete the snapshot and ensure that there are fewer acls
    dataRepoFixtures.deleteSnapshotLog(custodian(), snapshotModel.getId());

    Map<String, List<Acl>> postDeleteAcls =
        TestUtils.readDrsGCSAcls(drsObjectFile.getAccessMethods());
    validateDoesNotContainAcls(postDeleteAcls.values().iterator().next());
    // Make sure that the snapshot BigQuery Job User roles are removed
    validateBQJobUserRoleNotPresent(
        snapshotModel.getDataProject(),
        Arrays.asList(snapshotIamRoles.get(IamRole.STEWARD), snapshotIamRoles.get(IamRole.READER)));
    // ...and that the dataset roles are still present
    validateBQJobUserRolePresent(
        datasetModel.getDataProject(),
        Arrays.asList(
            datasetIamRoles.get(IamRole.STEWARD),
            datasetIamRoles.get(IamRole.CUSTODIAN),
            datasetIamRoles.get(IamRole.SNAPSHOT_CREATOR)));

    // Delete dataset and make sure that project level ACLs are reset
    dataRepoFixtures.deleteDatasetLog(steward(), datasetId);

    // Make sure that the dataset roles are now removed
    validateBQJobUserRoleNotPresent(
        datasetModel.getDataProject(),
        Arrays.asList(
            datasetIamRoles.get(IamRole.STEWARD),
            datasetIamRoles.get(IamRole.CUSTODIAN),
            datasetIamRoles.get(IamRole.SNAPSHOT_CREATOR)));
  }

  @Test
  @Ignore
  public void drsScaleTest() throws Exception {
    String failureMaxValue = "0";
    dataRepoFixtures.resetConfig(steward());

    // Get a DRS ID from the dataset using the custodianToken.
    // Note: the reader does not have permission to run big query jobs anywhere.
    BigQuery bigQueryCustodian =
        BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), custodianToken);
    String drsObjectId =
        BigQueryFixtures.queryForDrsId(bigQueryCustodian, snapshotModel, "file", "file_ref");

    // DRS lookup the file and validate
    logger.info("DRS Object Id - file: {}", drsObjectId);
    DrsResponse<DRSObject> response = dataRepoFixtures.drsGetObjectRaw(reader(), drsObjectId);
    assertThat(
        "object is successfully retrieved", response.getStatusCode(), equalTo(HttpStatus.OK));

    // Now lets cap the number allowed
    bio.terra.model.ConfigModel concurrentConfig =
        configurationService.getConfig(ConfigEnum.DRS_LOOKUP_MAX.name());

    concurrentConfig.setParameter(
        new bio.terra.model.ConfigParameterModel().value(failureMaxValue));
    bio.terra.model.ConfigGroupModel failureConfigGroupModel =
        new bio.terra.model.ConfigGroupModel().label("DRSTest").addGroupItem(concurrentConfig);

    List<bio.terra.model.ConfigModel> failureConfigList =
        dataRepoFixtures.setConfigList(steward(), failureConfigGroupModel).getItems();
    logger.info("Config model : " + failureConfigList.get(0));

    // DRS lookup the file and validate
    logger.info("DRS Object Id - file: {}", drsObjectId);
    DrsResponse<DRSObject> failureResponse =
        dataRepoFixtures.drsGetObjectRaw(reader(), drsObjectId);
    assertThat(
        "object is not successfully retrieved",
        failureResponse.getStatusCode(),
        equalTo(HttpStatus.TOO_MANY_REQUESTS));
  }

  @Test
  @Ignore
  public void testDrsErrorResponses() throws Exception {
    dataRepoFixtures.resetConfig(steward());

    // Get a DRS ID from the dataset using the custodianToken.
    // Note: the reader does not have permission to run big query jobs anywhere.
    BigQuery bigQueryCustodian =
        BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), custodianToken);
    String drsObjectId =
        BigQueryFixtures.queryForDrsId(bigQueryCustodian, snapshotModel, "file", "file_ref");

    String invalidDrsObjectId = drsObjectId.substring(1);

    // DRS lookup the file and validate
    logger.info("Invalid DRS Object Id - file: {}", invalidDrsObjectId);
    DrsResponse<DRSObject> badRequestResponse =
        dataRepoFixtures.drsGetObjectRaw(reader(), invalidDrsObjectId);
    assertThat(
        "a 400 BAD_REQUEST response is returned",
        badRequestResponse.getStatusCode(),
        equalTo(HttpStatus.BAD_REQUEST));

    // We need to return a string here so that the test passes both locally and in kubernetes
    // Locally, we get a json, but in the cloud, we get an HTML response from the proxy
    ResponseEntity<String> unauthorizedRequest =
        dataRepoClient.makeUnauthenticatedDrsRequest(
            "/ga4gh/drs/v1/objects/" + drsObjectId, HttpMethod.GET);
    assertThat(
        "a 401 UNAUTHORIZED response is returned",
        unauthorizedRequest.getStatusCode(),
        equalTo(HttpStatus.UNAUTHORIZED));

    DrsResponse<DRSObject> forbiddenResponse =
        dataRepoFixtures.drsGetObjectRaw(discoverer(), drsObjectId);
    assertThat(
        "a 403 FORBIDDEN response is returned",
        forbiddenResponse.getStatusCode(),
        equalTo(HttpStatus.FORBIDDEN));

    String nonExistentFileDrsObjectId =
        String.format("v1_%s_%s", snapshotModel.getId(), UUID.randomUUID());
    logger.info("Non-existent file DRS Object Id - file: {}", nonExistentFileDrsObjectId);
    DrsResponse<DRSObject> badFileResponse =
        dataRepoFixtures.drsGetObjectRaw(reader(), nonExistentFileDrsObjectId);
    assertThat(
        "a 404 NOT_FOUND response is returned",
        badFileResponse.getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));

    String nonExistentSnapshotObjectId =
        drsObjectId.replace(snapshotModel.getId().toString(), UUID.randomUUID().toString());
    logger.info("Non-existent snapshot DRS Object Id - file: {}", nonExistentSnapshotObjectId);
    DrsResponse<DRSObject> badSnapshotResponse =
        dataRepoFixtures.drsGetObjectRaw(reader(), nonExistentSnapshotObjectId);
    assertThat(
        "a 404 NOT_FOUND response is returned",
        badSnapshotResponse.getStatusCode(),
        equalTo(HttpStatus.NOT_FOUND));
  }

  private void validateDrsObject(DRSObject drsObject, String drsObjectId) {
    logger.info("DrsObject is:" + drsObject);
    assertThat("DRS id matches", drsObject.getId(), equalTo(drsObjectId));
    assertThat(
        "Create and update dates match",
        drsObject.getCreatedTime(),
        equalTo(drsObject.getUpdatedTime()));
    assertThat("DRS version is right", drsObject.getVersion(), equalTo("0"));

    for (DRSChecksum checksum : drsObject.getChecksums()) {
      assertTrue(
          "checksum is md5 or crc32c",
          StringUtils.equals(checksum.getType(), "md5")
              || StringUtils.equals(checksum.getType(), "crc32c"));
    }

    if (drsObject.getAccessMethods() != null) {
      for (DRSAccessMethod method : drsObject.getAccessMethods()) {
        if (method.getType() == DRSAccessMethod.TypeEnum.GS) {
          assertThat(
              "Has proper file name (gs)",
              method.getAccessUrl().getUrl(),
              endsWith(drsObject.getName()));
        } else if (method.getType() == DRSAccessMethod.TypeEnum.HTTPS) {
          try {
            assertThat(
                "Has proper file name (https)",
                new URL(method.getAccessUrl().getUrl()).getPath(),
                endsWith(drsObject.getName()));
          } catch (final MalformedURLException e) {
            throw new RuntimeException("Bad URL in DRS file access", e);
          }
        } else {
          throw new NotImplementedException(
              String.format(
                  "Check for access method %s not implemented", method.getType().toString()));
        }
      }
    }
  }

  private String getDirectoryPath(String path) {
    String[] pathParts = StringUtils.split(path, '/');
    assertThat("Valid directory path", pathParts.length, greaterThan(1));
    int endIndex = pathParts.length - 1;
    return StringUtils.join(pathParts, '/', 0, endIndex);
  }

  /** Given a set of file ACLs, make sure that the expected policy ACLs are present */
  private void validateContainsAcls(List<Acl> acls) {
    final Collection<String> entities =
        CollectionUtils.collect(acls, a -> a.getEntity().toString());
    assertThat(
        "Has steward ACLs",
        entities,
        hasItem(String.format("group-%s", snapshotIamRoles.get(IamRole.STEWARD))));
    assertThat(
        "Has reader ACLs",
        entities,
        hasItem(String.format("group-%s", snapshotIamRoles.get(IamRole.READER))));
  }

  /** Given a set of file ACLs, make sure that the expected policy ACLs are present */
  private void validateDoesNotContainAcls(List<Acl> acls) {
    final Collection<String> entities =
        CollectionUtils.collect(acls, a -> a.getEntity().toString());
    assertThat(
        "Doesn't have custodian ACLs",
        entities,
        not(hasItem(String.format("group-%s", snapshotIamRoles.get(IamRole.CUSTODIAN)))));
    assertThat(
        "Doesn't have reader ACLs",
        entities,
        not(hasItem(String.format("group-%s", snapshotIamRoles.get(IamRole.READER)))));
  }

  /** Verify that the specified member emails all have the BQ job user role in the data project */
  private void validateBQJobUserRolePresent(String dataProject, Collection<String> members)
      throws GeneralSecurityException, IOException {
    List<Binding> bindings = TestUtils.getPolicy(dataProject).getBindings();
    bindings.forEach(
        b -> {
          if (Objects.equals(b.getRole(), BQ_JOB_USER_ROLE)) {
            members.forEach(
                m ->
                    assertThat(
                        "Member has BQ job user role",
                        CollectionUtils.collect(b.getMembers(), e -> e.replaceAll("^group:", "")),
                        hasItem(m)));
          }
        });
  }

  /**
   * Verify that none of the specified member emails have the BQ job user role in the data project
   */
  private void validateBQJobUserRoleNotPresent(String dataProject, Collection<String> members)
      throws GeneralSecurityException, IOException {
    List<Binding> bindings = TestUtils.getPolicy(dataProject).getBindings();
    bindings.forEach(
        b -> {
          if (Objects.equals(b.getRole(), BQ_JOB_USER_ROLE)) {
            members.forEach(
                m ->
                    assertThat(
                        "Member does not have BQ job user role",
                        CollectionUtils.collect(b.getMembers(), e -> e.replaceAll("^group:", "")),
                        not(contains(m))));
          }
        });
  }
}
