package bio.terra.service.filedata;

import bio.terra.common.TestUtils;
import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.integration.BigQueryFixtures;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSObject;
import bio.terra.model.FileModel;
import bio.terra.model.SnapshotModel;
import bio.terra.service.filedata.google.firestore.EncodeFixture;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamRole;
import bio.terra.service.iam.IamService;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Acl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class DrsTest extends UsersBase {
    private static final Logger logger = LoggerFactory.getLogger(DrsTest.class);

    @Autowired private DataRepoFixtures dataRepoFixtures;
    @Autowired private EncodeFixture encodeFixture;
    @Autowired private AuthService authService;
    @Autowired private IamService iamService;

    private String custodianToken;
    private SnapshotModel snapshotModel;
    private String datasetId;
    private Map<IamRole, String> iamRoles;

    @Before
    public void setup() throws Exception {
        super.setup();
        custodianToken = authService.getDirectAccessAuthToken(custodian().getEmail());
        EncodeFixture.SetupResult setupResult = encodeFixture.setupEncode(steward(), custodian(), reader());
        snapshotModel = dataRepoFixtures.getSnapshot(custodian(), setupResult.getSummaryModel().getId());
        datasetId = setupResult.getDatasetId();
        AuthenticatedUserRequest authenticatedCustodianRequest =
            new AuthenticatedUserRequest().email(custodian().getEmail()).token(Optional.of(custodianToken));
        iamRoles = iamService.retrievePolicyEmails(authenticatedCustodianRequest,
            IamResourceType.DATASNAPSHOT, UUID.fromString(snapshotModel.getId()));
        logger.info("IAM: {}", iamRoles);
        logger.info("setup complete");
    }

    @After
    public void teardown() throws Exception {
        try {
            dataRepoFixtures.deleteSnapshotLog(custodian(), snapshotModel.getId());
        } catch (Throwable e) {
            // Already ran if everything was successful so skipping
        }
        dataRepoFixtures.deleteDatasetLog(steward(), datasetId);
    }

    @Test
    public void drsHackyTest() throws Exception {
        // Get a DRS ID from the dataset using the custodianToken.
        // Note: the reader does not have permission to run big query jobs anywhere.
        BigQuery bigQueryCustodian = BigQueryFixtures.getBigQuery(snapshotModel.getDataProject(), custodianToken);
        String drsObjectId = BigQueryFixtures.queryForDrsId(bigQueryCustodian,
            snapshotModel,
            "file",
            "file_ref");

        // DRS lookup the file and validate
        logger.info("DRS Object Id - file: {}", drsObjectId);
        final DRSObject drsObjectFile = dataRepoFixtures.drsGetObject(reader(), drsObjectId);
        validateDrsObject(drsObjectFile, drsObjectId);
        assertNull("Contents of file is null", drsObjectFile.getContents());
        TestUtils.validateDrsAccessMethods(drsObjectFile.getAccessMethods());
        Map<String, List<Acl>> preDeleteAcls = TestUtils.readDrsGCSAcls(drsObjectFile.getAccessMethods());
        validateContainsAcls(preDeleteAcls.values().iterator().next());

        // We don't have a DRS URI for a directory, so we back into it by computing the parent path
        // and using the non-DRS interface to get that file. Then we use that to build the
        // DRS object id of the directory. Only then do we get to do a directory lookup.
        assertThat("One alias", drsObjectFile.getAliases().size(), equalTo(1));
        String filePath = drsObjectFile.getAliases().get(0);
        String dirPath = StringUtils.prependIfMissing(getDirectoryPath(filePath), "/");

        FileModel fsObject = dataRepoFixtures.getSnapshotFileByName(steward(), snapshotModel.getId(), dirPath);
        String dirObjectId = "v1_" + snapshotModel.getId() + "_" + fsObject.getFileId();

        final DRSObject drsObjectDirectory = dataRepoFixtures.drsGetObject(reader(), dirObjectId);
        logger.info("DRS Object Id - dir: {}", dirObjectId);

        validateDrsObject(drsObjectDirectory, dirObjectId);
        assertNotNull("Contents of directory is not null", drsObjectDirectory.getContents());
        assertNull("Access method of directory is null", drsObjectDirectory.getAccessMethods());

        // When all is done, delete the snapshot and ensure that there are fewer acls
        dataRepoFixtures.deleteSnapshotLog(custodian(), snapshotModel.getId());


        Map<String, List<Acl>> postDeleteAcls = TestUtils.readDrsGCSAcls(drsObjectFile.getAccessMethods());
        validateDoesNotContainAcls(postDeleteAcls.values().iterator().next());
    }

    private void validateDrsObject(DRSObject drsObject, String drsObjectId) {
        logger.info("DrsObject is:" + drsObject);
        assertThat("DRS id matches", drsObject.getId(), equalTo(drsObjectId));
        assertThat("Create and update dates match", drsObject.getCreatedTime(), equalTo(drsObject.getUpdatedTime()));
        assertThat("DRS version is right", drsObject.getVersion(), equalTo("0"));

        for (DRSChecksum checksum : drsObject.getChecksums()) {
            assertTrue("checksum is md5 or crc32c",
                StringUtils.equals(checksum.getType(), "md5") ||
                    StringUtils.equals(checksum.getType(), "crc32c"));
        }

        if (drsObject.getAccessMethods() != null) {
            for (DRSAccessMethod method: drsObject.getAccessMethods()) {
                if (method.getType() == DRSAccessMethod.TypeEnum.GS) {
                    assertThat(
                        "Has proper file name (gs)",
                        method.getAccessUrl().getUrl(),
                        endsWith(drsObject.getName())
                    );
                } else if (method.getType() == DRSAccessMethod.TypeEnum.HTTPS) {
                    try {
                        assertThat(
                            "Has proper file name (https)",
                            new URL(method.getAccessUrl().getUrl()).getPath(),
                            endsWith(drsObject.getName())
                        );
                    } catch (final MalformedURLException e) {
                        throw new RuntimeException("Bad URL in DRS file access", e);
                    }
                } else {
                    throw new NotImplementedException(
                        String.format("Check for access method %s not implemented", method.getType().toString())
                    );
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

    /**
     * Given a set of file ACLs, make sure that the expected policy ACLs are present
     */
    private void validateContainsAcls(List<Acl> acls) {
        final Collection<String> entities = CollectionUtils.collect(acls, a -> a.getEntity().toString());
        logger.info("RAW: {}", acls);
        logger.info("EMAILS: {}", entities);
        assertThat("Has custodian ACLs", entities, hasItem(String.format("group-%s",
            iamRoles.get(IamRole.CUSTODIAN))));
        assertThat("Has steward ACLs", entities, hasItem(String.format("group-%s",
            iamRoles.get(IamRole.STEWARD))));
        assertThat("Has reader ACLs", entities, hasItem(String.format("group-%s",
            iamRoles.get(IamRole.READER))));
    }

    /**
     * Given a set of file ACLs, make sure that the expected policy ACLs are present
     */
    private void validateDoesNotContainAcls(List<Acl> acls) {
        final Collection<String> entities = CollectionUtils.collect(acls, a -> a.getEntity().toString());
        assertThat("Doesn't have custodian ACLs", entities, not(hasItem(String.format("group-%s",
            iamRoles.get(IamRole.CUSTODIAN)))));
        assertThat("Doesn't have steward ACLs", entities, not(hasItem(String.format("group-%s",
            iamRoles.get(IamRole.STEWARD)))));
        assertThat("Doesn't have reader ACLs", entities, not(hasItem(String.format("group-%s",
            iamRoles.get(IamRole.READER)))));
    }


}
