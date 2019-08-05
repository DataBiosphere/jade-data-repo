package bio.terra.filesystem;

import bio.terra.category.Connected;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.fixtures.ConnectedOperations;
import bio.terra.fixtures.Names;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.metadata.Dataset;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.resourcemanagement.service.google.GoogleResourceConfiguration;
import bio.terra.service.SamClientService;
import bio.terra.service.DatasetService;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class FileDaoTest {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FileDaoTest");

    private static final String mimeType = "application/octet-string";
    private static final String description = "dummy description";

    private String topDir = Names.randomizeName("top");
    private String secondDir = Names.randomizeName("second");
    private String thirdDir = Names.randomizeName("third");
    private String fileA = Names.randomizeName("fileA_") + ".ext";
    private String fileB = Names.randomizeName("fileB_") + ".ext";

    private String topPath = "/" + topDir;
    private String secondPath = topPath + "/" + secondDir;
    private String thirdPath = secondPath + "/" + thirdDir;
    private String fileAPath = thirdPath + "/" + fileA;
    private String fileBPath = thirdPath + "/" + fileB;

    private UUID datasetId;
    private String flightId = UUID.randomUUID().toString();
    private String snapshotId = UUID.randomUUID().toString();
    private DatasetSummaryModel datasetSummaryModel;
    private BillingProfileModel billingProfileModel;
    private Dataset dataset;

    @Autowired
    private FireStoreFileDao fileDao;

    @Autowired
    private FireStoreDependencyDao dependencyDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ConnectedOperations connectedOperations;

    @Autowired
    private GoogleResourceConfiguration googleResourceConfiguration;

    @Autowired
    private DatasetService datasetService;

    @MockBean
    private SamClientService samService;

    @Before
    public void setup() throws Exception {
        billingProfileModel = connectedOperations.getOrCreateProfileForAccount(
            googleResourceConfiguration.getCoreBillingAccount());

        datasetSummaryModel = connectedOperations.createDatasetWithFlight(
            billingProfileModel,
            "ingest-test-dataset.json");
        datasetId = UUID.fromString(datasetSummaryModel.getId());
        dataset = datasetService.retrieve(datasetId);
        connectedOperations.stubOutSamCalls(samService);
    }
    @Test
    public void pathMiscTest() throws Exception {
        String result = fileDao.getDirectoryPath("/foo/bar/fribble");
        assertThat("Valid path", result, equalTo("/foo/bar"));

        result = fileDao.getDirectoryPath("/foo/bar");
        assertThat("Valid path", result, equalTo("/foo"));

        result = fileDao.getDirectoryPath("/foo");
        assertThat("Should be empty", result, equalTo(StringUtils.EMPTY));
    }

    @Test
    public void fileStateTest() throws Exception {
        FSFile fsFile = new FSFile()
            .mimeType(mimeType)
            .flightId(flightId)
            .datasetId(datasetId)
            .objectType(FSObjectType.INGESTING_FILE)
            .path(fileAPath)
            .description(description);

        Dataset dataset = datasetService.retrieve(datasetId);
        UUID fileAId = fileDao.createFileStart(dataset, fsFile);
        assertNotNull("File id not null", fileAId);

        FSFileInfo fsFileInfo = new FSFileInfo()
            .objectId(fileAId.toString())
            .datasetId(datasetId.toString())
            .createdDate(Instant.now().toString())
            .gspath("gs://mybucket/mydataset/myfile")
            .checksumCrc32c("myChecksum")
            .checksumMd5(null)
            .size(42L)
            .flightId(flightId);

        FSObjectBase typeObject = fileDao.createFileComplete(dataset, fsFileInfo);
        assertThat("Id matches", typeObject.getObjectId(), equalTo(fileAId));
        assertThat("Type is FILE", typeObject.getObjectType(), equalTo(FSObjectType.FILE));

        fileDao.createFileCompleteUndo(dataset, fileAId.toString());

        typeObject = fileDao.retrieveByIdNoThrow(dataset, typeObject.getObjectId());
        assertThat("Type is INGESTING_FILE", typeObject.getObjectType(),
            equalTo(FSObjectType.INGESTING_FILE));

        fileDao.createFileComplete(dataset, fsFileInfo);

        boolean exists = fileDao.deleteFileStart(dataset, fileAId.toString(), flightId);
        assertTrue("File exists - start", exists);
        exists = fileDao.deleteFileComplete(dataset, fileAId.toString(), flightId);
        assertTrue("File exists - complete", exists);
    }

    @Test
    public void deleteOnEmptyTest() throws Exception {
        FSFile fsObject = new FSFile()
            .datasetId(datasetId)
            .objectType(FSObjectType.INGESTING_FILE)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .flightId(flightId);

        UUID fileAId = fileDao.createFileStart(dataset, fsObject);
        assertNotNull("File id not null", fileAId);

        FSObjectBase topObject = getCheckPath(topPath, datasetId, FSObjectType.DIRECTORY);
        FSObjectBase secondObject = getCheckPath(secondPath, datasetId, FSObjectType.DIRECTORY);
        FSObjectBase thirdObject = getCheckPath(thirdPath, datasetId, FSObjectType.DIRECTORY);
        FSObjectBase fileAObject = getCheckPath(fileAPath, datasetId, FSObjectType.INGESTING_FILE);

        // Try to delete a directory with the deleteFile method; should fail
        try {
            fileDao.createFileStartUndo(dataset, secondObject.getPath(), flightId);
            fail("Should not have successfully deleted a directory");
        } catch (Exception ex) {
            assertTrue("Expected exception", ex instanceof FileSystemCorruptException);
            assertThat("Check expected error", ex.getMessage(), containsString("bad file object type"));
        }

        FSFileInfo fsFileInfo = makeFsFileInfo(fileAId.toString());
        fileDao.createFileComplete(dataset, fsFileInfo);

        boolean exists = fileDao.deleteFileStart(dataset, fileAId.toString(), flightId);
        assertTrue("File exists", exists);

        try {
            fileDao.deleteFileComplete(dataset, fileAId.toString(), "badFlightId");
        } catch (Exception ex) {
            assertTrue("Expected delete exception", ex instanceof InvalidFileSystemObjectTypeException);
            assertThat("Delete reason", ex.getMessage(), containsString("being deleted by someone else"));
        }

        try {
            fileDao.deleteFileComplete(dataset, secondObject.getObjectId().toString(), flightId);
        } catch (Exception ex) {
            assertTrue("Expected delete exception", ex instanceof InvalidFileSystemObjectTypeException);
            assertThat("Delete reason", ex.getMessage(), containsString("attempt to delete a directory"));
        }

        exists = fileDao.deleteFileComplete(dataset, fileAId.toString(), flightId);
        assertTrue("File existed", exists);

        // Now verify that the objects are gone.
        checkObjectGone(topObject);
        checkObjectGone(secondObject);
        checkObjectGone(thirdObject);
        checkObjectGone(fileAObject);
    }

    @Test
    public void dontDeleteOnNotEmptyTest() throws Exception {
        FSFile fsObject = new FSFile()
            .datasetId(datasetId)
            .objectType(FSObjectType.INGESTING_FILE)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .flightId(flightId);

        UUID fileAId = fileDao.createFileStart(dataset, fsObject);
        assertNotNull("File id not null", fileAId);

        FSObjectBase topObject = getCheckPath(topPath, datasetId, FSObjectType.DIRECTORY);
        FSObjectBase secondObject = getCheckPath(secondPath, datasetId, FSObjectType.DIRECTORY);
        FSObjectBase thirdObject = getCheckPath(thirdPath, datasetId, FSObjectType.DIRECTORY);
        FSObjectBase fileAObject = getCheckPath(fileAPath, datasetId, FSObjectType.INGESTING_FILE);

        fsObject.path(fileBPath);
        UUID fileBId = fileDao.createFileStart(dataset, fsObject);
        assertNotNull("File id not null", fileBId);

        FSObjectBase fileBObject = getCheckPath(fileBPath, datasetId, FSObjectType.INGESTING_FILE);

        FSFileInfo fsFileInfo = makeFsFileInfo(fileAId.toString());
        fileDao.createFileComplete(dataset, fsFileInfo);
        fsFileInfo = makeFsFileInfo(fileBId.toString());
        fileDao.createFileComplete(dataset, fsFileInfo);


        boolean existed = fileDao.deleteFileStart(dataset, fileAId.toString(), flightId);
        assertTrue("File existed", existed);
        existed = fileDao.deleteFileComplete(dataset, fileAId.toString(), flightId);
        assertTrue("File existed", existed);

        // Directories and file B should still all be in place
        checkObjectPresent(topObject);
        checkObjectPresent(secondObject);
        checkObjectPresent(thirdObject);
        checkObjectPresent(fileBObject);
        checkObjectGone(fileAObject);

        // Don't delete file with dependencies
        addSnapshotDependency(fileBId);
        addSnapshotDependency(fileBId);
        addSnapshotDependency(fileBId);

        try {
            fileDao.deleteFileStart(dataset, fileBId.toString(), flightId);
            fail("Should not have successfully deleted");
        } catch (Exception ex) {
            assertTrue("Correct dependency exception", ex instanceof FileSystemObjectDependencyException);
            assertThat("Correct message", ex.getMessage(), containsString("snapshot"));
        }

        removeSnapshotDependency(fileBId);
        removeSnapshotDependency(fileBId);

        try {
            fileDao.deleteFileStart(dataset, fileBId.toString(), flightId);
            fail("Should not have successfully deleted");
        } catch (Exception ex) {
            assertTrue("Correct dependency exception", ex instanceof FileSystemObjectDependencyException);
            assertThat("Correct message", ex.getMessage(), containsString("snapshot"));
        }

        removeSnapshotDependency(fileBId);

        fileDao.deleteFileStart(dataset, fileBId.toString(), flightId);

        addSnapshotDependency(fileBId);

        try {
            fileDao.deleteFileComplete(dataset, fileBId.toString(), flightId);
            fail("Should not have successfully deleted");
        } catch (Exception ex) {
            assertTrue("Correct dependency exception", ex instanceof FileSystemCorruptException);
            assertThat("Correct message", ex.getMessage(), containsString("any references"));
        }

        removeSnapshotDependency(fileBId);

        existed = fileDao.deleteFileComplete(dataset, fileBId.toString(), flightId);
        assertTrue("File B existed", existed);

        // Now verify that the objects are gone.
        checkObjectGone(topObject);
        checkObjectGone(secondObject);
        checkObjectGone(thirdObject);
        checkObjectGone(fileAObject);
        checkObjectGone(fileBObject);
    }

    // Note: this test does logging because it runs for several minutes. Travis gets impatient if it hasn't seen
    // output for 10 minutes. Sometimes if things are slow, this test can take longer than that.
    @Test
    public void testDatasetDelete() throws Exception {
        // Make 1001 files and then delete the dataset
        FSFile fsObject = new FSFile()
            .datasetId(datasetId)
            .objectType(FSObjectType.INGESTING_FILE)
            .mimeType(mimeType)
            .description(description)
            .flightId(flightId);

        FSFileInfo fsFileInfo = makeFsFileInfo("later");

        logger.info("testDatasetDelete:Creating:");
        for (int i = 1; i <= 1001; i++) {
            fsObject.path("/file_" + i);
            UUID objectId = fileDao.createFileStart(dataset, fsObject);
            fsFileInfo.objectId(objectId.toString());
            fileDao.createFileComplete(dataset, fsFileInfo);
            if (i % 100 == 0) {
                logger.info(".." + i);
            }
        }

        // Make sure some of the files are there.
        logger.info("testDatasetDelete:Reading:");
        for (int i = 1; i <= 1001; i++) {
            FSObjectBase testObject = fileDao.retrieveByPathNoThrow(dataset, "/file_" + i);
            assertNotNull(testObject);
            if (i % 100 == 0) {
                logger.info(".." + i);
            }
        }

        logger.info("testDatasetDelete:Deleting...");
        fileDao.deleteFilesFromDataset(dataset);

        // Make sure they are all gone
        logger.info("testDatasetDelete:Check deleted:");
        for (int i = 1; i <= 1001; i++) {
            FSObjectBase noObject = fileDao.retrieveByPathNoThrow(dataset, "/file_" + i);
            assertNull("Object is deleted", noObject);
            if (i % 100 == 0) {
                logger.info(".." + i);
            }
        }
        logger.info("testDatasetDelete:Done");
    }

    @Test
    public void pathLookupTest() throws Exception {
        FSFile fsObject = new FSFile()
            .datasetId(datasetId)
            .objectType(FSObjectType.INGESTING_FILE)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .flightId(flightId);

        UUID fileAId = fileDao.createFileStart(dataset, fsObject);
        FSFileInfo fsFileInfo = makeFsFileInfo(fileAId.toString());
        fileDao.createFileComplete(dataset, fsFileInfo);

        FSObjectBase fileAObject = fileDao.retrieve(dataset, fileAId);
        FSObjectBase testObject = fileDao.retrieveByPath(dataset, fileAPath);
        assertThat("Path lookup matched fileid lookup", fileAObject, equalTo(testObject));

        try {
            fileDao.retrieveByPath(dataset, thirdPath + "/not-there");
            fail("Should not have successfully retrieved");
        } catch (Exception ex) {
            assertTrue("Correct path not found exception", ex instanceof FileSystemObjectNotFoundException);
            assertThat("Correct message", ex.getMessage(), containsString("Object not found"));
        }

        FSObjectBase dirObject = fileDao.retrieveWithContentsByPath(dataset, "/");
        assertThat("Root is directory", dirObject.getObjectType(), equalTo(FSObjectType.DIRECTORY));
        FSDir enumDir = (FSDir)dirObject;
        assertThat("Root has contents", enumDir.getContents().size(), equalTo(1));
        FSObjectBase nextDirObject = enumDir.getContents().get(0);
        assertThat("Top is directory", nextDirObject.getObjectType(), equalTo(FSObjectType.DIRECTORY));

        String path = nextDirObject.getPath(); // /top
        dirObject = fileDao.retrieveWithContentsByPath(dataset, path);
        assertThat("Top is directory", dirObject.getObjectType(), equalTo(FSObjectType.DIRECTORY));
        enumDir = (FSDir)dirObject;
        assertThat("Top has contents", enumDir.getContents().size(), equalTo(1));
        nextDirObject = enumDir.getContents().get(0);
        assertThat("Second is directory", nextDirObject.getObjectType(), equalTo(FSObjectType.DIRECTORY));

        path = nextDirObject.getPath(); // /top/second
        dirObject = fileDao.retrieveWithContentsByPath(dataset, path);
        assertThat("Second is directory", dirObject.getObjectType(), equalTo(FSObjectType.DIRECTORY));
        enumDir = (FSDir)dirObject;
        assertThat("Second has contents", enumDir.getContents().size(), equalTo(1));
        nextDirObject = enumDir.getContents().get(0);
        assertThat("Third is a directory", nextDirObject.getObjectType(), equalTo(FSObjectType.DIRECTORY));

        path = nextDirObject.getPath(); // /top/second/third
        dirObject = fileDao.retrieveWithContentsByPath(dataset, path);
        assertThat("Third is directory", dirObject.getObjectType(), equalTo(FSObjectType.DIRECTORY));
        enumDir = (FSDir)dirObject;
        assertThat("Third has contents", enumDir.getContents().size(), equalTo(1));
        nextDirObject = enumDir.getContents().get(0);
        assertThat("Fourth is a file", nextDirObject.getObjectType(), equalTo(FSObjectType.FILE));

        boolean existed = fileDao.deleteFileStart(dataset, fileAId.toString(), flightId);
        assertTrue("File existed", existed);
        existed = fileDao.deleteFileComplete(dataset, fileAId.toString(), flightId);
        assertTrue("File existed", existed);
    }


    private void addSnapshotDependency(UUID objectId) {
        dependencyDao.storeSnapshotFileDependency(dataset, snapshotId, objectId.toString());
    }

    private void removeSnapshotDependency(UUID objectId) {
        dependencyDao.removeSnapshotFileDependency(dataset, snapshotId, objectId.toString());
    }

    private void checkObjectPresent(FSObjectBase fsObject) {
        FSObjectBase thereObject = fileDao.retrieveByPathNoThrow(dataset, fsObject.getPath());
        assertNotNull("Object is there by path", thereObject);
        thereObject = fileDao.retrieveByIdNoThrow(dataset, fsObject.getObjectId());
        assertNotNull("Object is there by id", thereObject);
    }


    private void checkObjectGone(FSObjectBase fsObject) {
        FSObjectBase goneObject = fileDao.retrieveByPathNoThrow(dataset, fsObject.getPath());
        assertNull("Object is gone by path", goneObject);
        goneObject = fileDao.retrieveByIdNoThrow(dataset, fsObject.getObjectId());
        assertNull("Object is gone by id", goneObject);
    }

    private FSObjectBase getCheckPath(String path, UUID datasetId, FSObjectType objectType) {
        FSObjectBase fsObject = fileDao.retrieveByPathNoThrow(dataset, path);
        assertNotNull("Object not null", fsObject);
        assertThat("Object has correct path", fsObject.getPath(), equalTo(path));
        assertThat("Correct dataset", fsObject.getDatasetId(), equalTo(datasetId));
        assertThat("Correct object type", fsObject.getObjectType(), equalTo(objectType));
        if (objectType != FSObjectType.DIRECTORY) { // Only filled in for files
            FSFile fsFile = (FSFile)fsObject;
            assertThat("Correct mime type", mimeType, equalTo(fsFile.getMimeType()));
            assertThat("Correct description", description, equalTo(fsFile.getDescription()));
        }
        return fsObject;
    }

    private FSFileInfo makeFsFileInfo(String objectId) {
        return new FSFileInfo()
            .objectId(objectId)
            .datasetId(datasetId.toString())
            .createdDate(Instant.now().toString())
            .gspath("gs://mybucket/mydataset/myfile")
            .checksumCrc32c("myChecksum")
            .checksumMd5(null)
            .size(42L)
            .flightId(flightId);
    }

}
