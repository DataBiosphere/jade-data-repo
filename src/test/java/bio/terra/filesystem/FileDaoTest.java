package bio.terra.filesystem;

import bio.terra.category.Connected;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.filesystem.exception.FileSystemObjectDependencyException;
import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import bio.terra.fixtures.Names;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObject;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
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
@ActiveProfiles("google")
@Category(Connected.class)
public class FileDaoTest {
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

    private UUID studyId = UUID.randomUUID();
    private String flightId = UUID.randomUUID().toString();
    private String datasetId = UUID.randomUUID().toString();


    @Autowired
    private FireStoreFileDao fileDao;

    @Autowired
    private FireStoreDependencyDao dependencyDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

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
        FSObject fsObject = new FSObject()
            .studyId(studyId)
            .objectType(FSObject.FSObjectType.INGESTING_FILE)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .flightId(flightId);

        UUID fileAId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileAId);

        FSFileInfo fsFileInfo = new FSFileInfo()
            .objectId(fileAId.toString())
            .studyId(studyId.toString())
            .createdDate(Instant.now().toString())
            .gspath("gs://mybucket/mystudy/myfile")
            .checksumCrc32c("myChecksum")
            .checksumMd5(null)
            .size(42L)
            .flightId(flightId);

        FSObject typeObject = fileDao.createFileComplete(fsFileInfo);
        assertThat("Id matches", typeObject.getObjectId(), equalTo(fileAId));
        assertThat("Type is FILE", typeObject.getObjectType(), equalTo(FSObject.FSObjectType.FILE));

        fileDao.createFileCompleteUndo(studyId.toString(), fileAId.toString());

        typeObject = fileDao.retrieveByIdNoThrow(typeObject.getStudyId(), typeObject.getObjectId());
        assertThat("Type is INGESTING_FILE", typeObject.getObjectType(),
            equalTo(FSObject.FSObjectType.INGESTING_FILE));
    }

    @Test
    public void deleteOnEmptyTest() throws Exception {
        FSObject fsObject = new FSObject()
            .studyId(studyId)
            .objectType(FSObject.FSObjectType.INGESTING_FILE)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .flightId(flightId);

        UUID fileAId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileAId);

        FSObject topObject = getCheckPath(topPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject secondObject = getCheckPath(secondPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject thirdObject = getCheckPath(thirdPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject fileAObject = getCheckPath(fileAPath, studyId, FSObject.FSObjectType.INGESTING_FILE);

        // Try to delete a directory with the deleteFile method; should fail
        try {
            fileDao.createFileStartUndo(studyId.toString(), secondObject.getPath(), flightId);
            fail("Should not have successfully deleted a directory");
        } catch (Exception ex) {
            assertTrue("Expected exception", ex instanceof FileSystemCorruptException);
            assertThat("Check expected error", ex.getMessage(), containsString("bad file object type"));
        }

        FSFileInfo fsFileInfo = makeFsFileInfo(fileAId.toString());
        fileDao.createFileComplete(fsFileInfo);

        boolean exists = fileDao.deleteFileStart(studyId.toString(), fileAId.toString(), flightId);
        assertTrue("File exists", exists);

        try {
            fileDao.deleteFileComplete(studyId.toString(), fileAId.toString(), "badFlightId");
        } catch (Exception ex) {
            assertTrue("Expected delete exception", ex instanceof InvalidFileSystemObjectTypeException);
            assertThat("Delete reason", ex.getMessage(), containsString("being deleted by someone else"));
        }

        try {
            fileDao.deleteFileComplete(studyId.toString(), secondObject.getObjectId().toString(), flightId);
        } catch (Exception ex) {
            assertTrue("Expected delete exception", ex instanceof InvalidFileSystemObjectTypeException);
            assertThat("Delete reason", ex.getMessage(), containsString("attempt to delete a directory"));
        }

        exists = fileDao.deleteFileComplete(studyId.toString(), fileAId.toString(), flightId);
        assertTrue("File existed", exists);

        // Now verify that the objects are gone.
        checkObjectGone(topObject);
        checkObjectGone(secondObject);
        checkObjectGone(thirdObject);
        checkObjectGone(fileAObject);
    }

    @Test
    public void dontDeleteOnNotEmptyTest() throws Exception {
        FSObject fsObject = new FSObject()
            .studyId(studyId)
            .objectType(FSObject.FSObjectType.INGESTING_FILE)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .flightId(flightId);

        UUID fileAId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileAId);

        FSObject topObject = getCheckPath(topPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject secondObject = getCheckPath(secondPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject thirdObject = getCheckPath(thirdPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject fileAObject = getCheckPath(fileAPath, studyId, FSObject.FSObjectType.INGESTING_FILE);

        fsObject.path(fileBPath);
        UUID fileBId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileBId);

        FSObject fileBObject = getCheckPath(fileBPath, studyId, FSObject.FSObjectType.INGESTING_FILE);

        FSFileInfo fsFileInfo = makeFsFileInfo(fileAId.toString());
        fileDao.createFileComplete(fsFileInfo);
        fsFileInfo = makeFsFileInfo(fileBId.toString());
        fileDao.createFileComplete(fsFileInfo);


        boolean existed = fileDao.deleteFileStart(studyId.toString(), fileAId.toString(), flightId);
        assertTrue("File existed", existed);
        existed = fileDao.deleteFileComplete(studyId.toString(), fileAId.toString(), flightId);
        assertTrue("File existed", existed);

        // Directories and file B should still all be in place
        checkObjectPresent(topObject);
        checkObjectPresent(secondObject);
        checkObjectPresent(thirdObject);
        checkObjectPresent(fileBObject);
        checkObjectGone(fileAObject);

        // Don't delete file with dependencies
        addDatasetDependency(fileBId);

        try {
            fileDao.deleteFileStart(studyId.toString(), fileBId.toString(), flightId);
            fail("Should not have successfully deleted");
        } catch (Exception ex) {
            assertTrue("Correct dependency exception", ex instanceof FileSystemObjectDependencyException);
            assertThat("Correct message", ex.getMessage(), containsString("dataset"));
        }

        removeDatasetDependency(fileBId);

        fileDao.deleteFileStart(studyId.toString(), fileBId.toString(), flightId);

        addDatasetDependency(fileBId);

        try {
            fileDao.deleteFileComplete(studyId.toString(), fileBId.toString(), flightId);
            fail("Should not have successfully deleted");
        } catch (Exception ex) {
            assertTrue("Correct dependency exception", ex instanceof FileSystemObjectDependencyException);
            assertThat("Correct message", ex.getMessage(), containsString("dataset"));
        }

        removeDatasetDependency(fileBId);

        existed = fileDao.deleteFileComplete(studyId.toString(), fileBId.toString(), flightId);
        assertTrue("File B existed", existed);
    }

    private void addDatasetDependency(UUID objectId) {
        dependencyDao.storeDatasetFileDependency(studyId.toString(), datasetId, objectId.toString());
    }

    private void removeDatasetDependency(UUID objectId) {
        dependencyDao.removeDatasetFileDependency(studyId.toString(), datasetId, objectId.toString());
    }

    private void checkObjectPresent(FSObject fsObject) {
        FSObject thereObject = fileDao.retrieveByPathNoThrow(fsObject.getStudyId(), fsObject.getPath());
        assertNotNull("Object is there by path", thereObject);
        thereObject = fileDao.retrieveByIdNoThrow(fsObject.getStudyId(), fsObject.getObjectId());
        assertNotNull("Object is there by id", thereObject);
    }


    private void checkObjectGone(FSObject fsObject) {
        FSObject goneObject = fileDao.retrieveByPathNoThrow(fsObject.getStudyId(), fsObject.getPath());
        assertNull("Object is gone by path", goneObject);
        goneObject = fileDao.retrieveByIdNoThrow(fsObject.getStudyId(), fsObject.getObjectId());
        assertNull("Object is gone by id", goneObject);
    }

    private FSObject getCheckPath(String path, UUID studyId, FSObject.FSObjectType objectType) {
        FSObject fsObject = fileDao.retrieveByPathNoThrow(studyId, path);
        assertNotNull("Object not null", fsObject);
        assertThat("Object has correct path", fsObject.getPath(), equalTo(path));
        assertThat("Correct study", fsObject.getStudyId(), equalTo(studyId));
        assertThat("Correct object type", fsObject.getObjectType(), equalTo(objectType));
        if (objectType != FSObject.FSObjectType.DIRECTORY) { // Only filled in for files
            assertThat("Correct mime type", mimeType, equalTo(fsObject.getMimeType()));
            assertThat("Correct description", description, equalTo(fsObject.getDescription()));
        }
        return fsObject;
    }

    private FSFileInfo makeFsFileInfo(String objectId) {
        return new FSFileInfo()
            .objectId(objectId)
            .studyId(studyId.toString())
            .createdDate(Instant.now().toString())
            .gspath("gs://mybucket/mystudy/myfile")
            .checksumCrc32c("myChecksum")
            .checksumMd5(null)
            .size(42L)
            .flightId(flightId);
    }

}
