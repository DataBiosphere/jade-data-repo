package bio.terra.dao;

import bio.terra.category.Unit;
import bio.terra.dao.exception.FileSystemObjectDependencyException;
import bio.terra.dao.exception.InvalidFileSystemObjectTypeException;
import bio.terra.fixtures.Names;
import bio.terra.metadata.FSObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

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
@Category(Unit.class)
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


    @Autowired
    private FileDao fileDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Test
    public void pathMiscTest() throws Exception {
        String result = fileDao.getContainingDirectoryPath("/foo/bar/fribble");
        assertThat("Valid path", result, equalTo("/foo/bar"));

        result = fileDao.getContainingDirectoryPath("/foo/bar");
        assertThat("Valid path", result, equalTo("/foo"));

        result = fileDao.getContainingDirectoryPath("/foo");
        assertThat("Should be null", result, equalTo(null));
    }

    @Test
    public void fileStateTest() throws Exception {
        FSObject fsObject = new FSObject()
            .studyId(studyId)
            .objectType(FSObject.FSObjectType.FILE_NOT_PRESENT)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .creatingFlightId(flightId);

        UUID fileAId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileAId);

        // Dummy up the outputs of the primary data step
        fsObject
            .objectId(fileAId)
            .checksum("myChecksum")
            .gspath("fs://mybucket/mystudy/myfile")
            .size(42L);

        UUID completeId = fileDao.createFileComplete(fsObject);
        assertThat("Id matches", completeId, equalTo(fileAId));

        FSObject typeObject = fileDao.retrieveFileByIdNoThrow(completeId);
        assertThat("Type is FILE", typeObject.getObjectType(), equalTo(FSObject.FSObjectType.FILE));

        fileDao.createFileCompleteUndo(typeObject);

        typeObject = fileDao.retrieveFileByIdNoThrow(completeId);
        assertThat("Type is FILE_NOT_PRESENT", typeObject.getObjectType(),
            equalTo(FSObject.FSObjectType.FILE_NOT_PRESENT));
    }

    @Test
    public void deleteOnEmptyTest() throws Exception {
        FSObject fsObject = new FSObject()
            .studyId(studyId)
            .objectType(FSObject.FSObjectType.FILE_NOT_PRESENT)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .creatingFlightId(flightId);

        UUID fileAId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileAId);

        FSObject topObject = getCheckPath(topPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject secondObject = getCheckPath(secondPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject thirdObject = getCheckPath(thirdPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject fileAObject = getCheckPath(fileAPath, studyId, FSObject.FSObjectType.FILE_NOT_PRESENT);

        // Try to delete a directory with the deleteFile method; should fail
        try {
            fileDao.deleteFileForUndo(secondObject.getObjectId(), flightId);
            fail("Should not have successfully deleted a directory");
        } catch (Exception ex) {
            assertTrue("Expected exception", ex instanceof InvalidFileSystemObjectTypeException);
            assertThat("Check expected error", ex.getMessage(), containsString("directory"));
        }

        boolean existed = fileDao.deleteFileForUndo(fileAObject.getObjectId(), flightId);
        assertTrue("File existed", existed);

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
            .objectType(FSObject.FSObjectType.FILE_NOT_PRESENT)
            .path(fileAPath)
            .mimeType(mimeType)
            .description(description)
            .creatingFlightId(flightId);

        UUID fileAId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileAId);

        FSObject topObject = getCheckPath(topPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject secondObject = getCheckPath(secondPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject thirdObject = getCheckPath(thirdPath, studyId, FSObject.FSObjectType.DIRECTORY);
        FSObject fileAObject = getCheckPath(fileAPath, studyId, FSObject.FSObjectType.FILE_NOT_PRESENT);

        fsObject.path(fileBPath);
        UUID fileBId = fileDao.createFileStart(fsObject);
        assertNotNull("File id not null", fileBId);

        FSObject fileBObject = getCheckPath(fileBPath, studyId, FSObject.FSObjectType.FILE_NOT_PRESENT);

        boolean existed = fileDao.deleteFileForUndo(fileAObject.getObjectId(), flightId);
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
            fileDao.deleteFileForUndo(fileBId, flightId);
            fail("Should not have successfully deleted");
        } catch (Exception ex) {
            assertTrue("Correct dependency exception", ex instanceof FileSystemObjectDependencyException);
            assertThat("Correct message", ex.getMessage(), containsString("dataset"));
        }

        removeDatasetDependency(fileBId);

        existed = fileDao.deleteFileForUndo(fileBObject.getObjectId(), flightId);
        assertTrue("File B existed", existed);
    }

    private void addDatasetDependency(UUID objectId) {
        // We reuse the random study id for the dataset id; it doesn't matter what it is here
        String sql = "INSERT INTO fs_dataset (dataset_id,object_id) VALUES (:dataset_id,:object_id)";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("dataset_id", studyId)
            .addValue("object_id", objectId);
        jdbcTemplate.update(sql, params);
    }

    private void removeDatasetDependency(UUID objectId) {
        // We reuse the random study id for the dataset id; it doesn't matter what it is here
        String sql = "DELETE FROM fs_dataset WHERE dataset_id = :dataset_id AND object_id = :object_id";
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("dataset_id", studyId)
            .addValue("object_id", objectId);
        jdbcTemplate.update(sql, params);
    }

    private void checkObjectPresent(FSObject fsObject) {
        FSObject thereObject = fileDao.retrieveFileByPathNoThrow(fsObject.getPath());
        assertNotNull("Object is there by path", thereObject);
        thereObject = fileDao.retrieveFileByIdNoThrow(fsObject.getObjectId());
        assertNotNull("Object is there by id", thereObject);
    }


    private void checkObjectGone(FSObject fsObject) {
        FSObject goneObject = fileDao.retrieveFileByPathNoThrow(fsObject.getPath());
        assertNull("Object is gone by path", goneObject);
        goneObject = fileDao.retrieveFileByIdNoThrow(fsObject.getObjectId());
        assertNull("Object is gone by id", goneObject);
    }

    private FSObject getCheckPath(String path, UUID studyId, FSObject.FSObjectType objectType) {
        FSObject fsObject = fileDao.retrieveFileByPathNoThrow(path);
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


}
