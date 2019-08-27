package bio.terra.filesystem;

import bio.terra.category.Connected;
import bio.terra.fixtures.StringListCompare;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class DirectoryDaoTest {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.DirectoryDaoTest");

    @Autowired
    private FireStoreDirectoryDao directoryDao;

    @Autowired
    private FireStoreUtils fireStoreUtils;

    private String pretendDatasetId;
    private String collectionId;
    private Firestore firestore;

    @Before
    public void setup() throws Exception {
        pretendDatasetId = UUID.randomUUID().toString();
        collectionId = "directoryDaoTest_" + pretendDatasetId;
        firestore = FirestoreOptions.getDefaultInstance().getService();
    }

    @Test
    // Tests createFileRef, deleteFileRef, retrieveById, retrieveByPath
    public void createDeleteTest() throws Exception {
        FireStoreObject fileA = makeFileObject("/adir/A");

        // Verify file A should not exist
        FireStoreObject testFileA = directoryDao.retrieveById(firestore, collectionId, fileA.getObjectId());
        assertNull("Object id does not exist", testFileA);

        // Create the file
        directoryDao.createFileRef(firestore, collectionId, fileA);
        testFileA = directoryDao.retrieveById(firestore, collectionId, fileA.getObjectId());
        assertNotNull("Object id exists", testFileA);
        assertTrue("Is file object", testFileA.getFileRef());
        assertThat("Dataset id matches", pretendDatasetId, equalTo(testFileA.getDatasetId()));

        // Test overwrite semantics - a second create acts as an update
        String updatedDatasetId = pretendDatasetId + "X";
        fileA.datasetId(updatedDatasetId);
        directoryDao.createFileRef(firestore, collectionId, fileA);
        testFileA = directoryDao.retrieveById(firestore, collectionId, fileA.getObjectId());
        assertNotNull("Object id exists", testFileA);
        assertTrue("Is file object", testFileA.getFileRef());
        assertThat("Updated id matches", testFileA.getDatasetId(), equalTo(updatedDatasetId));

        // Lookup the directory by path to get its object id so that we can make sure
        // it goes away when we delete the file.
        FireStoreObject dirA = directoryDao.retrieveByPath(firestore, collectionId, "/adir");
        assertNotNull("Directory exists", dirA);
        assertFalse("Is dir object", dirA.getFileRef());

        // Delete file and verify everything is gone
        boolean objectExisted = directoryDao.deleteFileRef(firestore, collectionId, fileA.getObjectId());
        assertTrue("Object existed", objectExisted);
        testFileA = directoryDao.retrieveById(firestore, collectionId, fileA.getObjectId());
        assertNull("File was deleted", testFileA);
        FireStoreObject testDirA = directoryDao.retrieveById(firestore, collectionId, dirA.getObjectId());
        assertNull("Directory was deleted", testDirA);

        // Delete again. Should succeed and let us know the object didn't exist
        objectExisted = directoryDao.deleteFileRef(firestore, collectionId, fileA.getObjectId());
        assertFalse("Object did not exist", objectExisted);
    }


    @Test
    // Tests validateRefIds, enumerateDirectory, deleteDirectoryEntriesFromDataset, retrieveById, retrieveByPath
    public void directoryOperationsTest() throws Exception {
        List<FireStoreObject> fileObjects = new ArrayList<>();
        fileObjects.add(makeFileObject("/adir/A1"));
        fileObjects.add(makeFileObject("/adir/bdir/B1"));
        fileObjects.add(makeFileObject("/adir/bdir/cdir/C1"));
        fileObjects.add(makeFileObject("/adir/bdir/cdir/C2"));
        fileObjects.add(makeFileObject("/adir/bdir/B2"));
        fileObjects.add(makeFileObject("/adir/A2"));

        // Test all valid file references
        List<String> fileRefs = fileObjects
            .stream()
            .map(fireStoreObject -> fireStoreObject.getObjectId())
            .collect(Collectors.toList());
        List<String> mismatches = directoryDao.validateRefIds(firestore, collectionId, fileRefs);
        assertThat("No invalid file refs", mismatches.size(), equalTo(0));

        List<String> badids = Arrays.asList("badid1", "badid2");

        // Test with invalid file references
        List<String> badFileRefs = new ArrayList<>();
        badFileRefs.addAll(fileRefs);
        badFileRefs.addAll(badids);

        mismatches = directoryDao.validateRefIds(firestore, collectionId, badFileRefs);
        assertThat("Caught invalid file refs", mismatches.size(), equalTo(badids.size()));

        StringListCompare listCompare = new StringListCompare(mismatches, badids);
        assertTrue("Bad ids match", listCompare.compare());

        // Test enumeration with adir. We should get three things back: two files (A1, A2) and a directory (bdir).
        List<FireStoreObject> enumList = directoryDao.enumerateDirectory(firestore, collectionId, "/adir");
        assertThat("Correct number of object returned", enumList.size(), equalTo(3));
        List<String> expectedNames = Arrays.asList("A1", "A2", "bdir");
        List<String> enumNames = enumList
            .stream()
            .map(fireStoreObject -> fireStoreObject.getObjectId())
            .collect(Collectors.toList());

        StringListCompare enumCompare = new StringListCompare(expectedNames, enumNames);
        assertTrue("Enum names match", enumCompare.compare());

        // Now add to the ref list all of the valid file object ids and then include the directory ids. We'll use that
        // list to make sure that "delete everything" really deletes everything.
        fileRefs.add(retrieveDirectoryObjectId("/adir"));
        fileRefs.add(retrieveDirectoryObjectId("/adir/bdir"));
        fileRefs.add(retrieveDirectoryObjectId("/adir/cdir"));

        directoryDao.deleteDirectoryEntriesFromDataset(firestore, collectionId);

        for (String objectId : fileRefs) {
            FireStoreObject fso = directoryDao.retrieveById(firestore, collectionId, objectId);
            assertNull("File or dir object is deleted", fso);
        }
    }

    private String retrieveDirectoryObjectId(String fullPath) {
        FireStoreObject fireStoreObject = directoryDao.retrieveByPath(firestore, collectionId, fullPath);
        return fireStoreObject.getObjectId();
    }

    private FireStoreObject makeFileObject(String fullPath) {
        return new FireStoreObject()
            .objectId(UUID.randomUUID().toString())
            .fileRef(true)
            .path(fireStoreUtils.getDirectoryPath(fullPath))
            .name(fireStoreUtils.getObjectName(fullPath))
            .datasetId(pretendDatasetId);
    }

/*
 Tests:

    validateRefIds
    deleteDirectoryEntriesFromDataset
    retrieveByPath
    - create a file/directory structure
    - look up all objects by path - save object ids
    - validateRefIds - some valid, some invalid - check proper missing return
    - enumerateDirectory - make sure it has the right things in it
    - deleteDirectoryEntriesFromDataset
    - lookup all object ids - none should exist

 */




}
