package bio.terra.filesystem;

import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import com.google.cloud.firestore.Firestore;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;

// One DAO to rule them all...
//
// Operations on a file often need to touch file, directory, and dependency collections;
// that is, the FireStoreFileDao, the FireStoreDirectoryDao, and the FireStoreDirectoryDao.
// The data to make an FSDir or FSFile is now spread between the file collection and the
// directory collection, so a lookup needs to visit two places to generate a complete FSObject.
// This class coordinates operations between the daos.
//
// There are several functions performed in this layer.
//  1. Encapsulating the underlying daos
//  2. Converting from dao objects into DR metadata objects
//  3. Dealing with project, dataset, and snapshot objects, so the daos don't have to
//
@Component
public class FireStoreDao {
    private FireStoreDirectoryDao directoryDao;
    private FireStoreFileDao fileDao;
    private FireStoreDependencyDao dependencyDao;
    private FireStoreUtils fireStoreUtils;

    @Autowired
    public FireStoreDao(FireStoreDirectoryDao directoryDao,
                        FireStoreFileDao fileDao,
                        FireStoreDependencyDao dependencyDao,
                        FireStoreUtils fireStoreUtils) {
        this.directoryDao = directoryDao;
        this.fileDao = fileDao;
        this.dependencyDao = dependencyDao;
        this.fireStoreUtils = fireStoreUtils;
    }

    public void createDirectoryEntry(Dataset dataset, FireStoreObject newObject) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = newObject.getDatasetId().toString();
        directoryDao.createFileRef(firestore, datasetId, newObject);
    }

    public boolean deleteDirectoryEntry(Dataset dataset, String objectId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.deleteFileRef(firestore, datasetId, objectId);
    }

    public FireStoreObject retrieveDirectoryEntryByPath(Dataset dataset, String path) {
        return null;
    }

    public void createFileObject(Dataset dataset, FireStoreFile newFile) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        fileDao.createFileMetadata(firestore, datasetId, newFile);
    }

    public boolean deleteFileObject(Dataset dataset, String objectId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return fileDao.deleteFileMetadata(firestore, datasetId, objectId);
    }


    public void deleteFilesFromDataset(Dataset dataset) {
        // scan file collection deleting objects
        // scan directory collection deleting objects
    }

    public void forEachFsObjectInDataset(Dataset dataset, int batchSize, Consumer<FSObjectBase> func) {
        // scan file collection - for each file
        // lookup by id in directory
        // build FSObject
        // apply func
    }

    public FSObjectBase retrieveById(Dataset dataset, String objectId, boolean throwOnNotFound) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();

        FireStoreObject fireStoreObject = directoryDao.retrieveById(firestore, datasetId, objectId);
        if (fireStoreObject == null) {
            if (throwOnNotFound) {
                throw new FileSystemObjectNotFoundException("File system object not found. Id: " + objectId);
            } else {
                return null;
            }
        }

        String fullPath = fireStoreUtils.getFullPath(fireStoreObject.getPath(), fireStoreObject.getName());

        if (!fireStoreObject.isFileRef()) {
            // Make the directory and return it
            FSDir fsDir = new FSDir();
            fsDir
                .objectId(UUID.fromString(fireStoreObject.getObjectId()))
                .datasetId(dataset.getId())
                .createdDate(Instant.parse(fireStoreObject.getFileCreatedDate()))
                .path(fullPath)
                .checksumCrc32c(fireStoreObject.getChecksumCrc32c())
                .checksumMd5(fireStoreObject.getChecksumMd5())
                .size(fireStoreObject.getSize())
                .description(StringUtils.EMPTY);

            return fsDir;
        }

        // Handle files - the fireStoreObject is a reference to a file in a dataset.
        // TODO: when we do this for snapshot, we will need to lookup the dataset from
        // the id in the leaf. For now, just use the dataset we have.
        FireStoreFile fireStoreFile = fileDao.retrieveFileMetadata(firestore, datasetId, objectId);

        FSFile fsFile = new FSFile();
        fsFile
            .objectId(UUID.fromString(objectId))
            .datasetId(dataset.getId())
            .createdDate(Instant.parse(fireStoreFile.getFileCreatedDate()))
            .path(fullPath)
            .checksumCrc32c(fireStoreFile.getChecksumCrc32c())
            .checksumMd5(fireStoreFile.getChecksumMd5())
            .size(fireStoreFile.getSize())
            .description(fireStoreFile.getDescription())
            .gspath(fireStoreFile.getGspath())
            .mimeType(fireStoreFile.getMimeType())
            .profileId(fireStoreFile.getProfileId())
            .region(fireStoreFile.getRegion())
            .bucketResourceId(fireStoreFile.getBucketResourceId());

        return fsFile;
    }

// Let usage drive the retrieve methods we put in here. There are many right now...
/*
    public FSObjectBase retrieveWithContents(Dataset dataset, UUID objectId) {}
    public FSObjectBase retrieve(Dataset dataset, UUID objectId) {}
    public FSObjectBase retrieveByIdNoThrow(Dataset dataset, UUID objectId) {}
    public FSObjectBase retrieveWithContentsByPath(Dataset dataset, String fullPath) {}
    public FSObjectBase retrieveByPath(Dataset dataset, String fullPath) {}
    public FSObjectBase retrieveByPathNoThrow(Dataset dataset, String fullPath) {}}

    public List<String> validateRefIds(Dataset dataset, List<String> refIdArray) {
        // lookup id in directory
        // temporary: count non-files as missing until we implement DIRREF
    }
*/

}
