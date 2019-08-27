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
import java.util.ArrayList;
import java.util.List;
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

    public void deleteFilesFromDataset(Dataset dataset, Consumer<FireStoreFile> func) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        fileDao.deleteFilesFromDataset(firestore, datasetId, func);
        directoryDao.deleteDirectoryEntriesFromDataset(firestore, datasetId);
    }
    public FireStoreObject lookupDirectoryEntry(Dataset dataset, String objectId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.retrieveById(firestore, datasetId, objectId);
    }

    public FireStoreFile lookupFile(Dataset dataset, String objectId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
    }

    /**
     * Retrieve an FS Object by path
     *
     * @param dataset - dataset containing the filesystem object
     * @param fullPath - path of the object in the directory
     * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all;
     *                         1 means contents of this directory; 2 means this and its directories, etc.
     *                         -1 means the entire tree.
     * @param throwOnNotFound - if true, throw an exception if the object id not found; if false,
     *                         null is returned on not found.
     * @return FSFile or FSDir of retrieved object; can return null on not found
     */
    public FSObjectBase retrieveByPath(Dataset dataset, String fullPath, int enumerateDepth, boolean throwOnNotFound) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();

        FireStoreObject fireStoreObject = directoryDao.retrieveByPath(firestore, datasetId, fullPath);
        return retrieveWorker(firestore, datasetId, enumerateDepth, fireStoreObject, throwOnNotFound, fullPath);
    }

    /**
     * Retrieve an FS Object by id
     *
     * @param dataset - dataset containing the filesystem object
     * @param objectId - id of the objecct
     * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all;
     *                         1 means contents of this directory; 2 means this and its directories, etc.
     *                         -1 means the entire tree.
     * @param throwOnNotFound - if true, throw an exception if the object id not found; if false,
     *                         null is returned on not found.
     * @return FSFile or FSDir of retrieved object; can return null on not found
     */
    public FSObjectBase retrieveById(Dataset dataset, String objectId, int enumerateDepth, boolean throwOnNotFound) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();

        FireStoreObject fireStoreObject = directoryDao.retrieveById(firestore, datasetId, objectId);
        return retrieveWorker(firestore, datasetId, enumerateDepth, fireStoreObject, throwOnNotFound, objectId);
    }

    public List<String> validateRefIds(Dataset dataset, List<String> refIdArray) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.validateRefIds(firestore, datasetId, refIdArray);
    }

    // -- private methods --

    private FSObjectBase retrieveWorker(Firestore firestore,
                                        String datasetId,
                                        int enumerateDepth,
                                        FireStoreObject fireStoreObject,
                                        boolean throwOnNotFound,
                                        String context) {
        if (fireStoreObject == null) {
            return handleNotFound(throwOnNotFound, context);
        }

        if (fireStoreObject.getFileRef()) {
            FSObjectBase fsFile = makeFSFile(firestore, datasetId, fireStoreObject);
            if (fsFile == null) {
                // We found a file in the directory that is not done being created. We treat this
                // as not found.
                return handleNotFound(throwOnNotFound, context);
            }
        }
        return makeFSDir(firestore, datasetId, enumerateDepth, fireStoreObject);
    }

    private FSObjectBase handleNotFound(boolean throwOnNotFound, String context) {
        if (throwOnNotFound) {
            throw new FileSystemObjectNotFoundException("File system object not found: " + context);
        } else {
            return null;
        }
    }

    private FSObjectBase makeFSDir(Firestore firestore,
                                   String datasetId,
                                   int level,
                                   FireStoreObject fireStoreObject) {
        if (fireStoreObject.getFileRef()) {
            throw new IllegalStateException("Expected directory; got file!");
        }

        String fullPath = fireStoreUtils.getFullPath(fireStoreObject.getPath(), fireStoreObject.getName());

        FSDir fsDir = new FSDir();
        fsDir
            .objectId(UUID.fromString(fireStoreObject.getObjectId()))
            .datasetId(UUID.fromString(datasetId))
            .createdDate(Instant.parse(fireStoreObject.getFileCreatedDate()))
            .path(fullPath)
            .checksumCrc32c(fireStoreObject.getChecksumCrc32c())
            .checksumMd5(fireStoreObject.getChecksumMd5())
            .size(fireStoreObject.getSize())
            .description(StringUtils.EMPTY);


        if (level != 0) {
            List<FSObjectBase> fsContents = new ArrayList<>();
            List<FireStoreObject> dirContents =
                directoryDao.enumerateDirectory(firestore, datasetId, fireStoreObject.getPath());
            for (FireStoreObject fso : dirContents) {
                if (fso.getFileRef()) {
                    fsContents.add(makeFSFile(firestore, datasetId, fso));
                } else {
                    fsContents.add(makeFSDir(firestore, datasetId, level - 1, fso));
                }
            }
            fsDir.contents(fsContents);
        }

        return fsDir;
    }

    // Handle files - the fireStoreObject is a reference to a file in a dataset.
    // TODO: when we do this for snapshot, we will need to lookup the dataset from
    // the id in the leaf. For now, just use dataset.
    private FSObjectBase makeFSFile(Firestore firestore,
                                    String datasetId,
                                    FireStoreObject fireStoreObject) {
        if (!fireStoreObject.getFileRef()) {
            throw new IllegalStateException("Expected file; got directory!");
        }

        String fullPath = fireStoreUtils.getFullPath(fireStoreObject.getPath(), fireStoreObject.getName());
        String objectId = fireStoreObject.getObjectId();

        FireStoreFile fireStoreFile = fileDao.retrieveFileMetadata(firestore, datasetId, objectId);

        FSFile fsFile = new FSFile();
        fsFile
            .objectId(UUID.fromString(objectId))
            .datasetId(UUID.fromString(datasetId))
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

}
