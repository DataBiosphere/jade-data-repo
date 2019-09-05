package bio.terra.filesystem;

import bio.terra.filesystem.exception.FileSystemObjectNotFoundException;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSContainerInterface;
import bio.terra.metadata.FSDir;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.Snapshot;
import com.google.cloud.firestore.Firestore;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

// Operations on a file often need to touch file and directory collections that is,
// the FireStoreFileDao and the FireStoreDirectoryDao.
// The data to make an FSDir or FSFile is now spread between the file collection and the
// directory collection, so a lookup needs to visit two places to generate a complete FSObject.
// This class coordinates operations between the daos.
//
// The dependency collection is independent, so it is not included under this dao.
// Perhaps it should be.
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
    private FireStoreUtils fireStoreUtils;

    @Autowired
    public FireStoreDao(FireStoreDirectoryDao directoryDao,
                        FireStoreFileDao fileDao,
                        FireStoreUtils fireStoreUtils) {
        this.directoryDao = directoryDao;
        this.fileDao = fileDao;
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
        directoryDao.deleteDirectoryEntriesFromCollection(firestore, datasetId);
    }

    public FireStoreObject lookupDirectoryEntry(Dataset dataset, String objectId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.retrieveById(firestore, datasetId, objectId);
    }

    public FireStoreObject lookupDirectoryEntryByPath(Dataset dataset, String path) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.retrieveByPath(firestore, datasetId, path);
    }

    public FireStoreFile lookupFile(Dataset dataset, String objectId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return fileDao.retrieveFileMetadata(firestore, datasetId, objectId);
    }

    public void addFilesToSnapshot(Dataset dataset, Snapshot snapshot, List<String> refIds) {
        Firestore datasetFirestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        Firestore snapshotFirestore = FireStoreProject.get(snapshot.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        // TODO: Do we need to clean up the database name?
        String datasetName = dataset.getName();
        String snapshotId = snapshot.getId().toString();

        for (String objectId : refIds) {
            directoryDao.addObjectToSnapshot(
                datasetFirestore,
                datasetId,
                datasetName,
                snapshotFirestore,
                snapshotId,
                objectId);
        }
    }

    public void deleteFilesFromSnapshot(Snapshot snapshot) {
        Firestore firestore = FireStoreProject.get(snapshot.getDataProjectId()).getFirestore();
        String snapshotId = snapshot.getId().toString();
        directoryDao.deleteDirectoryEntriesFromCollection(firestore, snapshotId);
    }

    public void snapshotCompute(Snapshot snapshot) {
        Firestore firestore = FireStoreProject.get(snapshot.getDataProjectId()).getFirestore();
        String snapshotId = snapshot.getId().toString();
        FireStoreObject topDir = directoryDao.retrieveByPath(firestore, snapshotId, "/");
        // If topDir is null, it means no files were added to the snapshot file system in the previous
        // step. So there is nothing to compute
        if (topDir != null) {
            computeDirectory(firestore, snapshotId, topDir);
        }
    }

    /**
     * Retrieve an FS Object by path
     *
     * @param container - dataset or snapshot containing the filesystem object
     * @param fullPath - path of the object in the directory
     * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all;
     *                         1 means contents of this directory; 2 means this and its directories, etc.
     *                         -1 means the entire tree.
     * @param throwOnNotFound - if true, throw an exception if the object id not found; if false,
     *                         null is returned on not found.
     * @return FSFile or FSDir of retrieved object; can return null on not found
     */
    public FSObjectBase retrieveByPath(FSContainerInterface container,
                                       String fullPath,
                                       int enumerateDepth,
                                       boolean throwOnNotFound) {
        Firestore firestore = FireStoreProject.get(container.getDataProjectId()).getFirestore();
        String datasetId = container.getId().toString();

        FireStoreObject fireStoreObject = directoryDao.retrieveByPath(firestore, datasetId, fullPath);
        return retrieveWorker(firestore, datasetId, enumerateDepth, fireStoreObject, throwOnNotFound, fullPath);
    }

    /**
     * Retrieve an FS Object by id
     *
     * @param container - dataset or snapshot containing the filesystem object
     * @param objectId - id of the objecct
     * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all;
     *                         1 means contents of this directory; 2 means this and its directories, etc.
     *                         -1 means the entire tree.
     * @param throwOnNotFound - if true, throw an exception if the object id not found; if false,
     *                         null is returned on not found.
     * @return FSFile or FSDir of retrieved object; can return null on not found
     */
    public FSObjectBase retrieveById(FSContainerInterface container,
                                     String objectId,
                                     int enumerateDepth,
                                     boolean throwOnNotFound) {
        Firestore firestore = FireStoreProject.get(container.getDataProjectId()).getFirestore();
        String datasetId = container.getId().toString();

        FireStoreObject fireStoreObject = directoryDao.retrieveById(firestore, datasetId, objectId);
        return retrieveWorker(firestore, datasetId, enumerateDepth, fireStoreObject, throwOnNotFound, objectId);
    }

    public List<String> validateRefIds(Dataset dataset, List<String> refIdArray) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.validateRefIds(firestore, datasetId, refIdArray);
    }

    // -- private methods --

    // The context string provides either the object id or the file path, for use in error messages.
    private FSObjectBase retrieveWorker(Firestore firestore,
                                        String collectionId,
                                        int enumerateDepth,
                                        FireStoreObject fireStoreObject,
                                        boolean throwOnNotFound,
                                        String context) {
        if (fireStoreObject == null) {
            return handleNotFound(throwOnNotFound, context);
        }

        if (fireStoreObject.getFileRef()) {
            FSObjectBase fsFile = makeFSFile(firestore, collectionId, fireStoreObject);
            if (fsFile == null) {
                // We found a file in the directory that is not done being created. We treat this
                // as not found.
                return handleNotFound(throwOnNotFound, context);
            }
            return fsFile;
        }
        FSObjectBase fsObjectBase = makeFSDir(firestore, collectionId, enumerateDepth, fireStoreObject);

        return fsObjectBase;
    }

    private FSObjectBase handleNotFound(boolean throwOnNotFound, String context) {
        if (throwOnNotFound) {
            throw new FileSystemObjectNotFoundException("File system object not found: " + context);
        } else {
            return null;
        }
    }

    private FSObjectBase makeFSDir(Firestore firestore,
                                   String collectionId,
                                   int level,
                                   FireStoreObject fireStoreObject) {
        if (fireStoreObject.getFileRef()) {
            throw new IllegalStateException("Expected directory; got file!");
        }

        String fullPath = fireStoreUtils.getFullPath(fireStoreObject.getPath(), fireStoreObject.getName());

        FSDir fsDir = new FSDir();
        fsDir
            .objectId(UUID.fromString(fireStoreObject.getObjectId()))
            .collectionId(UUID.fromString(collectionId))
            .createdDate(Instant.parse(fireStoreObject.getFileCreatedDate()))
            .path(fullPath)
            .checksumCrc32c(fireStoreObject.getChecksumCrc32c())
            .checksumMd5(fireStoreObject.getChecksumMd5())
            .size(fireStoreObject.getSize())
            .description(StringUtils.EMPTY);


        if (level != 0) {
            List<FSObjectBase> fsContents = new ArrayList<>();
            List<FireStoreObject> dirContents =
                directoryDao.enumerateDirectory(firestore, collectionId, fullPath);
            for (FireStoreObject fso : dirContents) {
                if (fso.getFileRef()) {
                    // Files that are in the middle of being ingested can have a directory entry, but not yet have
                    // a file entry. We do not return files that do not yet have a file entry.
                    FSObjectBase fsFile = makeFSFile(firestore, collectionId, fso);
                    if (fsFile != null) {
                        fsContents.add(fsFile);
                    }
                } else {
                    fsContents.add(makeFSDir(firestore, collectionId, level - 1, fso));
                }
            }
            fsDir.contents(fsContents);
        }

        return fsDir;
    }

    // Handle files - the fireStoreObject is a reference to a file in a dataset.
    private FSObjectBase makeFSFile(Firestore firestore,
                                    String collectionId,
                                    FireStoreObject fireStoreObject) {
        if (!fireStoreObject.getFileRef()) {
            throw new IllegalStateException("Expected file; got directory!");
        }

        String fullPath = fireStoreUtils.getFullPath(fireStoreObject.getPath(), fireStoreObject.getName());
        String objectId = fireStoreObject.getObjectId();

        // Lookup the file in its owning dataset, not in the collection. The collection may be a snapshot directory
        // pointing to the files in one or more datasets.
        FireStoreFile fireStoreFile =
            fileDao.retrieveFileMetadata(firestore, fireStoreObject.getDatasetId(), objectId);
        if (fireStoreFile == null) {
            return null;
        }

        FSFile fsFile = new FSFile();
        fsFile
            .objectId(UUID.fromString(objectId))
            .collectionId(UUID.fromString(collectionId))
            .datasetId(UUID.fromString(fireStoreObject.getDatasetId()))
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

    // Recursively compute the size and checksums of a directory
    FireStoreObject computeDirectory(Firestore firestore, String snapshotId, FireStoreObject dirObject) {
        String fullPath = fireStoreUtils.getFullPath(dirObject.getPath(), dirObject.getName());
        List<FireStoreObject> enumDir = directoryDao.enumerateDirectory(firestore, snapshotId, fullPath);

        // Recurse to compute results from underlying directories
        List<FireStoreObject> enumComputed = new ArrayList<>();
        for (FireStoreObject dirItem : enumDir) {
            if (dirItem.getFileRef()) {
                // Read the file object to get the size and checksums
                // We do a bit of a hack and copy them into the dirItem. That way we have a homogenous object
                // to deal with in the computation later.
                FireStoreFile file =
                    fileDao.retrieveFileMetadata(firestore, dirItem.getDatasetId(), dirItem.getObjectId());

                dirItem
                    .size(file.getSize())
                    .checksumMd5(file.getChecksumMd5())
                    .checksumCrc32c(file.getChecksumCrc32c());

                enumComputed.add(dirItem);
            } else {
                enumComputed.add(computeDirectory(firestore, snapshotId, dirItem));
            }
        }

        // Collect the ingredients for computing this directory's checksums and size
        List<String> md5Collection = new ArrayList<>();
        List<String> crc32cCollection = new ArrayList<>();
        Long totalSize = 0L;

        for (FireStoreObject dirItem : enumComputed) {
            totalSize = totalSize + dirItem.getSize();
            crc32cCollection.add(StringUtils.lowerCase(dirItem.getChecksumCrc32c()));
            if (dirItem.getChecksumMd5() != null) {
                md5Collection.add(StringUtils.lowerCase(dirItem.getChecksumMd5()));
            }
        }

        // Compute checksums
        // The spec is not 100% clear on the algorithm. I made specific choices on
        // how to implement it:
        // - set hex strings to lowercase before processing so we get consistent sort
        //   order and digest results.
        // - do not make leading zeros converting crc32 long to hex and it is returned
        //   in lowercase. (Matches the semantics of toHexString).
        Collections.sort(md5Collection);
        String md5Concat = StringUtils.join(md5Collection, StringUtils.EMPTY);
        String md5Checksum = fireStoreUtils.computeMd5(md5Concat);

        Collections.sort(crc32cCollection);
        String crc32cConcat = StringUtils.join(crc32cCollection, StringUtils.EMPTY);
        String crc32cChecksum = fireStoreUtils.computeCrc32c(crc32cConcat);

        // Update the directory in place
        dirObject
            .checksumCrc32c(crc32cChecksum)
            .checksumMd5(md5Checksum)
            .size(totalSize);
        directoryDao.updateFileStoreObject(firestore, snapshotId, dirObject);

        return dirObject;
    }

}
