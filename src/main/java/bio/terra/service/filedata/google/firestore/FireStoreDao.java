package bio.terra.service.filedata.google.firestore;

import bio.terra.service.filedata.exception.FileNotFoundException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSContainerInterface;
import bio.terra.service.filedata.FSDir;
import bio.terra.service.filedata.FSFile;
import bio.terra.service.filedata.FSItem;
import bio.terra.service.snapshot.Snapshot;
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
// directory collection, so a lookup needs to visit two places to generate a complete FSItem.
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

    public void createDirectoryEntry(Dataset dataset, FireStoreDirectoryEntry newEntry) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = newEntry.getDatasetId().toString();
        directoryDao.createDirectoryEntry(firestore, datasetId, newEntry);
    }

    public boolean deleteDirectoryEntry(Dataset dataset, String fileId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.deleteDirectoryEntry(firestore, datasetId, fileId);
    }

    public void createFileMetadata(Dataset dataset, FireStoreFile newFile) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        fileDao.createFileMetadata(firestore, datasetId, newFile);
    }

    public boolean deleteFileMetadata(Dataset dataset, String fileId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return fileDao.deleteFileMetadata(firestore, datasetId, fileId);
    }

    public void deleteFilesFromDataset(Dataset dataset, Consumer<FireStoreFile> func) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        fileDao.deleteFilesFromDataset(firestore, datasetId, func);
        directoryDao.deleteDirectoryEntriesFromCollection(firestore, datasetId);
    }

    public FireStoreDirectoryEntry lookupDirectoryEntry(Dataset dataset, String fileId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.retrieveById(firestore, datasetId, fileId);
    }

    public FireStoreDirectoryEntry lookupDirectoryEntryByPath(Dataset dataset, String path) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.retrieveByPath(firestore, datasetId, path);
    }

    public FireStoreFile lookupFile(Dataset dataset, String fileId) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return fileDao.retrieveFileMetadata(firestore, datasetId, fileId);
    }

    public void addFilesToSnapshot(Dataset dataset, Snapshot snapshot, List<String> refIds) {
        Firestore datasetFirestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        Firestore snapshotFirestore = FireStoreProject.get(snapshot.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        // TODO: Do we need to make sure the dataset name does not contain characters that are invalid for paths?
        // Added the work to figure that out to DR-325
        String datasetName = dataset.getName();
        String snapshotId = snapshot.getId().toString();

        for (String fileId : refIds) {
            directoryDao.addEntryToSnapshot(
                datasetFirestore,
                datasetId,
                datasetName,
                snapshotFirestore,
                snapshotId,
                fileId);
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
        FireStoreDirectoryEntry topDir = directoryDao.retrieveByPath(firestore, snapshotId, "/");
        // If topDir is null, it means no files were added to the snapshot file system in the previous
        // step. So there is nothing to compute
        if (topDir != null) {
            computeDirectory(firestore, snapshotId, topDir);
        }
    }

    /**
     * Retrieve an FSItem by path
     *
     * @param container - dataset or snapshot containing file's directory entry
     * @param fullPath - path of the file in the directory
     * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all;
     *                         1 means contents of this directory; 2 means this and its directories, etc.
     *                         -1 means the entire tree.
     * @param throwOnNotFound - if true, throw an exception if the file id not found; if false,
     *                         null is returned on not found.
     * @return FSFile or FSDir of retrieved file; can return null on not found
     */
    public FSItem retrieveByPath(FSContainerInterface container,
                                 String fullPath,
                                 int enumerateDepth,
                                 boolean throwOnNotFound) {
        Firestore firestore = FireStoreProject.get(container.getDataProjectId()).getFirestore();
        String datasetId = container.getId().toString();

        FireStoreDirectoryEntry fireStoreDirectoryEntry = directoryDao.retrieveByPath(firestore, datasetId, fullPath);
        return retrieveWorker(firestore, datasetId, enumerateDepth, fireStoreDirectoryEntry, throwOnNotFound, fullPath);
    }

    /**
     * Retrieve an FSItem by id
     *
     * @param container - dataset or snapshot containing file's directory entry
     * @param fileId - id of the file or directory
     * @param enumerateDepth - how far to enumerate the directory structure; 0 means not at all;
     *                         1 means contents of this directory; 2 means this and its directories, etc.
     *                         -1 means the entire tree.
     * @param throwOnNotFound - if true, throw an exception if the file id not found; if false,
     *                         null is returned on not found.
     * @return FSFile or FSDir of retrieved file; can return null on not found
     */
    public FSItem retrieveById(FSContainerInterface container,
                               String fileId,
                               int enumerateDepth,
                               boolean throwOnNotFound) {
        Firestore firestore = FireStoreProject.get(container.getDataProjectId()).getFirestore();
        String datasetId = container.getId().toString();

        FireStoreDirectoryEntry fireStoreDirectoryEntry = directoryDao.retrieveById(firestore, datasetId, fileId);
        return retrieveWorker(firestore, datasetId, enumerateDepth, fireStoreDirectoryEntry, throwOnNotFound, fileId);
    }

    public List<String> validateRefIds(Dataset dataset, List<String> refIdArray) {
        Firestore firestore = FireStoreProject.get(dataset.getDataProjectId()).getFirestore();
        String datasetId = dataset.getId().toString();
        return directoryDao.validateRefIds(firestore, datasetId, refIdArray);
    }

    // -- private methods --

    // The context string provides either the file id or the file path, for use in error messages.
    private FSItem retrieveWorker(Firestore firestore,
                                  String collectionId,
                                  int enumerateDepth,
                                  FireStoreDirectoryEntry fireStoreDirectoryEntry,
                                  boolean throwOnNotFound,
                                  String context) {
        if (fireStoreDirectoryEntry == null) {
            return handleNotFound(throwOnNotFound, context);
        }

        if (fireStoreDirectoryEntry.getIsFileRef()) {
            FSItem fsFile = makeFSFile(firestore, collectionId, fireStoreDirectoryEntry);
            if (fsFile == null) {
                // We found a file in the directory that is not done being created. We treat this
                // as not found.
                return handleNotFound(throwOnNotFound, context);
            }
            return fsFile;
        }
        FSItem fsItem = makeFSDir(firestore, collectionId, enumerateDepth, fireStoreDirectoryEntry);

        return fsItem;
    }

    private FSItem handleNotFound(boolean throwOnNotFound, String context) {
        if (throwOnNotFound) {
            throw new FileNotFoundException("File not found: " + context);
        } else {
            return null;
        }
    }

    private FSItem makeFSDir(Firestore firestore,
                             String collectionId,
                             int level,
                             FireStoreDirectoryEntry fireStoreDirectoryEntry) {
        if (fireStoreDirectoryEntry.getIsFileRef()) {
            throw new IllegalStateException("Expected directory; got file!");
        }

        String fullPath =
            fireStoreUtils.getFullPath(fireStoreDirectoryEntry.getPath(), fireStoreDirectoryEntry.getName());

        FSDir fsDir = new FSDir();
        fsDir
            .fileId(UUID.fromString(fireStoreDirectoryEntry.getFileId()))
            .collectionId(UUID.fromString(collectionId))
            .createdDate(Instant.parse(fireStoreDirectoryEntry.getFileCreatedDate()))
            .path(fullPath)
            .checksumCrc32c(fireStoreDirectoryEntry.getChecksumCrc32c())
            .checksumMd5(fireStoreDirectoryEntry.getChecksumMd5())
            .size(fireStoreDirectoryEntry.getSize())
            .description(StringUtils.EMPTY);


        if (level != 0) {
            List<FSItem> fsContents = new ArrayList<>();
            List<FireStoreDirectoryEntry> dirContents =
                directoryDao.enumerateDirectory(firestore, collectionId, fullPath);
            for (FireStoreDirectoryEntry fso : dirContents) {
                if (fso.getIsFileRef()) {
                    // Files that are in the middle of being ingested can have a directory entry, but not yet have
                    // a file entry. We do not return files that do not yet have a file entry.
                    FSItem fsFile = makeFSFile(firestore, collectionId, fso);
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

    // Handle files - the fireStoreDirectoryEntry is a reference to a file in a dataset.
    private FSItem makeFSFile(Firestore firestore,
                              String collectionId,
                              FireStoreDirectoryEntry fireStoreDirectoryEntry) {
        if (!fireStoreDirectoryEntry.getIsFileRef()) {
            throw new IllegalStateException("Expected file; got directory!");
        }

        String fullPath =
            fireStoreUtils.getFullPath(fireStoreDirectoryEntry.getPath(), fireStoreDirectoryEntry.getName());
        String fileId = fireStoreDirectoryEntry.getFileId();

        // Lookup the file in its owning dataset, not in the collection. The collection may be a snapshot directory
        // pointing to the files in one or more datasets.
        FireStoreFile fireStoreFile =
            fileDao.retrieveFileMetadata(firestore, fireStoreDirectoryEntry.getDatasetId(), fileId);
        if (fireStoreFile == null) {
            return null;
        }

        FSFile fsFile = new FSFile();
        fsFile
            .fileId(UUID.fromString(fileId))
            .collectionId(UUID.fromString(collectionId))
            .datasetId(UUID.fromString(fireStoreDirectoryEntry.getDatasetId()))
            .createdDate(Instant.parse(fireStoreFile.getFileCreatedDate()))
            .path(fullPath)
            .checksumCrc32c(fireStoreFile.getChecksumCrc32c())
            .checksumMd5(fireStoreFile.getChecksumMd5())
            .size(fireStoreFile.getSize())
            .description(fireStoreFile.getDescription())
            .gspath(fireStoreFile.getGspath())
            .mimeType(fireStoreFile.getMimeType())
            .bucketResourceId(fireStoreFile.getBucketResourceId());

        return fsFile;
    }

    // Recursively compute the size and checksums of a directory
    FireStoreDirectoryEntry computeDirectory(Firestore firestore, String snapshotId, FireStoreDirectoryEntry dirEntry) {
        String fullPath = fireStoreUtils.getFullPath(dirEntry.getPath(), dirEntry.getName());
        List<FireStoreDirectoryEntry> enumDir = directoryDao.enumerateDirectory(firestore, snapshotId, fullPath);

        // Recurse to compute results from underlying directories
        List<FireStoreDirectoryEntry> enumComputed = new ArrayList<>();
        for (FireStoreDirectoryEntry dirItem : enumDir) {
            if (dirItem.getIsFileRef()) {
                // Read the file metadata to get the size and checksum. We do a bit of a hack and copy
                // the size and checksums into the in-memory dirItem. That way we only compute on the directory
                // objects.
                FireStoreFile file =
                    fileDao.retrieveFileMetadata(firestore, dirItem.getDatasetId(), dirItem.getFileId());

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

        for (FireStoreDirectoryEntry dirItem : enumComputed) {
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
        dirEntry
            .checksumCrc32c(crc32cChecksum)
            .checksumMd5(md5Checksum)
            .size(totalSize);
        directoryDao.updateDirectoryEntry(firestore, snapshotId, dirEntry);

        return dirEntry;
    }

}
