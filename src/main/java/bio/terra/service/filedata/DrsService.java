package bio.terra.service.filedata;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.model.DRSAccessMethod;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSChecksum;
import bio.terra.model.DRSContentsObject;
import bio.terra.model.DRSObject;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.exception.DrsObjectNotFoundException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.kubernetes.KubeService;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.SnapshotProject;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@Component
public class DrsService {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.service.filedata.DrsService");

    private static final String DRS_OBJECT_VERSION = "0";
    // atomic counter that we incr on request arrival and decr on request response
    private static int currentDRSRequests = 0;

    private final SnapshotService snapshotService;
    private final FileService fileService;
    private final DrsIdService drsIdService;
    private final IamService samService;
    private final ResourceService resourceService;
    private final ConfigurationService configurationService;
    private final KubeService kubeService;
    private final PerformanceLogger performanceLogger;

    @Autowired
    public DrsService(SnapshotService snapshotService,
                      FileService fileService,
                      DrsIdService drsIdService,
                      IamService samService,
                      ResourceService resourceService,
                      ConfigurationService configurationService,
                      KubeService kubeService,
                      PerformanceLogger performanceLogger) {
        this.snapshotService = snapshotService;
        this.fileService = fileService;
        this.drsIdService = drsIdService;
        this.samService = samService;
        this.resourceService = resourceService;
        this.configurationService = configurationService;
        this.kubeService = kubeService;
        this.performanceLogger = performanceLogger;
    }



    public DRSObject lookupObjectByDrsId(AuthenticatedUserRequest authUser, String drsObjectId, Boolean expand) {

        // make sure not too many requests are being made at once
        currentDRSRequests++;
        int podCount = kubeService.getActivePodCount();
        int maxDRSLookups = configurationService.getParameterValue(ConfigEnum.DRS_LOOKUP_MAX);
        int max = maxDRSLookups / podCount;
        if (currentDRSRequests >= max) {
            // what's the best way to return this in our current format?
            HttpStatus.TOO_MANY_REQUESTS;
        }
        DrsId drsId = drsIdService.fromObjectId(drsObjectId);
        SnapshotProject snapshotProject = null;
        try {
            UUID snapshotId = UUID.fromString(drsId.getSnapshotId());
            // We only look up DRS ids for unlocked snapshots.
            String retrieveTimer = performanceLogger.timerStart();

            snapshotProject = snapshotService.retrieveAvailableSnapshotProject(snapshotId);

            performanceLogger.timerEndAndLog(
                retrieveTimer,
                drsObjectId, // not a flight, so no job id
                this.getClass().getName(),
                "snapshotService.retrieveAvailable");
        } catch (IllegalArgumentException ex) {
            throw new InvalidDrsIdException("Invalid object id format '" + drsObjectId + "'", ex);
        } catch (SnapshotNotFoundException ex) {
            throw new DrsObjectNotFoundException("No snapshot found for DRS object id '" + drsObjectId + "'", ex);
        }

        // Make sure requester is a READER on the snapshot
        String samTimer = performanceLogger.timerStart();

        samService.verifyAuthorization(
            authUser,
            IamResourceType.DATASNAPSHOT,
            drsId.getSnapshotId(),
            IamAction.READ_DATA);

        performanceLogger.timerEndAndLog(
            samTimer,
            drsObjectId, // not a flight, so no job id
            this.getClass().getName(),
            "samService.verifyAuthorization");

        int depth = (expand ? -1 : 1);

        FSItem fsObject = null;
        try {
            String lookupTimer = performanceLogger.timerStart();
            fsObject = fileService.lookupSnapshotFSItem(
                snapshotProject,
                drsId.getFsObjectId(),
                depth);

            performanceLogger.timerEndAndLog(
                lookupTimer,
                drsObjectId, // not a flight, so no job id
                this.getClass().getName(),
                "fileService.lookupSnapshotFSItem");
        } catch (InterruptedException ex) {
            currentDRSRequests--;
            throw new FileSystemExecutionException("Unexpected interruption during file system processing", ex);
        }
        currentDRSRequests--; // TODO is there a better place for this?

        if (fsObject instanceof FSFile) {
            return drsObjectFromFSFile((FSFile)fsObject, drsId.getSnapshotId(), authUser);
        } else if (fsObject instanceof FSDir) {
            return drsObjectFromFSDir((FSDir)fsObject, drsId.getSnapshotId());
        }

        throw new IllegalArgumentException("Invalid object type");
    }

    private DRSObject drsObjectFromFSFile(FSFile fsFile, String snapshotId, AuthenticatedUserRequest authUser) {
        DRSObject fileObject = makeCommonDrsObject(fsFile, snapshotId);

        GoogleBucketResource bucketResource = resourceService.lookupBucketMetadata(fsFile.getBucketResourceId());

        DRSAccessURL gsAccessURL = new DRSAccessURL()
            .url(fsFile.getGspath());

        DRSAccessMethod gsAccessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.GS)
            .accessUrl(gsAccessURL)
            .region(bucketResource.getRegion());

        DRSAccessURL httpsAccessURL = new DRSAccessURL()
            .url(makeHttpsFromGs(fsFile.getGspath()))
            .headers(makeAuthHeader(authUser));

        DRSAccessMethod httpsAccessMethod = new DRSAccessMethod()
            .type(DRSAccessMethod.TypeEnum.HTTPS)
            .accessUrl(httpsAccessURL)
            .region(bucketResource.getRegion());

        List<DRSAccessMethod> accessMethods = new ArrayList<>();
        accessMethods.add(gsAccessMethod);
        accessMethods.add(httpsAccessMethod);


        fileObject
            .mimeType(fsFile.getMimeType())
            .checksums(fileService.makeChecksums(fsFile))
            .accessMethods(accessMethods);

        return fileObject;
    }

    private DRSObject drsObjectFromFSDir(FSDir fsDir, String snapshotId) {
        DRSObject dirObject = makeCommonDrsObject(fsDir, snapshotId);

        DRSChecksum drsChecksum = new DRSChecksum().type("crc32c").checksum("0");
        dirObject
            .size(0L)
            .addChecksumsItem(drsChecksum)
            .contents(makeContentsList(fsDir, snapshotId));

        return dirObject;
    }

    private DRSObject makeCommonDrsObject(FSItem fsObject, String snapshotId) {
        // Compute the time once; used for both created and updated times as per DRS spec for immutable objects
        String theTime = fsObject.getCreatedDate().toString();
        DrsId drsId = drsIdService.makeDrsId(fsObject, snapshotId);

        return new DRSObject()
            .id(drsId.toDrsObjectId())
            .name(getLastNameFromPath(fsObject.getPath()))
            .createdTime(theTime)
            .updatedTime(theTime)
            .version(DRS_OBJECT_VERSION)
            .description(fsObject.getDescription())
            .aliases(Collections.singletonList(fsObject.getPath()))
            .size(fsObject.getSize())
            .checksums(fileService.makeChecksums(fsObject));
    }

    private List<DRSContentsObject> makeContentsList(FSDir fsDir, String snapshotId) {
        List<DRSContentsObject> contentsList = new ArrayList<>();

        for (FSItem fsObject : fsDir.getContents()) {
            contentsList.add(makeDrsContentsObject(fsObject, snapshotId));
        }

        return contentsList;
    }

    private DRSContentsObject makeDrsContentsObject(FSItem fsObject, String snapshotId) {
        DrsId drsId = drsIdService.makeDrsId(fsObject, snapshotId);

        List<String> drsUris = new ArrayList<>();
        drsUris.add(drsId.toDrsUri());

        DRSContentsObject contentsObject = new DRSContentsObject()
            .name(getLastNameFromPath(fsObject.getPath()))
            .id(drsId.toDrsObjectId())
            .drsUri(drsUris);

        if (fsObject instanceof FSDir) {
            FSDir fsDir = (FSDir) fsObject;
            if (fsDir.isEnumerated()) {
                contentsObject.contents(makeContentsList(fsDir, snapshotId));
            }
        }

        return contentsObject;
    }

    private String makeHttpsFromGs(String gspath) {
        try {
            GcsPdao.GcsLocator locator = GcsPdao.getGcsLocatorFromGsPath(gspath);
            String gsBucket = locator.getBucket();
            String gsPath = locator.getPath();
            String encodedPath = URLEncoder.encode(gsPath, StandardCharsets.UTF_8.toString())
                // Google does not recognize the + characters that are produced from spaces by the URLEncoder.encode
                // method. As a result, these must be converted to %2B.
                .replaceAll("\\+", "%20");
            return String.format("https://www.googleapis.com/storage/v1/b/%s/o/%s?alt=media", gsBucket, encodedPath);
        } catch (UnsupportedEncodingException ex) {
            throw new InvalidDrsIdException("Failed to urlencode file path", ex);
        }
    }

    private List<String> makeAuthHeader(AuthenticatedUserRequest authUser) {
        // TODO: I added this so that connected tests would work. Seems like we should have a better solution.
        // I don't like putting test-path-only stuff into the production code.
        if (authUser == null || !authUser.getToken().isPresent()) {
            return Collections.EMPTY_LIST;
        }

        String hdr = String.format("Authorization: Bearer %s", authUser.getRequiredToken());
        return Collections.singletonList(hdr);
    }

    public static String getLastNameFromPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        return pathParts[pathParts.length - 1];
    }

}
