package org.cobbzilla.s3s3util;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

/**
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class KeyCopyJob extends BaseKeyJob {

    public KeyCopyJob(AmazonS3Client client, S3ToS3Context context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
   }

    @Override public Logger getLog() { return log; }

    @Override
    boolean execute(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        String key = summary.getKey();
        S3ToS3Options options = context.getOptions();
        boolean verbose = options.isVerbose();
        int maxRetries= options.getMaxRetries();
        S3ToS3Stats stats = context.getStats();
        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);
            final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest);

            request.setStorageClass(StorageClass.valueOf(options.getStorageClass()));
            
            if (options.isEncrypt()) {
				request.putCustomRequestHeader("x-amz-server-side-encryption", "AES256");
			}
            
            request.setNewObjectMetadata(sourceMetadata);
            if (options.isCrossAccountCopy()) {
                request.setCannedAccessControlList(CannedAccessControlList.BucketOwnerFullControl);
            } else {
                request.setAccessControlList(objectAcl);
            }
            try {
                client.copyObject(request);
                stats.s3copyCount.incrementAndGet();
                stats.bytesCopied.addAndGet(sourceMetadata.getContentLength());
                if (verbose) log.info("successfully copied (on try #" + tries + "): " + key + " to: " + keydest);
                return true;
            } catch (AmazonS3Exception s3e) {
                log.error("s3 exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + s3e);
            } catch (Exception e) {
                log.error("unexpected exception copying (try #" + tries + ") " + key + " to: " + keydest + ": " + e);
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                log.error("interrupted while waiting to retry key: " + key);
                return false;
            }
        }
        return false;
    }

//    private boolean shouldTransfer() {
//        final MirrorOptions options = context.getOptions();
//        final String key = summary.getKey();
//        final boolean verbose = options.isVerbose();
//
//        if (options.hasCtime()) {
//            final Date lastModified = summary.getLastModified();
//            if (lastModified == null) {
//                if (verbose) log.info("No Last-Modified header for key: " + key);
//
//            } else {
//                if (lastModified.getTime() < options.getAge()) {
//                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is older than "+options.getCtime()+" (cutoff="+options.getAgeDate()+"), not copying");
//                    return false;
//                }
//            }
//        }
//        final ObjectMetadata metadata;
//        try {
//            metadata = getObjectMetadata(options.getDestinationBucket(), keydest, options);
//        } catch (AmazonS3Exception e) {
//            if (e.getStatusCode() == 404) {
//                if (verbose) log.info("Key not found in destination bucket (will copy): "+ keydest);
//                return true;
//            } else {
//                log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
//                return false;
//            }
//        } catch (Exception e) {
//            log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
//            return false;
//        }
//
//        if (summary.getSize() > MirrorOptions.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
//            return metadata.getContentLength() != summary.getSize();
//        }
//        final boolean objectChanged = objectChanged(metadata);
//        if (verbose && !objectChanged) log.info("Destination file is same as source, not copying: "+ key);
//
//        return objectChanged;
//    }
//
//    boolean objectChanged(ObjectMetadata metadata) {
//        final MirrorOptions options = context.getOptions();
//        final KeyFingerprint sourceFingerprint;
//        final KeyFingerprint destFingerprint;
//
//        if (options.isSizeOnly()) {
//            sourceFingerprint = new KeyFingerprint(summary.getSize());
//            destFingerprint = new KeyFingerprint(metadata.getContentLength());
//        } else {
//            sourceFingerprint = new KeyFingerprint(summary.getSize(), summary.getETag());
//            destFingerprint = new KeyFingerprint(metadata.getContentLength(), metadata.getETag());
//        }
//
//        return !sourceFingerprint.equals(destFingerprint);
//    }
}
