package org.cobbzilla.s3s3mirror;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.Date;

/**
 * Handles a single key. Determines if it should be copied, and if so, performs the copy operation.
 */
@Slf4j
public class KeyMoveJob extends BaseKeyJob {

    protected String keydest;

    public KeyMoveJob(AmazonS3Client client, MirrorContext context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
    }

    @Override public Logger getLog() { return log; }

    @Override
    boolean execute(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        String key = summary.getKey();
        MirrorOptions options = context.getOptions();
        boolean verbose = options.isVerbose();
        int maxRetries= options.getMaxRetries();
        MirrorStats stats = context.getStats();
        for (int tries = 0; tries < maxRetries; tries++) {
            if (verbose) log.info("copying (try #" + tries + "): " + key + " to: " + keydest);
            final CopyObjectRequest request = new CopyObjectRequest(options.getSourceBucket(), key, options.getDestinationBucket(), keydest);
            final DeleteObjectRequest deleteObjectRequest=new DeleteObjectRequest(options.getSourceBucket(),key);

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
                stats.s3copyCount.incrementAndGet();
                client.copyObject(request);
                client.deleteObject(deleteObjectRequest);
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

}
