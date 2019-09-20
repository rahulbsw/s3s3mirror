package org.cobbzilla.s3s3util;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.Date;

@Slf4j
public class KeyDeleteJob extends BaseKeyJob {

    private String keysrc;

    public KeyDeleteJob (AmazonS3Client client, S3ToS3Context context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);

        final S3ToS3Options options = context.getOptions();
        keysrc = summary.getKey(); // NOTE: summary.getKey is the key in the destination bucket
        if (options.hasPrefix()) {
            keysrc = keysrc.substring(options.getDestPrefixLength());
            keysrc = options.getPrefix() + keysrc;
        }
    }

    @Override public Logger getLog() { return log; }


    @Override
    public boolean execute(ObjectMetadata sourceMetadata, AccessControlList objectAcl) {
        final S3ToS3Options options = context.getOptions();
        final S3ToS3Stats stats = context.getStats();
        final boolean verbose = options.isVerbose();
        final int maxRetries = options.getMaxRetries();
        final String key = summary.getKey();
        try {
            if (!shouldAction()) return false;

            final DeleteObjectRequest request = new DeleteObjectRequest(((options.isDelete()) ? options.getSourceBucket() : options.getDestinationBucket()), key);

            if (options.isDryRun()) {
                log.info("Would have deleted "+key+" from destination because "+keysrc+" does not exist in source");
            } else {
                boolean deletedOK = false;
                for (int tries=0; tries<maxRetries; tries++) {
                    if (verbose) log.info("deleting (try #"+tries+"): "+key);
                    try {
                        stats.s3deleteCount.incrementAndGet();
                        client.deleteObject(request);
                        deletedOK = true;
                        if (verbose) log.info("successfully deleted (on try #"+tries+"): "+key);
                        break;

                    } catch (AmazonS3Exception s3e) {
                        log.error("s3 exception deleting (try #"+tries+") "+key+": "+s3e);

                    } catch (Exception e) {
                        log.error("unexpected exception deleting (try #"+tries+") "+key+": "+e);
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        log.error("interrupted while waiting to retry key: "+key);
                        break;
                    }
                }
                if (deletedOK) {
                    context.getStats().objectsDeleted.incrementAndGet();
                } else {
                    context.getStats().deleteErrors.incrementAndGet();
                }
            }

        } catch (Exception e) {
            log.error("error deleting key: "+key+": "+e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (verbose) log.info("done with "+key);
        }
        return true;
    }

    @Override
    protected boolean shouldAction() {

        final S3ToS3Options options = context.getOptions();
        final boolean verbose = options.isVerbose();

        // Does it exist in the source bucket
        if (options.isDelete() && options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + keysrc);

            } else {
                if (options.isYounger() && lastModified.getTime() < options.getAge()) {
                    if (verbose)
                        log.info("key " + keysrc + " (lastmod=" + lastModified + ") is older than " + options.getCtime() + " (cutoff=" + options.getAgeDate() + "), not copying");
                    return false;
                } else if (!options.isYounger() && lastModified.getTime() > options.getAge()) {
                    if (verbose)
                        log.info("key " + keysrc + " (lastmod=" + lastModified + ") is younger than " + options.getCtime() + " (cutoff=" + options.getAgeDate() + "), not copying");
                    return false;
                }
            }
        }

        try {
            ObjectMetadata metadata = getObjectMetadata(options.getSourceBucket(), keysrc, options);
            return context.getOptions().isDelete(); // object exists in source bucket, don't delete it from destination bucket

        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in source bucket (will delete from destination): "+ keysrc);
                return true;
            } else {
                log.warn("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getSourceBucket() + "/" + keysrc + " (not deleting): " + e);
            return false;
        }
    }

}
