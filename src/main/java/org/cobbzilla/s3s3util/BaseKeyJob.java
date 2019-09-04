package org.cobbzilla.s3s3util;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.Date;

@Slf4j
abstract public class BaseKeyJob extends KeyJob {
    protected String keydest;

    public BaseKeyJob(AmazonS3Client client, S3ToS3Context context, S3ObjectSummary summary, Object notifyLock) {
        super(client, context, summary, notifyLock);
        keydest = summary.getKey();
        final S3ToS3Options options = context.getOptions();
        if (options.hasDestPrefix()) {
            keydest = keydest.substring(options.getPrefixLength());
            keydest = options.getDestPrefix() + keydest;
        }

    }
    @Override public Logger getLog() { return log; }

    abstract boolean execute(ObjectMetadata sourceMetadata, AccessControlList objectAcl);

    @Override
    public void run() {
        final S3ToS3Options options = context.getOptions();
        final String key = summary.getKey();
        try {
            if (!shouldTransfer()) return;
            final ObjectMetadata sourceMetadata = getObjectMetadata(options.getSourceBucket(), key, options);
            final AccessControlList objectAcl = getAccessControlList(options, key);

            if (options.isDryRun()) {
                log.info("Would have copied " + key + " to destination: " + keydest);
            } else {
                if (execute(sourceMetadata, objectAcl)) {
                    context.getStats().objectsCopied.incrementAndGet();
                } else {
                    context.getStats().copyErrors.incrementAndGet();
                }
            }
        } catch (Exception e) {
            log.error("error copying key: " + key + ": " + e);

        } finally {
            synchronized (notifyLock) {
                notifyLock.notifyAll();
            }
            if (options.isVerbose()) log.info("done with " + key);
        }
    }

    private boolean shouldTransfer() {
        final S3ToS3Options options = context.getOptions();
        final String key = summary.getKey();
        final boolean verbose = options.isVerbose();

        if (options.hasCtime()) {
            final Date lastModified = summary.getLastModified();
            if (lastModified == null) {
                if (verbose) log.info("No Last-Modified header for key: " + key);

            } else {
                if (options.isYounger() && lastModified.getTime() < options.getAge()) {
                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is older than "+options.getCtime()+" (cutoff="+options.getAgeDate()+"), not copying");
                    return false;
                }
                else if(!options.isYounger() && lastModified.getTime() > options.getAge()) {
                    if (verbose) log.info("key "+key+" (lastmod="+lastModified+") is younger than "+options.getCtime()+" (cutoff="+options.getAgeDate()+"), not copying");
                    return false;
                }
            }
        }
        final ObjectMetadata metadata;
        try {
            metadata = getObjectMetadata(options.getDestinationBucket(), keydest, options);
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                if (verbose) log.info("Key not found in destination bucket (will copy): "+ keydest);
                return true;
            } else {
                log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
                return false;
            }
        } catch (Exception e) {
            log.warn("Error getting metadata for " + options.getDestinationBucket() + "/" + keydest + " (not copying): " + e);
            return false;
        }

        if (summary.getSize() > S3ToS3Options.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return metadata.getContentLength() != summary.getSize();
        }
        final boolean objectChanged = objectChanged(metadata);
        if (verbose && !objectChanged) log.info("Destination file is same as source, not copying: "+ key);

        return objectChanged;
    }

    boolean objectChanged(ObjectMetadata metadata) {
        final S3ToS3Options options = context.getOptions();
        final KeyFingerprint sourceFingerprint;
        final KeyFingerprint destFingerprint;

        if (options.isSizeOnly()) {
            sourceFingerprint = new KeyFingerprint(summary.getSize());
            destFingerprint = new KeyFingerprint(metadata.getContentLength());
        } else {
            sourceFingerprint = new KeyFingerprint(summary.getSize(), summary.getETag());
            destFingerprint = new KeyFingerprint(metadata.getContentLength(), metadata.getETag());
        }

        return !sourceFingerprint.equals(destFingerprint);
    }
}
