package org.cobbzilla.s3s3util;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class CopyMaster extends KeyMaster {

    public CopyMaster(AmazonS3Client client, S3ToS3Context context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(client, context, workQueue, executorService);
    }

    protected String getPrefix(S3ToS3Options options) { return options.getPrefix(); }
    protected String getBucket(S3ToS3Options options) { return options.getSourceBucket(); }

    protected BaseKeyJob getTask(S3ObjectSummary summary) {
        if (context.getOptions().isDelete())
            return new KeyDeleteJob(client, context, summary, notifyLock);
        if (summary.getSize() > S3ToS3Options.MAX_SINGLE_REQUEST_UPLOAD_FILE_SIZE) {
            return ((context.getOptions().isMove()))? new MultipartKeyMoveJob(client, context, summary, notifyLock):new MultipartKeyCopyJob(client, context, summary, notifyLock);
         }
        return ((context.getOptions().isMove()))? new KeyMoveJob(client, context, summary, notifyLock):new KeyCopyJob(client, context, summary, notifyLock);
    }
}
