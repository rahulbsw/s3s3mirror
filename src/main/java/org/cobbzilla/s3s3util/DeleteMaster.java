package org.cobbzilla.s3s3util;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class DeleteMaster extends KeyMaster {

    public DeleteMaster(AmazonS3Client client, S3ToS3Context context, BlockingQueue<Runnable> workQueue, ThreadPoolExecutor executorService) {
        super(client, context, workQueue, executorService);
    }

    protected String getPrefix(S3ToS3Options options) {
        return options.hasDestPrefix() ? options.getDestPrefix() : options.getPrefix();
    }

    protected String getBucket(S3ToS3Options options) { return options.getDestinationBucket(); }

    @Override
    protected KeyJob getTask(S3ObjectSummary summary) {
        return new KeyDeleteJob(client, context, summary, notifyLock);
    }
}
