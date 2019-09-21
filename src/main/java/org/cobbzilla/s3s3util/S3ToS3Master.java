package org.cobbzilla.s3s3util;

import com.amazonaws.services.s3.AmazonS3Client;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * Manages the Starts a KeyLister and sends batches of keys to the ExecutorService for handling by KeyJobs
 */
@Slf4j
public class S3ToS3Master {

    public static final String VERSION = System.getProperty("s3tos3util.version");

    private final AmazonS3Client client;
    private final S3ToS3Context context;

    public S3ToS3Master(AmazonS3Client client, S3ToS3Context context) {
        this.client = client;
        this.context = context;
    }

    public void mirror() {

        log.info("version "+VERSION+" starting");

        final S3ToS3Options options = context.getOptions();

        if (options.isVerbose() && options.hasCtime())
            log.info("will not copy anything "+((options.isYounger())?"older":"younger") + "than "+options.getCtime()+" (cutoff="+options.getAgeDate()+")");

        final int maxQueueCapacity = getMaxQueueCapacity(options);
        final BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(maxQueueCapacity);
        final RejectedExecutionHandler rejectedExecutionHandler = new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                log.error("Error submitting job: "+r+", possible queue overflow");
            }
        };

        final ThreadPoolExecutor executorService = new ThreadPoolExecutor(options.getMaxThreads(), options.getMaxThreads(), 1, TimeUnit.MINUTES, workQueue, rejectedExecutionHandler);

        final KeyMaster copyMaster = new CopyMaster(client, context, workQueue, executorService);
        KeyMaster deleteMaster = null;

        try {
            copyMaster.start();

            if (!context.getOptions().isDelete() && context.getOptions().isDeleteRemoved()) {
                deleteMaster = new DeleteMaster(client, context, workQueue, executorService);
                deleteMaster.start();
            }

            while (true) {
                if (copyMaster.isDone() && (deleteMaster == null || deleteMaster.isDone())) {
                    log.info("mirror: completed");
                    break;
                }
                if (Sleep.sleep(100)) return;
            }

        } catch (Exception e) {
            log.error("Unexpected exception in mirror: "+e, e);

        } finally {
            try { copyMaster.stop();   } catch (Exception e) { log.error("Error stopping copyMaster: "+e, e); }
            if (deleteMaster != null) {
                try { deleteMaster.stop(); } catch (Exception e) { log.error("Error stopping deleteMaster: "+e, e); }
            }
        }
    }

    public static int getMaxQueueCapacity(S3ToS3Options options) {
        long freeMem = Runtime.getRuntime().freeMemory();
        long maxMem = Runtime.getRuntime().maxMemory();
        int maxCapacity = Math.max(10 * options.getMaxThreads(), (int) (freeMem * 0.40) / 20);
        log.info("Total Free available memory {}", freeMem);
        log.info("Max Queue Capacity {}", maxCapacity);
        return maxCapacity;
    }

}
