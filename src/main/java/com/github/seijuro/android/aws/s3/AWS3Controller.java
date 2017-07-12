package com.github.seijuro.android.aws.s3;

import android.content.Context;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.mobileconnectors.s3.transferutility.TransferListener;
import com.amazonaws.mobileconnectors.s3.transferutility.TransferObserver;
import com.amazonaws.mobileconnectors.s3.transferutility.TransferUtility;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by seijuro
 */

public class AWS3Controller {
    /**
     * Singleton instance
     */
    static AWS3Controller instance = null;

    /**
     * Singleton pattern method.
     *
     * @return
     */
    synchronized public static AWS3Controller getInstance() {
        if (instance == null) {
            instance = new AWS3Controller();
        }

        return instance;
    }


    /**
     * Instance Properties
     */
    private boolean initialized = false;
    private AmazonS3 s3Client = null;
    private ExecutorService executor = null;
    private Context context;

    protected boolean isInitialized() {
        return this.initialized;
    }

    protected void throwExceptionIfNotInitialized() throws Exception {
        if (this.initialized) { throw new Exception(AWS3Controller.class.getCanonicalName() + " isn't initialized."); }
    }

    /**
     * [Caution] Must initialize controller using this method before using controller
     *
     * @param appContext
     * @param executor
     * @param credentials
     * @throws Exception
     */
    protected void init(Context appContext, ExecutorService executor, AWSCredentialsProvider credentials, Regions regions) throws Exception {
        this.executor = executor;
        this.s3Client = new AmazonS3Client(credentials);
        this.s3Client.setRegion(Region.getRegion(regions));

        initialized = true;
    }

    /**
     * [Caution] Must initialize controller using this method before using controller
     *
     * @param appContext
     * @param credentials
     * @throws Exception
     */
    protected void init(Context appContext, AWSCredentialsProvider credentials, Regions regions) throws Exception {
        init(appContext, Executors.newSingleThreadExecutor(), credentials, regions);
    }

    /**
     *
     * @param bucket
     * @param key
     * @param filepath
     * @param listener
     */
    public void download(String bucket, String key, String filepath, TransferListener listener) throws Exception {
        throwExceptionIfNotInitialized();

        File file = new File(filepath);
        if (file.exists()) {
            if (!file.canWrite() && !file.setWritable(true)) {
                throw new Exception(String.format("File(%s) exists, but couldn't write on it.", filepath));
            }
        }
        else if (file.createNewFile()) {
            if (file.setWritable(true)) {
                throw new Exception(String.format("File(%s) is created, but couldn't write on it.", filepath));
            }
        }
        else {
            throw new Exception(String.format("check Filepath(%s)", filepath));
        }

        TransferUtility transferUtility = new TransferUtility(s3Client, this.context);
        TransferObserver observer = transferUtility.download(bucket, key, file);

        if (listener != null) { observer.setTransferListener(listener); }
    }
}
