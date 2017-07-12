package com.github.seijuro.android.aws.ddb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.mobileconnectors.dynamodbv2.dynamodbmapper.DynamoDBMapper;
import com.amazonaws.mobileconnectors.dynamodbv2.dynamodbmapper.DynamoDBScanExpression;
import com.amazonaws.mobileconnectors.dynamodbv2.dynamodbmapper.PaginatedScanList;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by seijuro
 */

public class DynamoDBController {
    static final int DEFAULT_THREADPOOL_SIZE = 5;

    /**
     * Scanning result listener
     *
     * @param <T>
     */
    public interface ScanListener<T> {
        public void received(PaginatedScanList<T> list);
    }

    static final class ScanRequest<T> implements Runnable {
        private ScanListener<T> listener = null;
        private Class<T> type;

        public ScanRequest(Class<T> $type, ScanListener<T> $listener) {
            assert ($listener != null);

            this.type = $type;
            this.listener = $listener;
        }

        @Override
        public void run() {
            try {
                DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();
                PaginatedScanList<T> result = mapper.scan(this.type, scanExpression);

                this.listener.received(result);

                // release reference(s)
                result = null;
                this.listener = null;
            }
            catch (Exception excp) {
                excp.printStackTrace();
            }
        }
    }

    /**
     * Singleton Instance
     */
    static DynamoDBController instance = null;
    static DynamoDBMapper mapper = null;

    public static DynamoDBController getInstance() throws Exception {
        try {
            synchronized (DynamoDBController.class) {
                if (instance == null) {
                    instance = new DynamoDBController();
                }

                return instance;
            }
        }
        catch (Exception excp) {
            excp.printStackTrace();

            throw excp;
        }
    }

    /**
     * Instance Property
     */
    private boolean initialized = false;
    private AmazonDynamoDBClient ddbClient = null;
    private ExecutorService executor = null;

    protected boolean isInitialized() {
        return this.initialized;
    }

    protected void throwExceptionIfNotInitialized() throws Exception {
        if (this.initialized) { throw new Exception(DynamoDBController.class.getCanonicalName() + " isn't initialized."); }
    }

    /**
     * [Caution] Must initialize controller using this method before using controller
     *
     * @param executor
     * @param credentials
     * @throws Exception
     */
    protected void init(ExecutorService executor, AWSCredentialsProvider credentials) throws Exception {
        this.executor = executor;
        this.ddbClient = new AmazonDynamoDBClient(credentials);
        this.mapper = new DynamoDBMapper(ddbClient);

        initialized = true;
    }

    /**
     * [Caution] Must initialize controller using this method before using controller
     *
     * @param credentials
     * @throws Exception
     */
    protected void init(AWSCredentialsProvider credentials) throws Exception {
        init(Executors.newFixedThreadPool(DEFAULT_THREADPOOL_SIZE), credentials);
    }

    /**
     * @brief
     *  request to scan table
     *  This method will invoke background thread.
     *  Calling this method on main/UI thread doesn't matter.
     *
     * @param type
     * @param listener
     * @param <T>
     * @throws Exception
     */
    public <T> void scan(Class<T> type, ScanListener<T> listener) throws Exception {
        throwExceptionIfNotInitialized();

        ScanRequest<T> request = new ScanRequest<T>(type, listener);
        this.executor.execute(request);
    }
}
