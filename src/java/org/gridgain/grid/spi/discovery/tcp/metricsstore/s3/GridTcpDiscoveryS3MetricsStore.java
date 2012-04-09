// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.s3;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * AWS S3-based metrics store.
 * <p>
 * For information about Amazon S3 visit <a href="http://aws.amazon.com">aws.amazon.com</a>.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * <ul>
 *      <li>AWS credentials (see {@link #setAwsCredentials(AWSCredentials)})</li>
 * </ul>
 * <h2 class="header">Optional</h2>
 * <ul>
 *      <li>Client configuration (see {@link #setClientConfiguration(ClientConfiguration)})</li>
 *      <li>Bucket name suffix (see {@link #setBucketNameSuffix(String)})</li>
 *      <li>Metrics expire time (see {@link #setMetricsExpireTime(int)})</li>
 * </ul>
 * <p>
 * The store will create S3 bucket. The name is built in the following way:
 * {@link #BUCKET_NAME_PREFIX} + suffix provided by user (see {@link #setBucketNameSuffix(String)}).
 * If suffix is not provided by user, then {@link #DFLT_BUCKET_NAME_SUFFIX} is used.
 * <p>
 * The bucket will contain entries with serialized metrics named like the following
 * 94816A59-EB51-44EE-BB67-8B24B9C10A09.
 * <p>
 * Note that storing data in AWS S3 service will result in charges to your AWS account.
 * Choose another implementation of {@link GridTcpDiscoveryMetricsStore} for local
 * or home network tests.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.09042012
 */
public class GridTcpDiscoveryS3MetricsStore extends GridTcpDiscoveryMetricsStoreAdapter {
    /** Prefix to use in bucket name generation. */
    public static final String BUCKET_NAME_PREFIX = "gridgain-tcp-metrics-store-";

    /** Suffix to use in bucket name generation. */
    public static final String DFLT_BUCKET_NAME_SUFFIX = "default-bucket";

    /** Entry metadata with content length set. */
    private static final ObjectMetadata ENTRY_METADATA;

    static {
        ENTRY_METADATA = new ObjectMetadata();

        ENTRY_METADATA.setContentLength(GridDiscoveryMetricsHelper.METRICS_SIZE);
    }

    /** Grid logger. */
    @GridLoggerResource
    private GridLogger log;

    /** Client to interact with S3 storage. */
    @GridToStringExclude
    private AmazonS3 s3;

    /** Bucket name suffix (set by user). */
    private String bucketNameSuffix;

    /** Bucket name (generated). */
    private String bucketName;

    /** Init guard. */
    @GridToStringExclude
    private final AtomicBoolean initGuard = new AtomicBoolean();

    /** Init latch. */
    @GridToStringExclude
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Amazon client configuration. */
    private ClientConfiguration cfg;

    /** AWS Credentials. */
    @GridToStringExclude
    private AWSCredentials cred;

    /** {@inheritDoc} */
    @Override public void updateLocalMetrics(UUID locNodeId, GridNodeMetrics metrics) throws GridSpiException {
        assert locNodeId != null;
        assert metrics != null;

        initClient();

        try {
            byte res[] = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

            GridDiscoveryMetricsHelper.serialize(res, 0, metrics);

            s3.putObject(bucketName, locNodeId.toString(), new ByteArrayInputStream(res),
                ENTRY_METADATA);
        }
        catch (AmazonClientException e) {
            throw new GridSpiException("Failed to put entry [bucketName=" + bucketName +
                ", entry=" + locNodeId.toString() + ']', e);
        }

    }

    /** {@docRoot} */
    @Override protected Map<UUID, GridNodeMetrics> metrics0(Collection<UUID> nodeIds) throws GridSpiException {
        assert !F.isEmpty(nodeIds);

        initClient();

        Map<UUID, GridNodeMetrics> res = new HashMap<UUID, GridNodeMetrics>();

        try {
            ObjectListing list = s3.listObjects(bucketName);

            while (true) {
                for (S3ObjectSummary sum : list.getObjectSummaries()) {
                    UUID id = UUID.fromString(sum.getKey());

                    if (!nodeIds.contains(id))
                        continue;

                    InputStream in = null;

                    try {
                        in = s3.getObject(bucketName, sum.getKey()).getObjectContent();

                        byte[] buf = new byte[GridDiscoveryMetricsHelper.METRICS_SIZE];

                        in.read(buf);

                        res.put(id, GridDiscoveryMetricsHelper.deserialize(buf, 0));
                    }
                    catch (IllegalArgumentException ignored) {
                        U.warn(log, "Failed to parse UUID from entry key: " + sum.getKey());
                    }
                    catch (IOException e) {
                        U.error(log, "Failed to get entry content [bucketName=" + bucketName +
                            ", entry=" + id.toString() + ']', e);
                    }
                    finally {
                        U.closeQuiet(in);
                    }
                }

                if (list.isTruncated())
                    list = s3.listNextBatchOfObjects(list);
                else
                    break;
            }
        }
        catch (AmazonClientException e) {
            throw new GridSpiException("Failed to list objects in the bucket: " + bucketName, e);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> allNodeIds() throws GridSpiException {
        initClient();

        Collection<UUID> res = new LinkedList<UUID>();

        try {
            ObjectListing list = s3.listObjects(bucketName);

            while (true) {
                for (S3ObjectSummary sum : list.getObjectSummaries())
                    try {
                        UUID id = UUID.fromString(sum.getKey());

                        res.add(id);
                    }
                    catch (IllegalArgumentException ignored) {
                        U.warn(log, "Failed to parse UUID from entry key: " + sum.getKey());
                    }

                if (list.isTruncated())
                    list = s3.listNextBatchOfObjects(list);
                else
                    break;
            }
        }
        catch (AmazonClientException e) {
            throw new GridSpiException("Failed to list objects in the bucket: " + bucketName, e);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void removeMetrics0(Collection<UUID> nodeIds) throws GridSpiException {
        assert !F.isEmpty(nodeIds);

        initClient();

        for (UUID id : nodeIds) {
            try {
                s3.deleteObject(bucketName, id.toString());
            }
            catch (AmazonClientException e) {
                throw new GridSpiException("Failed to delete entry [bucketName=" + bucketName +
                    ", entry=" + id.toString() + ']', e);
            }
        }
    }

    /**
     * Amazon s3 client initialization.
     *
     * @throws GridSpiException In case of error.
     */
    @SuppressWarnings({"BusyWait"})
    private void initClient() throws GridSpiException {
        if (initGuard.compareAndSet(false, true))
            try {
                if (cred == null)
                    throw new GridSpiException("AWS credentials are not set.");

                if (cfg == null)
                    U.warn(log, "Amazon client configuration is not set (will use default).");

                if (F.isEmpty(bucketNameSuffix)) {
                    U.warn(log, "Bucket name suffix is null or empty (will use default bucket name).");

                    bucketName = BUCKET_NAME_PREFIX + DFLT_BUCKET_NAME_SUFFIX;
                }
                else
                    bucketName = BUCKET_NAME_PREFIX + bucketNameSuffix;

                s3 = cfg != null ? new AmazonS3Client(cred, cfg) : new AmazonS3Client(cred);

                if (!s3.doesBucketExist(bucketName)) {
                    try {
                        s3.createBucket(bucketName);

                        if (log.isDebugEnabled())
                            log.debug("Created S3 bucket: " + bucketName);

                        while (!s3.doesBucketExist(bucketName))
                            try {
                                Thread.sleep(200);
                            }
                            catch (InterruptedException e) {
                                throw new GridSpiException("Thread has been interrupted.", e);
                            }
                    }
                    catch (AmazonClientException e) {
                        if (!s3.doesBucketExist(bucketName)) {
                            s3 = null;

                            throw new GridSpiException("Failed to create bucket: " + bucketName, e);
                        }
                    }
                }
            }
            finally {
                initLatch.countDown();
            }
        else {
            try {
                initLatch.await();
            }
            catch (InterruptedException e) {
                throw new GridSpiException("Thread has been interrupted.", e);
            }

            if (s3 == null)
                throw new GridSpiException("Metrics store has not been properly initialized.");
        }
    }

    /**
     * Sets bucket name suffix.
     *
     * @param bucketNameSuffix Bucket name suffix.
     */
    @GridSpiConfiguration(optional = true)
    public void setBucketNameSuffix(String bucketNameSuffix) {
        this.bucketNameSuffix = bucketNameSuffix;
    }

    /**
     * Sets Amazon client configuration.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cfg Amazon client configuration.
     */
    @GridSpiConfiguration(optional = true)
    public void setClientConfiguration(ClientConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Sets AWS credentials.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cred AWS credentials.
     */
    @GridSpiConfiguration(optional = false)
    public void setAwsCredentials(AWSCredentials cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryS3MetricsStore.class, this);
    }
}
