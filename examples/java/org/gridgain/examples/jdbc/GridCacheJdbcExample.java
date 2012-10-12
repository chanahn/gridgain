// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.jdbc;

import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.typedef.*;

import java.io.*;
import java.sql.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * This example shows how to use GridGain JDBC driver. It populates cache with
 * sample data, opens JDBC connection with cache and executes several queries.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/spring-cache.xml'}.
 * <h2 class="header">Limitations</h2>
 * Data in GridGain cache is usually distributed across several nodes,
 * so some queries may not work as expected. Keep in mind following limitations
 * (not applied if data is queried from one node only):
 * <ul>
 *     <li>
 *         {@code Group by} and {@code sort by} statements are applied separately
 *         on each node, so result set will likely be incorrectly grouped or sorted
 *         after results from multiple remote nodes are grouped together.
 *     </li>
 *     <li>
 *         Aggregation functions like {@code sum}, {@code max}, {@code avg}, etc.
 *         are also applied on each node. Therefore you will get several results
 *         containing aggregated values, one for each node.
 *     </li>
 *     <li>
 *         Joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to
 *         {@link org.gridgain.grid.cache.affinity.GridCacheAffinityKey}
 *         javadoc for more details.
 *     </li>
 *     <li>
 *         Note that if you are connected to local or replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you are connected to partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <h2 class="header">SQL Notice</h2>
 * Driver allows to query data from several caches. Cache that driver is connected to is
 * treated as default schema in this case. Other caches can be referenced by their names.
 * <p>
 * Note that cache name is case sensitive and you have to always specify it in quotes.
 *
 * @author @java.author
 * @version @java.version
 */
@GridOnlyAvailableIn(GridEdition.BIG_DATA)
public class GridCacheJdbcExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";
    //private static final String CACHE_NAME = "replicated";
    //private static final String CACHE_NAME = "local";

    /**
     * Runs JDBC example.
     *
     * @param args Command line arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        Grid grid = G.start("examples/config/spring-cache.xml");

        Connection conn = null;

        try {
            // Get cache by name.
            GridCache<Object, Object> cache = grid.cache(CACHE_NAME);

            // Populate cache with data.
            populate(cache);

            // Register JDBC driver.
            Class.forName("org.gridgain.jdbc.GridJdbcDriver");

            String url = "jdbc:gridgain://localhost/" + CACHE_NAME;

            // Append local node ID as an URL parameter when querying local cache,
            // as we should not include remote nodes when working with local caches.
            if (cache.configuration().getCacheMode() == LOCAL)
                url += "?nodeId=" + grid.localNode().id();

            // Open JDBC connection.
            conn = DriverManager.getConnection(url, configuration());

            X.println(">>>");

            // Query all persons.
            queryAllPersons(conn);

            X.println(">>>");

            // Query person older than 30 years.
            queryPersons(conn, 30);

            X.println(">>>");

            // Query persons working in GridGain.
            queryPersonsInOrganization(conn, "GridGain");

            X.println(">>>");
        }
        finally {
            // Close JDBC connection.
            if (conn != null)
                conn.close();

            G.stop(true);
        }
    }

    /**
     * Creates configuration properties object for new connection.
     * <p>
     * See GridGain client javadoc for more information: {@link GridClientConfiguration}.
     * <p>
     * All parameters are optional.
     *
     * @return Configuration.
     */
    private static Properties configuration() {
        Properties cfg = new Properties();

        // Node ID where to execute query. This property is useful when you have several
        // local caches with same name in topology and want to specify which of them to connect.
        //
        // Uncomment line below and provide correct ID if needed.
        // cfg.setProperty("gg.jdbc.nodeId", "E0869485-512C-41F9-866D-BE906B591BEA");

        // Communication protocol (TCP or HTTP). Default is TCP.
        cfg.setProperty("gg.client.protocol", "TCP");

        // Socket timeout. Default is 0 which means infinite timeout.
        cfg.setProperty("gg.client.connectTimeout", "0");

        // Flag indicating whether TCP_NODELAY flag should be enabled for outgoing
        // connections. Default is true.
        cfg.setProperty("gg.client.tcp.noDelay", "true");

        // Flag indicating that SSL is needed for connection. Default is false.
        // cfg.setProperty("gg.client.ssl.enabled", "false");

        // SSL protocol.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.protocol", "TLS");

        // Key manager algorithm.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.key.algorithm", "SunX509");

        // Key store to be used by client to connect with GridGain topology.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.keystore.location", "/path/to/keystore");

        // Key store password.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.keystore.password", "s3cr3t");

        // Key store type.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.keystore.type", "jks");

        // Trusty store to be used by client to connect with GridGain topology.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.truststore.location", "/path/to/truststore");

        // Trust store password.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.truststore.password", "s3cr3t");

        // Trust store type.
        // Ignored is SSL is disabled.
        // cfg.setProperty("gg.client.ssl.truststore.type", "jks");

        // Client credentials used in authentication process.
        // cfg.setProperty("gg.client.credentials", "s3cr3t");

        // Flag indicating that topology is cached internally. Cache will be refreshed
        // in the background with interval defined by CONF_TOP_REFRESH_FREQ property
        // (see below). Default is false.
        cfg.setProperty("gg.client.cacheTop", "false");

        // Topology cache refresh frequency. Default is 2000 ms.
        cfg.setProperty("gg.client.topology.refresh", "2000");

        // Maximum amount of time that connection can be idle before it is closed.
        // Default is 30000 ms.
        cfg.setProperty("gg.client.idleTimeout", "30000");

        return cfg;
    }

    /**
     * Populates cache with test data.
     *
     * @param cache Cache.
     * @throws GridException In case of error.
     */
    private static void populate(GridCache<Object, Object> cache) throws GridException {
        cache.put("o1", new Organization(1, "GridGain"));
        cache.put("o2", new Organization(2, "Other"));

        // Persons are collocated with their organizations to support joins.
        cache.put(new GridCacheAffinityKey<String>("p1", "o1"), new Person(1, "John White", 25, 1));
        cache.put(new GridCacheAffinityKey<String>("p2", "o1"), new Person(2, "Joe Black", 35, 1));
        cache.put(new GridCacheAffinityKey<String>("p3", "o2"), new Person(3, "Mike Green", 40, 2));
    }

    /**
     * Queries all persons and shows their names.
     *
     * @param conn JDBC connection.
     * @throws SQLException In case of SQL error.
     */
    private static void queryAllPersons(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("select name from Person");

        X.println(">>> All persons:");

        while (rs.next())
            X.println(">>>     " + rs.getString(1));
    }

    /**
     * Queries persons older than provided age.
     *
     * @param conn JDBC connection.
     * @param minAge Minimum age.
     * @throws SQLException In case of SQL error.
     */
    private static void queryPersons(Connection conn, int minAge) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("select name, age from Person where age >= ?");

        stmt.setInt(1, minAge);

        ResultSet rs = stmt.executeQuery();

        X.println(">>> Persons older than " + minAge + ":");

        while (rs.next())
            X.println(">>>     " + rs.getString("NAME") + " (" + rs.getInt("AGE") + " years old)");
    }

    /**
     * Queries persons working in provided organization.
     *
     * @param conn JDBC connection.
     * @param orgName Organization name.
     * @throws SQLException In case of SQL error.
     */
    private static void queryPersonsInOrganization(Connection conn, String orgName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(
            "select p.name from Person p, Organization o where p.orgId = o.id and o.name = ?");

        stmt.setString(1, orgName);

        ResultSet rs = stmt.executeQuery();

        X.println(">>> Persons working in " + orgName + ":");

        while (rs.next())
            X.println(">>>     " + rs.getString(1));
    }

    /**
     * Person.
     *
     * @author @java.author
     * @version @java.version
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Person implements Serializable {
        /** ID. */
        @GridCacheQuerySqlField
        private final int id;

        /** Name. */
        @GridCacheQuerySqlField(index = false)
        private final String name;

        /** Age. */
        @GridCacheQuerySqlField
        private final int age;

        /** Organization ID. */
        @GridCacheQuerySqlField
        private final int orgId;

        /**
         * @param id ID.
         * @param name Name.
         * @param age Age.
         * @param orgId Organization ID.
         */
        private Person(int id, String name, int age, int orgId) {
            assert !F.isEmpty(name);
            assert age > 0;
            assert orgId > 0;

            this.id = id;
            this.name = name;
            this.age = age;
            this.orgId = orgId;
        }
    }

    /**
     * Organization.
     *
     * @author @java.author
     * @version @java.version
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class Organization implements Serializable {
        /** ID. */
        @GridCacheQuerySqlField
        private final int id;

        /** Name. */
        @GridCacheQuerySqlField(index = false)
        private final String name;

        /**
         * @param id ID.
         * @param name Name.
         */
        private Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
