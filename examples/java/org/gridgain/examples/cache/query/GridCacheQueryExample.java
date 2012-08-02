// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.cache.query;

import org.gridgain.examples.cache.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.editions.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.*;

import java.util.*;

import static org.gridgain.grid.cache.query.GridCacheQueryType.*;

/**
 * Grid cache queries example. This example demonstrates SQL, TEXT, and FULL SCAN
 * queries over cache.
 * <p>
 * Example also demonstrates usage of fields queries that return only required
 * fields instead of whole key-value pairs. When fields queries are distributed
 * across several nodes, they may not work as expected. Keep in mind following
 * limitations (not applied if data is queried from one node only):
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
 *         Note that if you created query on to local or replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you created it on partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * cache: {@code 'ggstart.sh examples/config/spring-cache.xml'}.
 *
 * @author @java.author
 * @version @java.version
 */
@GridNotAvailableIn(GridEdition.COMPUTE_GRID)
public class GridCacheQueryExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";
    // private static final String CACHE_NAME = "replicated";

    /** Grid instance. */
    private static Grid grid;

    /**
     * Put data to cache and then queries them.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        grid = args.length == 0 ? G.start("examples/config/spring-cache.xml") : G.start(args[0]);

        try {
            print("Query example started.");

            // Populate cache.
            initialize();

            // Using distributed queries in this example.
            GridProjection p = grid;

            // Example for SQL-based querying employees based on salary ranges.
            querySalaries(p);

            // Example for SQL-based querying employees for a given organization (includes SQL join).
            queryEmployees(p);

            // Example for TEXT-based querying for a given string in peoples resumes.
            queryDegree(p);

            // Example for SQL-based querying with custom remote and local reducers
            // to calculate average salary among all employees within a company.
            queryAverageSalary(p);

            // Example for SQL-based querying with custom remote transformer to make sure
            // that only required data without any overhead is returned to caller.
            queryEmployeeNames(p);

            // Example for SQL-based fields queries that return only required
            // fields instead of whole key-value pairs.
            queryFields(p);

            print("Query example finished.");
        }
        finally {
            //GridFactory.stop(true);
        }
    }

    /**
     * Gets instance of cache to use.
     *
     * @return Cache to use.
     */
    private static <K, V> GridCacheProjection<K, V> cache() {
        return grid.cache(CACHE_NAME);
    }

    /**
     * Example for SQL queries based on salary ranges.
     *
     * @param p Grid projection to run query on.
     */
    private static void querySalaries(GridProjection p) {
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> cache = cache();

        // Create query which selects salaries based on range.
        GridCacheQuery<GridCacheAffinityKey<UUID>, Person> qry =
            cache.createQuery(SQL, Person.class, "salary > ? and salary <= ?");

        // Execute queries for salary ranges.
        print("People with salaries between 0 and 1000: ",
            qry.queryArguments(0, 1000).execute(p));

        print("People with salaries between 1000 and 2000: ",
            qry.queryArguments(1000, 2000).execute(p));

        print("People with salaries greater than 2000: ",
            qry.queryArguments(2000, Integer.MAX_VALUE).execute(p));
    }

    /**
     * Example for SQL queries based on all employees working for a specific organization.
     *
     * @param p Grid projection to run query on.
     */
    private static void queryEmployees(GridProjection p) {
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> cache = cache();

        // Create query which joins on 2 types to select people for a specific organization.
        GridCacheQuery<GridCacheAffinityKey<UUID>, Person> qry =
            cache.createQuery(SQL, Person.class,
                "from Person, Organization " +
                    "where Person.orgId = Organization.id and lower(Organization.name) = lower(?)");

        // Execute queries for find employees for different organizations.
        print("Following people are 'GridGain' employees: ",
            qry.queryArguments("GridGain").execute(p));

        print("Following people are 'Other' employees: ",
            qry.queryArguments("Other").execute(p));
    }

    /**
     * Example for TEXT queries using LUCENE-based indexing of people's resumes.
     *
     * @param p Grid projection to run query on.
     */
    private static void queryDegree(GridProjection p) {
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> cache = cache();

        //  Query for all people with "Master Degree" in their resumes.
        GridCacheQuery<GridCacheAffinityKey<UUID>, Person> masters =
            cache.createQuery(TEXT, Person.class, "Master");

        // Query for all people with "Bachelor Degree"in their resumes.
        GridCacheQuery<GridCacheAffinityKey<UUID>, Person> bachelors =
            cache.createQuery(TEXT, Person.class, "Bachelor");

        print("Following people have 'Master Degree' in their resumes: ", masters.execute(p));

        print("Following people have 'Bachelor Degree' in their resumes: ", bachelors.execute(p));
    }

    /**
     * Example for SQL queries with custom remote and local reducers to calculate
     * average salary for a specific organization.
     *
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryAverageSalary(GridProjection p) throws GridException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> cache = cache();

        // Calculate average of salary of all persons in GridGain.
        GridCacheReduceQuery<GridCacheAffinityKey<UUID>, Person, GridTuple2<Double, Integer>, Double> qry =
            cache.createReduceQuery(SQL, Person.class,
                "from Person, Organization " +
                    "where Person.orgId = Organization.id and lower(Organization.name) = lower(?)");

        // Calculate sum of salaries and employee count on remote nodes.
        qry.remoteReducer(new C1<Object[], GridReducer<Map.Entry<GridCacheAffinityKey<UUID>, Person>,
            GridTuple2<Double, Integer>>>() {
            private GridReducer<Map.Entry<GridCacheAffinityKey<UUID>, Person>, GridTuple2<Double, Integer>> rdc =
                new GridReducer<Map.Entry<GridCacheAffinityKey<UUID>, Person>, GridTuple2<Double, Integer>>() {
                    private double sum;
                    private int cnt;

                    @Override public boolean collect(Map.Entry<GridCacheAffinityKey<UUID>, Person> e) {
                        sum += e.getValue().getSalary();

                        cnt++;

                        // Continue collecting.
                        return true;
                    }

                    @Override public GridTuple2<Double, Integer> apply() {
                        return F.t(sum, cnt);
                    }
                };

                @Override public GridReducer<Map.Entry<GridCacheAffinityKey<UUID>, Person>,
                    GridTuple2<Double, Integer>> apply(Object[] args) {
                    return rdc;
                }
            });

        // Reduce totals from remote nodes into overall average.
        qry.localReducer(new C1<Object[], GridReducer<GridTuple2<Double, Integer>, Double>>() {
            private GridReducer<GridTuple2<Double, Integer>, Double> rdc =
                new GridReducer<GridTuple2<Double, Integer>, Double>() {
                    private double sum;
                    private int cnt;

                    @Override public boolean collect(GridTuple2<Double, Integer> e) {
                        sum += e.get1();
                        cnt += e.get2();

                        // Continue collecting.
                        return true;
                    }

                    @Override public Double apply() {
                        return cnt == 0 ? 0 : sum / cnt;
                    }
                };

                @Override public GridReducer<GridTuple2<Double, Integer>, Double> apply(Object[] args) {
                    return rdc;
                }
            });

        // Calculate average salary for a specific organization.
        print("Average salary for 'GridGain' employees: " +
            qry.queryArguments("GridGain").reduce(p).get());

        print("Average salary for 'Other' employees: " +
            qry.queryArguments("Other").reduce(p).get());
    }

    /**
     * Example for SQL queries with custom transformer to allow passing
     * only the required set of fields back to caller.
     *
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryEmployeeNames(GridProjection p) throws GridException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> cache = cache();

        // Create query to get names of all employees working for some company.
        GridCacheTransformQuery<GridCacheAffinityKey<UUID>, Person, String> qry =
            cache.createTransformQuery(SQL, Person.class,
                "from Person, Organization " +
                    "where Person.orgId = Organization.id and lower(Organization.name) = lower(?)");

        // Transformer to convert Person objects to String.
        // Since caller only needs employee names, we only
        // send names back.
        qry.remoteTransformer(new C1<Object[], GridClosure<Person, String>>() {
            @Override public GridClosure<Person, String> apply(Object[] args) {
                return new C1<Person, String>() {
                    @Override public String apply(Person p) {
                        return p.getLastName();
                    }
                };
            }
        });

        // Query all nodes for names of all GridGain employees.
        print("Names of all 'GridGain' employees: " +
            qry.queryArguments("GridGain").execute(p).get());
    }

    /**
     * Example for SQL-based fields queries that return only required
     * fields instead of whole key-value pairs.
     *
     * @param p Grid projection to run query on.
     * @throws GridException In case of error.
     */
    private static void queryFields(GridProjection p) throws GridException {
        GridCache<?, ?> cache = grid.cache(CACHE_NAME);

        // Create query to get names of all employees.
        GridCacheFieldsQuery qry1 = cache.createFieldsQuery(
            "select concat(firstName, ' ', lastName) from Person");

        // Execute query to get collection of rows. In this particular
        // case each row will have one element with full name of an employees.
        Collection<List<Object>> res = qry1.execute(p).get();

        // Print names.
        print("Names of all employees:", F.flat(res).iterator());

        // Create query that gets employee by name and returns his salary.
        GridCacheFieldsQuery qry2 = cache.createFieldsQuery(
            "select salary from Person where concat(firstName, ' ', lastName) = ?");

        // Only one row with one field is expected in result of this query,
        // so you can use convenient 'executeSingleField' method here.
        print("Salary of John Doe: " + qry2.queryArguments("John Doe").executeSingleField(p).get());
        print("Salary of John Smith: " + qry2.queryArguments("John Smith").executeSingleField(p).get());
    }

    /**
     * Populate cache with test data.
     *
     * @throws GridException In case of error.
     * @throws InterruptedException In case of error.
     */
    private static void initialize() throws GridException, InterruptedException {
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> cache = cache();

        // Organization projection.
        GridCacheProjection<UUID, Organization> orgCache = cache.projection(UUID.class, Organization.class);

        // Person projection.
        GridCacheProjection<GridCacheAffinityKey<UUID>, Person> personCache =
            cache.projection(GridCacheAffinityKey.class, Person.class);

        // Organizations.
        Organization org1 = new Organization("GridGain");
        Organization org2 = new Organization("Other");

        // People.
        Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
        Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
        Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
        Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

        orgCache.put(org1.getId(), org1);
        orgCache.put(org2.getId(), org2);

        // Note that in this example we use custom affinity key for Person objects
        // to ensure that all persons are collocated with their organizations.
        personCache.put(p1.key(), p1);
        personCache.put(p2.key(), p2);
        personCache.put(p3.key(), p3);
        personCache.put(p4.key(), p4);

        // Wait 1 second to be sure that all nodes processed put requests.
        Thread.sleep(1000);
    }

    /**
     * Prints collection of objects to standard out.
     *
     * @param msg Message to print before all objects are printed.
     * @param it Iterator over query results.
     */
    private static void print(String msg, Iterator<?> it) {
        if (msg != null)
            X.println(">>> " + msg);

        while (it.hasNext())
            X.println(">>>     " + it.next());
    }

    /**
     * Prints out given object to standard out.
     *
     * @param o Object to print.
     */
    private static void print(Object o) {
        X.println(">>> " + o);
    }
}
