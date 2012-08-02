// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

// To run this example start a node from IDE:
// run org.gridgain.examples.client.GridClientExampleNodeStartup

#include <vector>
#include <string>
#include <unordered_map>

#include <iostream>
#include <sstream>

#include <gridgain/gridgain.hpp>

#include <boost/lexical_cast.hpp>

using namespace std;

static string SERVER_ADDRESS = "127.0.0.1";
static string CACHE_NAME = "partitioned";
static int KEYS_CNT = 10;
static int TCP_PORT = 11211;

GridClientConfiguration clientConfiguration() {
    GridClientConfiguration clientConfig;

    vector<GridSocketAddress> servers;

//    To enable communication with GridGain instance by HTTP, not by TCP, uncomment the following lines
//    and comment push_back with TCP.
//    ================================
//    GridClientProtocolConfiguration protoCfg;
//
//    protoCfg.protocol(HTTP);
//
//    clientConfig.setProtocolConfiguration(protoCfg);
//
//    servers.push_back(GridSocketAddress(SERVER_ADDRESS, GridClientProtocolConfiguration::DFLT_HTTP_PORT));

//    To enable communication over SSL uncomment the following lines.
//    ================================
//    GridGain node should be started with config examples/config/spring-cache-ssl.xml .
//
//    GridClientProtocolConfiguration protoCfg;
//
//    protoCfg.sslEnabled(true);
//
//    protoCfg.certificateFilePath("examples/keystore/client.pem");
//
//    protoCfg.certificateFilePassword("123456");
//
//    clientConfig.setProtocolConfiguration(protoCfg);

    cout << "connecting to " << SERVER_ADDRESS << ", port " << TCP_PORT << endl;

    servers.push_back(GridSocketAddress(SERVER_ADDRESS, TCP_PORT));

    clientConfig.servers(servers);

    return clientConfig;
}

void clientCacheExample(TGridClientPtr client) {
    TGridClientComputePtr cc = client->compute();

    TGridClientNodeList nodes = cc->nodes();

    if (nodes.empty()) {
        cerr << "Failed to connect to grid in cache example, make sure that it is started and connection "
                "properties are correct." << endl;

        GridClientFactory::stopAll();

        return;
    }

    cout << "Current grid topology: " << nodes.size() << endl;

    // Random node ID.
    GridUuid randNodeId = nodes[0]->getNodeId();

    // Get client projection of grid partitioned cache.
    TGridClientDataPtr rmtCache = client->data(CACHE_NAME);

    TGridClientVariantSet keys;

    // Put some values to the cache.
    for (int32_t i = 0; i < KEYS_CNT; i++) {
        ostringstream oss;

        oss << "val-" << i;

        string v = oss.str();

        string key=boost::lexical_cast<string>(i);

        rmtCache->put(key, v);

        GridUuid nodeId = rmtCache->affinity(key);

        cout << ">>> Storing key " << key << " on node " << nodeId << endl;

        keys.push_back(key);
    }

    TGridClientNodeList nodelst;
    TGridClientNodePtr p = client->compute()->node(randNodeId);

    nodelst.push_back(p);

    // Pin a remote node for communication. All further communication
    // on returned projection will happen through this pinned node.
    TGridClientDataPtr prj = rmtCache->pinNodes(nodelst);

    GridClientVariant key0 = GridClientVariant(boost::lexical_cast<string>(0));

    GridClientVariant key6 = GridClientVariant(boost::lexical_cast<string>(6));

    GridClientVariant val = prj->get(key0);

    cout << ">>> Loaded single value: " << val.debugString() << endl;

    TGridClientVariantMap vals = prj->getAll(keys);

    cout << ">>> Loaded multiple values, size: " << vals.size() << endl;

    for (TGridClientVariantMap::const_iterator iter = vals.begin(); iter != vals.end(); ++iter)
        cout << ">>> Loaded cache entry [key=" << iter->first <<
                ", val=" << iter->second << ']' << endl;

    TGridBoolFuturePtr futPut = prj->putAsync(key0, "new value for 0");

    cout << ">>> Result of asynchronous put: " << (futPut->get() ? "success" : "failure") << endl;

    unordered_map<GridUuid, TGridClientVariantMap> keyVals;

    GridUuid nodeId = rmtCache->affinity(key0);

    for (int32_t i = 0; i < KEYS_CNT; i++) {
        ostringstream oss;

        oss << "updated-val-" << i;

        string v = oss.str();

        string key=boost::lexical_cast<string>(i);

        keyVals[nodeId][key] = v;
    }

    for (unordered_map<GridUuid, TGridClientVariantMap>::iterator iter = keyVals.begin();
            iter != keyVals.end(); ++iter)
       rmtCache->putAll(iter->second);

    vector<TGridBoolFuturePtr> futs;

    for (unordered_map<GridUuid, TGridClientVariantMap>::iterator iter = keyVals.begin();
            iter != keyVals.end(); ++iter) {
       TGridBoolFuturePtr fut = rmtCache->putAllAsync(iter->second);

       futs.push_back(fut);
    }

    for (vector<TGridBoolFuturePtr>::iterator iter = futs.begin(); iter != futs.end(); ++iter)
       cout << ">>> Result of async putAll: " << (iter->get()->get() ? "success" : "failure") << endl;

    cout << ">>> Value for key " << key0.debugString() << " is " <<
            rmtCache->get(key0).debugString() << endl;

    // Asynchronous gets, too.
    TGridClientFutureVariant futVal = rmtCache->getAsync(key0);

    futVal->get();

    if (futVal->success())
       cout << ">>> Asynchronous value for key " << key0.debugString() << " is " <<
           futVal->result().debugString() << endl;
    else
       cout << ">>> Asynchronous retrieving value for key " << key0.debugString() << " failed" << endl;

    // Multiple values can be fetched at once. Here we batch our get
    // requests by affinity nodes to ensure least amount of network trips.
    for (unordered_map<GridUuid, TGridClientVariantMap>::iterator iter = keyVals.begin();
            iter != keyVals.end(); ++iter) {
       GridUuid uuid = iter->first;

       TGridClientVariantMap m = iter->second;

       TGridClientVariantSet keys;

       for (TGridClientVariantMap::const_iterator miter = m.begin(); miter != m.end(); ++miter )
           keys.push_back(miter->first);

       // Since all keys in our getAll(...) call are mapped to the same primary node,
       // grid cache client will pick this node for the request, so we only have one
       // network trip here.

       TGridClientVariantMap map = rmtCache->getAll(keys);

       cout << ">>> Values from node [nodeId=" + uuid.uuid() + ", values=[" << endl;

       for (TGridClientVariantMap::const_iterator miter = map.begin(); miter != map.end(); ++miter) {
           if (miter != map.begin())
               cout << ", " << endl;
           cout << "[key=" << miter->first << ", value=" << miter->second << ']';
       }

       cout << ']' << endl;
    }

    // Multiple values may be retrieved asynchronously, too.
    // Here we retrieve all keys at once. Since this request
    // will be sent to some grid node, this node may not be
    // the primary node for all keys and additional network
    // trips will have to be made within grid.

    TGridClientFutureVariantMap futVals = rmtCache->getAllAsync(keys);

    futVals->get();

    if (futVals->success()) {
       TGridClientVariantMap map = futVals->result();

       cout << ">>> Values retrieved asynchronously: " << endl;

       for (TGridClientVariantMap::const_iterator miter = map.begin(); miter != map.end(); ++miter) {
           if (miter != map.begin())
               cout << ", " << endl;
           cout << "[key=" << miter->first << ", value=" << miter->second << ']';
       }

       cout << endl;
    }
    else
       cout << ">>> Asynchronous retrieving multiple values failed" << endl;

    // Contents of cache may be removed one by one synchronously.
    // Again, this operation is affinity aware and only the primary
    // node for the key is contacted.
    bool res = rmtCache->remove(key0);

    cout << ">>> Result of removal: " << (res ? "success" : "failure") << endl;

    // ... and asynchronously.
    TGridBoolFuturePtr futRes = rmtCache->removeAsync(boost::lexical_cast<string>(1));

    cout << ">>> Result of asynchronous removal: " << (futRes->get() ? "success" : "failure") << endl;

    // Multiple entries may be removed at once synchronously...
    TGridClientVariantSet keysRemove;

    keysRemove.push_back(boost::lexical_cast<string>(2));
    keysRemove.push_back(boost::lexical_cast<string>(3));

    bool rmvRslt = rmtCache->removeAll(keysRemove);

    cout << ">>> Result of removeAll: " << (rmvRslt ? "success" : "failure") << endl;

    // ... and asynchronously.
    keysRemove.clear();

    keysRemove.push_back(boost::lexical_cast<string>(4));
    keysRemove.push_back(boost::lexical_cast<string>(5));

    TGridBoolFuturePtr rmvFut = rmtCache->removeAllAsync(keysRemove);

    cout << ">>> Result of asynchronous removeAll: " << (rmvFut->get() ? "success" : "failure") << endl;

    // Values may also be replaced.
    res = rmtCache->replace(key6, "newer value for 6");

    cout << ">>> Result for replace for existent key6 is " << (res ? "success" : "failure") << endl;

    // Asynchronous replace is supported, too. This one is expected to fail, since key0 doesn't exist.
    futRes = rmtCache->replaceAsync(key0, "newest value for 0");

    res = futRes->get();

    cout << ">>> Result for asynchronous replace for nonexistent key0 is " <<
            (res ? "success" : "failure") << endl;

    rmtCache->put(key0, boost::lexical_cast<string>(0));

    // Compare and set are implemented, too.
    res = rmtCache->cas(key0, "newest cas value for 0", boost::lexical_cast<string>(0));

    cout << ">>> Result for put using cas is " << (res ? "success" : "failure") << endl;

    // CAS can be asynchronous.
    futRes = rmtCache->casAsync(key0, boost::lexical_cast<string>(0), "newest cas value for 0");

    //boost::this_thread::sleep(boost::posix_time::milliseconds(1000));

    res = futRes->get();

    // Expected to fail.
    cout << ">>> Result for put using asynchronous cas is "
        << (res? "success" : "failure") << endl;

    // It's possible to obtain cache metrics using data client API.
    GridClientDataMetrics metrics = rmtCache->metrics();
    cout << ">>> Cache metrics : " << metrics << endl;

    // Cache metrics may be retrieved for individual keys.
    metrics = rmtCache->metrics(key0);
    cout << ">>> Cache metrics for a key : " << metrics << endl;

    TGridClientFutureDataMetrics futMetrics = rmtCache->metricsAsync();

    futMetrics->get();

    if (futMetrics->success())
       cout << ">>> Cache asynchronous metrics: " << futMetrics->result() << endl;
    else
       cout << ">>> Cache asynchronous metrics failed. " << endl;

    futMetrics = rmtCache->metricsAsync(key0);

    futMetrics->get();

    if (futMetrics->success())
       cout << ">>> Cache asynchronous metrics for a key: " << futMetrics->result() << endl;
    else
       cout << ">>> Cache asynchronous metrics for a key failed. " << endl;
}

class GridUuidNodePredicate : public GridClientPredicate<GridClientNode> {
    private:
        GridUuid uu;
    public:
        GridUuidNodePredicate(const GridUuid& u) : uu(u) {
        }

        bool apply(const GridClientNode& node) const {
            return node.getNodeId() == uu;
        }
};

void clientComputeExample(TGridClientPtr client) {
    TGridClientComputePtr clientCompute = client->compute();

    TGridClientNodeList nodes = clientCompute->nodes();

    if (nodes.empty()) {
        cerr << "Failed to connect to grid in compute example, make sure that it is started and connection "
                "properties are correct." << endl;

        GridClientFactory::stopAll();

        return;
    }

    cout << "Current grid topology: " << nodes.size() << endl;

    GridUuid randNodeId = nodes[0]->getNodeId();

    cout << "RandNodeId is " << randNodeId.uuid() << endl;

    TGridClientNodePtr p = clientCompute->node(randNodeId);

    TGridClientComputePtr prj = clientCompute->projection(*p);

    GridClientVariant rslt = prj->execute("org.gridgain.examples.client.GridClientExampleTask");

    cout << ">>> GridClientNode projection : there are totally " << rslt.toString() <<
        " test entries on the grid" << endl;

    TGridClientNodeList prjNodes;

    prjNodes.push_back(p);

    prj = clientCompute->projection(prjNodes);

    rslt = prj->execute("org.gridgain.examples.client.GridClientExampleTask");

    cout << ">>> Collection execution : there are totally " << rslt.toString() <<
        " test entries on the grid" << endl;

    GridUuidNodePredicate* uuidPredicate = new GridUuidNodePredicate(randNodeId);

    TGridClientNodePredicatePtr predPtr(uuidPredicate);

    prj = clientCompute->projection(predPtr);

    rslt = prj->execute("org.gridgain.examples.client.GridClientExampleTask");

    cout << ">>> Predicate execution : there are totally " << rslt.toString() <<
        " test entries on the grid" << endl;

    // Balancing - may be random or round-robin. Users can create
    // custom load balancers as well.
    TGridClientLoadBalancerPtr balancer(new GridClientRandomBalancer());

    prj = clientCompute->projection(predPtr, balancer);

    rslt = prj->execute("org.gridgain.examples.client.GridClientExampleTask");

    cout << ">>> Predicate execution with balancer : there are totally " << rslt.toString() <<
            " test entries on the grid" << endl;

    // Now let's try round-robin load balancer.
    balancer = TGridClientLoadBalancerPtr(new GridClientRoundRobinBalancer());

    prj = prj->projection(prjNodes, balancer);

    rslt = prj->execute("org.gridgain.examples.client.GridClientExampleTask");

    cout << ">>> GridClientNode projection : there are totally " << rslt.toString() <<
            " test entries on the grid" << endl;

    TGridClientFutureVariant futVal = prj->executeAsync("org.gridgain.examples.client.GridClientExampleTask");

    futVal->get();

    if (futVal->success())
       cout << ">>> Execute async : there are totally " << futVal->result().toString() <<
       " test entries on the grid" << endl;
    else
       cout << ">>> Execute async failed" << endl;

    TGridClientDataPtr rmtCache = client->data("partitioned");

    GridClientVariant key0((int32_t)0);

    GridClientVariant val0("new value for 0");

    rmtCache->put(key0, val0);

    rslt = prj->affinityExecute("org.gridgain.examples.client.GridClientExampleTask", "partitioned", key0);

    cout << ">>> Affinity execute : there are totally " << rslt.toString() << " test entries on the grid" <<
            endl;

    futVal = prj->affinityExecuteAsync("org.gridgain.examples.client.GridClientExampleTask", "partitioned", key0);

    futVal->get();

    if (futVal->success())
       cout << ">>> Affinity execute async : there are totally " << futVal->result().toString() <<
           " test entries on the grid" << endl;
    else
       cout << ">>> Affinity execute async failed" << endl;

    vector<GridUuid> uuids;

    uuids.push_back(randNodeId);

    nodes = prj->nodes(uuids);

    cout << ">>> Nodes with UUID " << randNodeId.uuid() << " : ";

    for (size_t i = 0 ; i < nodes.size(); i++) {
        if (i != 0)
            cout << ", ";

        cout << *(nodes[i]);
    }

    cout << endl;

    // Nodes may also be filtered with predicate. Here
    // we create projection which only contains local node.
    GridUuidNodePredicate* uuidNodePredicate = new GridUuidNodePredicate(randNodeId);

    TGridClientNodePredicatePtr predFullPtr(uuidNodePredicate);

    nodes = prj->nodes(predFullPtr);

    cout << ">>> Nodes filtered with predicate : ";

    for (size_t i = 0 ; i < nodes.size(); i++) {
        if (i != 0)
            cout << ", ";

        cout << *(nodes[i]);
    }

    cout << endl;

    // Information about nodes may be refreshed explicitly.
    TGridClientNodePtr clntNode = prj->refreshNode(randNodeId, true, true);

    cout << ">>> Refreshed node : " << *clntNode << endl;

    TGridClientNodeFuturePtr futClntNode = prj->refreshNodeAsync(randNodeId, false, false);

    futClntNode->get();

    if (futClntNode->success())
        cout << ">>> Refreshed node asynchronously : " << *(futClntNode->result()) << endl;
    else
        cout << ">>> Refresh node asynchronously failed." << endl;

    // Nodes may also be refreshed by IP address.
    string clntAddr = "127.0.0.1";

    vector<GridSocketAddress> addrs = clntNode->availableAddresses(TCP);

    if (addrs.size() > 0)
        clntAddr = addrs[0].host();

    clntNode = prj->refreshNode(clntAddr, true, true);

    cout << ">>> Refreshed node by IP : " << *clntNode << endl;

    // Asynchronous version.
    futClntNode = prj->refreshNodeAsync(clntAddr, false, false);

    futClntNode->get();

    if (futClntNode->success())
        cout << ">>> Refreshed node by IP asynchronously : " << *futClntNode->result() << endl;
    else
        cout << ">>> Refreshed node by IP asynchronously failed." << endl;

    // Topology as a whole may be refreshed, too.
    TGridClientNodeList top = prj->refreshTopology(true, true);

    cout << ">>> Refreshed topology : ";

    for (size_t i = 0 ; i < top.size(); i++) {
        if (i != 0)
            cout << ", ";

        cout << *top[i];
    }

    cout << endl;

    // Asynchronous version.
    TGridClientNodeFutureList topFut = prj->refreshTopologyAsync(false, false);

    topFut->get();

    if (topFut->success()) {
        cout << ">>> Refreshed topology asynchronously : ";

        top = topFut->result();

        for (size_t i = 0 ; i < top.size(); i++) {
            if (i != 0)
                cout << ", ";

            cout << *top[i];
        }

        cout << endl;
    }
    else
        cout << ">>> Refresh topology asynchronously failed." << endl;

    try {
        vector<string> log = prj->log(0, 1);

        cout << ">>> First log lines : " << endl;

        cout << log[0] << endl;
        cout << log[1] << endl;

        // Log entries may be fetched asynchronously.
        TGridFutureStringList futLog = prj->logAsync(1, 2);

        futLog->get();

        if (futLog->success()) {
            log = futLog->result();

            cout << ">>> First log lines fetched asynchronously : " << endl;
            cout << log[0] << endl;
            cout << log[1] << endl;
        }
        else
            cout << ">>> First log lines fetching asynchronously failed " << endl;

        // Log file name can also be specified explicitly.
        log = prj->log("work/log/gridgain.log", 0, 1);

        cout << ">>> First log lines from log file work/log/gridgain.log : " << endl;
        cout << log[0] << endl;
        cout << log[1] << endl;

        // Asynchronous version supported as well.
        futLog = prj->logAsync("work/log/gridgain.log", 1, 2);

        futLog->get();

        if (futLog->success()) {
            log = futLog->result();

            cout << ">>> First log lines from log file work/log/gridgain.log fetched asynchronously : " << endl;
            cout << log[0] << endl;
            cout << log[1] << endl;
        }
        else
            cout << ">>> First log lines from log file work/log/gridgain.log fetching asynchronously failed " << endl;
    }
    catch (GridClientException&) {
        cout << "Log file was not found " << endl;
    }

    cout << "In the end of example" << endl;
}

int main ()
{
    try{
        TGridClientPtr client = GridClientFactory::start(clientConfiguration());

        clientCacheExample(client);

        clientComputeExample(client);
    }catch(exception& e){
        cerr << "Caught unhandled exception: " << e.what() << endl;
    }

    GridClientFactory::stopAll();
}
