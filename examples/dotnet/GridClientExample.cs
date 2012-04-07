// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    /**
     * <summary>
     * Grid client usage showcase.</summary>
     *
     * <remarks>
     * This example shows you how to configure, create and use Gird client.
     * You can work with cache and with computation parts of the big Grid from your C# application.
     * <para/>
     * This example expects that you start grid node via java example
     * "org.gridgain.examples.client.GridServerNodeStartup" from the examples package.
     * <para/>
     * Once you start the server example, it provides you an access to grid with several protocols:
     * TCP (+SSL) - preferred protocol (binary, small, speed optimized, a lot of other advantages...)
     * and HTTP(s) - based on the old rest protocol (has some restrictions from his nature).
     * <para/>
     * Note: in production systems you should setup and configure published endpoints of the Grid explicitly.
     * </remarks>
     */
    class GridClientExample {
        /** <summary>Grid node address to connect to.</summary> */
        private static string ServerAddress = "127.0.0.1";

        /** <summary>Pre-configured port for unsecured TCP communications.</summary> */
        public static int BaseTcpPort = 10080;

        /** <summary>Pre-configured port for SSL-protected TCP communications.</summary> */
        public static int SecureTcpPort = 10443;

        /** <summary>Pre-configured port for unsecured HTTP communications.</summary> */
        public static int BaseHttpPort = 11080;

        /** <summary>Pre-configured port for SSL-protected HTTP communications.</summary> */
        public static int SecureHttpPort = 11443;

        /** <summary>Start grid client example.</summary> */
        static void Main() {
            /* Enable debug messages. */
            Debug.Listeners.Add(new TextWriterTraceListener(System.Console.Out));

            /* Bypass all certificates for SSL communications. */
            //System.Net.ServicePointManager.ServerCertificateValidationCallback = (sender, cert, chain, error) => true;

            try {
                showcase();
            }
            catch (GridClientException e) {
                echo("Unexpected grid client exception happens: {0}", e);
            }
            finally {
                GridClientFactory.StopAll();
            }
        }

        /** <summary>Execute showcase for grid client usage.</summary> */
        static void showcase() {
            /**
             * Creates client configuration.
             */
            var cfg = new GridClientConfiguration();

            cfg.Protocol = GridClientProtocol.Tcp;
            cfg.Servers.Add(ServerAddress + ":" + BaseTcpPort);
            //cfg.Servers.Add(ServerAddress + ":" + SecureTcpPort);
            //cfg.SslContext = new Ssl.GridClientSslContext();

            /* Disable this option, if authentication SPI is disabled on your server. */
            cfg.Credentials = "s3cret";

            /**
             * Start new grid client.
             */
            var client = GridClientFactory.Start(cfg);

            echo("Grid client started: {0}", client.Id);


            /**
             * Cache manipulations.
             */

            /* Define data to work with. */
            var data = new Dictionary<String, String>();

            data["key1"] = "value1";
            data["key2"] = "value3";
            data["key3"] = "value3";

            /* Initialize remote cache. */
            var cache = client.Data();

            /* Store several values in synchronous manner. */
            cache.Put<String, String>("key1", data["key1"]);
            cache.Put<String, String>("key2", data["key2"]);

            /* Call to store the value in background task. */
            var fut = cache.PutAsync<String, String>("key3", "val3");

            /* You may do some work before asynchronous task completes. */
            echo("Value in cache by key1={0}", cache.GetItem<String, String>("key1"));
            echo("Value in cache by key2={0}", cache.GetItem<String, String>("key2"));

            /* You may skip explicit waiting - it will be performed on the first result usage. */
            //fut.WaitDone();

            /* Work with the asynchronous task result. */
            echo("Asynchronous operation result: {0}", fut.Result);

            /**
             * Compute grid usage.
             */

            /* Define task to work with. */
            var taskName = "org.gridgain.examples.client.GridServerNodeStartup$TestTask";
            var taskArg = new String[] {"1", "22", "333", "4444"};

            /* Initialize remote compute. */
            var compute = client.Compute();

            /* Execute task on server. */
            var task = compute.ExecuteAsync<int>(taskName, taskArg);

            // You may do some work before this task completes.
            // ...

            /* You may skip explicit waiting - it will be performed on the first result usage. */
            task.WaitDone();

            /* Work with the asynchronous task result. */
            echo("Asynchronous operation result: " + task.Result);
        }

        /**
         * <summary>
         * Writes the text representation of the specified array of objects, followed
         * by the current line terminator, to the standard output stream using the specified
         * format information.</summary>
         *
         * <param name="msg">A composite format string.</param>
         * <param name="args">An array of objects to write using format.</param>
         */
        private static void echo(String msg, params Object[] args) {
            Debug.WriteLine(msg, args);
        }
    }
}
