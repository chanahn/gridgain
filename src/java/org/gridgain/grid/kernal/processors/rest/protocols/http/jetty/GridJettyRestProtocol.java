// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.protocols.http.jetty;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.*;
import org.eclipse.jetty.xml.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.protocols.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.xml.sax.*;

import java.io.*;
import java.net.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Jetty REST protocol implementation.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.21032012
 */
public class GridJettyRestProtocol extends GridRestProtocolAdapter {
    /** Default Jetty configuration file path. */
    public static final String DFLT_CFG_PATH = "config/rest-jetty.xml";

    /** Rebind frequency in milliseconds in case of unsuccessful start (value is <tt>3000</tt>). */
    public static final long REBIND_FREQ = 3000;

    /** Jetty handler. */
    private GridJettyRestHandler jettyHnd;

    /** HTTP server. */
    private volatile Server httpSrv;

    /** Binder thread. */
    private GridThread binder;

    /**
     * @param ctx Context.
     */
    public GridJettyRestProtocol(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "Jetty REST";
    }

    /** {@inheritDoc} */
    @Override public void start(GridRestProtocolHandler hnd) throws GridException {
        String locHost = ctx.config().getLocalHost();

        // Bind Jetty to local host.
        if (!F.isEmpty(locHost))
            System.setProperty(GG_JETTY_HOST, locHost);

        jettyHnd = new GridJettyRestHandler(hnd, new C1<String, Boolean>() {
            @Override public Boolean apply(String tok) {
                return F.isEmpty(secretKey) || authenticate(tok);
            }
        }, log);

        String jettyPath = ctx.config().getRestJettyPath();

        final URL cfgUrl = U.resolveGridGainUrl(jettyPath == null ? DFLT_CFG_PATH : jettyPath);

        if (cfgUrl == null)
            throw new GridSpiException("Invalid Jetty configuration file: " + DFLT_CFG_PATH);
        else if (log.isDebugEnabled())
            log.debug("Jetty configuration file: " + cfgUrl);

        if (!startJetty(cfgUrl, true)) {
            U.warn(log, "Jetty failed to start (retrying every " + REBIND_FREQ + " ms). Another node on this host?");

            binder = new GridThread(new GridWorker(ctx.gridName(), "grid-connector-jetty-binder", log) {
                @SuppressWarnings({"BusyWait"})
                @Override public void body() {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(REBIND_FREQ);
                        }
                        catch (InterruptedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Jetty binder thread was interrupted.");

                            break;
                        }

                        try {
                            if (startJetty(cfgUrl, false))
                                break;
                        }
                        catch (GridException e) {
                            // Stop execution.
                            U.error(log, "Failed to start HTTP server with: " + cfgUrl, e);

                            break;
                        }
                    }
                }
            }) {
                @Override public void interrupt() {
                    try {
                        stopJetty();
                    }
                    finally {
                        super.interrupt();
                    }
                }
            };

            binder.start();
        }
        else
            if (log.isInfoEnabled())
                log.info(startInfo());
    }

    /**
     * @param con Jetty connector.
     */
    private void override(Connector con) {
        String host = System.getProperty(GG_JETTY_HOST);

        int jettyPort = ctx.config().getRestJettyPort();

        if (jettyPort == 0) {
            Integer port = Integer.getInteger(GG_JETTY_PORT);

            if (port != null)
                jettyPort = port;
        }

        if (!F.isEmpty(host))
            con.setHost(host);

        if (jettyPort != 0)
            con.setPort(jettyPort);
    }

    /**
     * Gets "on" or "off" string for given boolean value.
     *
     * @param b Boolean value to convert.
     * @return Result string.
     */
    private String onOff(boolean b) {
        return b ? "on" : "off";
    }

    /**
     * @param cfgUrl Jetty configuration file URL.
     * @param first First time trying to start.
     * @throws GridException If failed.
     * @return {@code True} if Jetty started.
     */
    @SuppressWarnings("IfMayBeConditional")
    private boolean startJetty(URL cfgUrl, boolean first) throws GridException {
        XmlConfiguration cfg;

        try {
            cfg = new XmlConfiguration(cfgUrl);
        }
        catch (FileNotFoundException e) {
            throw new GridSpiException("Failed to find configuration file: " + cfgUrl, e);
        }
        catch (SAXException e) {
            throw new GridSpiException("Failed to parse configuration file: " + cfgUrl, e);
        }
        catch (IOException e) {
            throw new GridSpiException("Failed to load configuration file: " + cfgUrl, e);
        }
        catch (Exception e) {
            throw new GridSpiException("Failed to start HTTP server with configuration file: " + cfgUrl, e);
        }

        try {
            httpSrv = (Server)cfg.configure();

            httpSrv.setHandler(jettyHnd);

            for (Connector con : httpSrv.getConnectors())
                override(con);

            httpSrv.start();

            if (httpSrv.isStarted()) {
                SB sb = new SB();

                sb.a(name());
                sb.a(" [");
                sb.a("security: ").a(onOff(ctx.config().getRestSecretKey() != null));
                sb.a(", URLs: (");

                for (Connector con : httpSrv.getConnectors()) {
                    int port = con.getPort();

                    Integer expPort;

                    // This if-else statement cannot be replaced with conditional
                    // because in this case it may lead to NPE.
                    if (ctx.config().getRestJettyPort() != 0)
                        expPort = ctx.config().getRestJettyPort();
                    else
                        expPort = Integer.getInteger(GG_JETTY_PORT);

                    if (expPort != null && expPort != port)
                        U.error(log, "Jetty did not use " + GG_JETTY_PORT + " system property for port " +
                            "value [port=" + port + ", expected=" + expPort + ", connector=" +
                            con.getName() + ']');

                    if (port > 0) {
                        ctx.ports().registerPort(port, GridPortProtocol.TCP, getClass());

                        String host = con.getHost();

                        if (host == null)
                            host = "localhost";

                        sb.a(host).a(":").a(port).a(", ");
                    }
                }

                sb.d(sb.length() - 2, sb.length()); // Remove last ', '.
                sb.a(")").a("]");

                // Don't log if not 1st time and in daemon.
                if (!first && !ctx.config().isDaemon()) {
                    if (log.isQuiet())
                        U.quiet(sb);
                    else if (log.isInfoEnabled())
                        log.info(sb.toString());

                    if (log.isInfoEnabled())
                        log.info("Successfully started Jetty HTTP server.");
                }

                return true;
            }

            return  false;
        }
        catch (BindException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to bind HTTP server to configured port (will retry in " + REBIND_FREQ + " ms).");

            stopJetty();

            return false;
        }
        catch (MultiException e) {
            if (log.isDebugEnabled())
                log.debug("Caught multi exception: " + e);

            for (Object obj : e.getThrowables())
                if (!(obj instanceof BindException))
                    throw new GridException("Failed to start Jetty HTTP server.", e);

            if (log.isDebugEnabled())
                log.debug("Failed to bind HTTP server to configured port (will retry in " + REBIND_FREQ + " ms).");

            stopJetty();

            return false;
        }
        catch (Exception e) {
            throw new GridException("Failed to start Jetty HTTP server.", e);
        }
    }

    /**
     * Stops Jetty.
     */
    private void stopJetty() {
        // Jetty does not really stop the server if port is busy.
        try {
            if (httpSrv != null) {
                // If server was successfully started, deregister ports.
                if (httpSrv.isStarted())
                    ctx.ports().deregisterPorts(getClass());

                // Record current interrupted status of calling thread.
                boolean interrupted = Thread.interrupted();

                try {
                    httpSrv.stop();

                    httpSrv = null;
                }
                finally {
                    // Reset interrupted flag on calling thread.
                    if (interrupted)
                        Thread.currentThread().interrupt();
                }
            }
        }
        catch (InterruptedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Thread has been interrupted.");

            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            U.error(log, "Failed to stop Jetty HTTP server.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        if (binder != null) {
            U.interrupt(binder);
            U.join(binder, log);
        }

        stopJetty();

        httpSrv = null;
        jettyHnd = null;

        if (log.isInfoEnabled())
            log.info(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJettyRestProtocol.class, this);
    }
}
