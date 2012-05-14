// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nodestart;

import com.jcraft.jsch.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * SSH-based node starter.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
public class GridNodeRunnable implements Runnable {
    /** Default GridGain home path for Windows (taken from environment variable). */
    private static final String DFLT_GG_HOME_WIN = "%GRIDGAIN_HOME%";

    /** Default GridGain home path for Linux (taken from environment variable). */
    private static final String DFLT_GG_HOME_LINUX = "$GRIDGAIN_HOME";

    /** Default start script path for Windows. */
    private static final String DFLT_SCRIPT_WIN = "bin\\ggstart.bat -v -np";

    /** Default start script path for Linux. */
    private static final String DFLT_SCRIPT_LINUX = "bin/ggstart.sh -v";

    /** Default log folder. */
    private static final String DFLT_LOG_DIR = "work/log";

    /** Windows service executable. */
    private static final String SVC_EXE = "bin\\ggservice.exe";

    /** Hostname. */
    private final String host;

    /** Port number. */
    private final int port;

    /** Username. */
    private final String uname;

    /** Password. */
    private final String passwd;

    /** Private key file. */
    private final File key;

    /** GridGain installation path. */
    private String ggHome;

    /** Start script path. */
    private String script;

    /** Configuration file path. */
    private String cfg;

    /** Log folder path. */
    private String logDir;

    /** Start results. */
    private final Collection<GridTuple3<String, Boolean, String>> res;

    /**
     * Constructor.
     *
     * @param host Hostname.
     * @param port Port number.
     * @param uname Username.
     * @param passwd Password.
     * @param key Private key file.
     * @param ggHome GridGain installation path.
     * @param script Start script path.
     * @param cfg Configuration file path.
     * @param logDir Log folder path.
     * @param res Start results.
     */
    public GridNodeRunnable(String host, int port, String uname, @Nullable String passwd,
        @Nullable File key, @Nullable String ggHome, @Nullable String script, @Nullable String cfg,
        @Nullable String logDir, Collection<GridTuple3<String, Boolean, String>> res) {
        assert host != null;
        assert port > 0;
        assert uname != null;
        assert res != null;

        this.host = host;
        this.port = port;
        this.uname = uname;
        this.passwd = passwd;
        this.key = key;
        this.ggHome = ggHome;
        this.script = script;
        this.cfg = cfg;
        this.logDir = logDir;
        this.res = res;
    }

    /** {@inheritDoc} */
    @Override public void run() {
        JSch ssh = new JSch();

        Session ses = null;

        try {
            if (key != null)
                ssh.addIdentity(key.getAbsolutePath());

            ses = ssh.getSession(uname, host, port);

            if (passwd != null)
                ses.setPassword(passwd);

            ses.setConfig("StrictHostKeyChecking", "no");

            ses.connect();

            boolean win = isWindows(ses);

            String separator = win ? "\\" : "/";

            if (ggHome == null)
                ggHome = win ? DFLT_GG_HOME_WIN : DFLT_GG_HOME_LINUX;

            if (script == null)
                script = win ? DFLT_SCRIPT_WIN : DFLT_SCRIPT_LINUX;

            if (cfg == null)
                cfg = "";

            if (logDir == null)
                logDir = DFLT_LOG_DIR;

            // Replace all slashes with correct separator.
            ggHome = ggHome.replace("\\", separator).replace("/", separator);
            script = script.replace("\\", separator).replace("/", separator);
            cfg = cfg.replace("\\", separator).replace("/", separator);
            logDir = logDir.replace("\\", separator).replace("/", separator);

            UUID id = UUID.randomUUID();

            String createLogDirCmd = new SB().
                a("mkdir ").a(ggHome).a(separator).a(logDir).
                toString();

            String startNodeCmd;

            if (win) {
                String svcName = "GridGain-" + id;

                startNodeCmd = new SB().
                    a("sc create ").a(svcName).
                    a(" binPath= \"").a(ggHome).a(separator).a(SVC_EXE).a("\"").
                    a(" & ").
                    a("sc start ").a(svcName).
                    a(" ").a(svcName).
                    a(" \"").a(ggHome).a(separator).a(script).
                    a(" ").a(cfg).a("\"").
                    a(" \"").a(ggHome).a(separator).a(logDir).a(separator).
                    a("gridgain.").a(id).a(".log\"").
                    toString();
            }
            else {
                int spaceIdx = script.indexOf(' ');

                String scriptPath = spaceIdx > -1 ? script.substring(0, spaceIdx) : script;
                String scriptArgs = spaceIdx > -1 ? script.substring(spaceIdx + 1) : "";

                startNodeCmd = new SB().
                    a("nohup ").
                    a("\"").a(ggHome).a(separator).a(scriptPath).a("\"").
                    a(" ").a(scriptArgs).
                    a(!cfg.isEmpty() ? " \"" : "").a(cfg).a(!cfg.isEmpty() ? "\"" : "").
                    a(" > ").
                    a("\"").a(ggHome).a(separator).a(logDir).a(separator).
                    a("gridgain.").a(id).a(".log\"").
                    a(" 2>& 1 &").
                    toString();
            }

            execute(ses, createLogDirCmd);
            execute(ses, startNodeCmd);

            synchronized (res) {
                res.add(new GridTuple3<String, Boolean, String>(host, true, null));
            }
        }
        catch (Exception e) {
            synchronized (res) {
                res.add(new GridTuple3<String, Boolean, String>(host, false, e.getMessage()));
            }
        }
        finally {
            if (ses != null && ses.isConnected())
                ses.disconnect();
        }
    }

    /**
     * Executes command using {@code shell} channel.
     *
     * @param ses SSH session.
     * @param cmd Command.
     * @throws Exception If execution failed.
     */
    private void execute(Session ses, String cmd) throws Exception {
        ChannelShell ch = null;
        PrintStream out = null;

        try {
            ch = (ChannelShell)ses.openChannel("shell");

            ch.connect();

            out = new PrintStream(ch.getOutputStream(), true);

            out.println(cmd);

            Thread.sleep(1000);
        }
        finally {
            U.closeQuiet(out);

            if (ch != null && ch.isConnected())
                ch.disconnect();
        }
    }

    /**
     * Checks whether host is running Windows OS.
     *
     * @param ses SSH session.
     * @return Whether host is running Windows OS.
     * @throws JSchException In case of SSH error.
     */
    private boolean isWindows(Session ses) throws JSchException {
        ChannelExec ch = (ChannelExec)ses.openChannel("exec");

        ch.setCommand("cmd.exe");

        try {
            ch.connect();

            return new BufferedReader(new InputStreamReader(ch.getInputStream())).readLine() != null;
        }
        catch (JSchException ignored) {
            return false;
        }
        catch (IOException ignored) {
            return false;
        }
        finally {
            if (ch.isConnected())
                ch.disconnect();
        }
    }
}
