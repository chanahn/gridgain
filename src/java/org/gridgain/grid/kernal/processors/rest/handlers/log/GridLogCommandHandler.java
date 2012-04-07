// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.log;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.typedef.*;
import org.gridgain.grid.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.GridRestCommand.*;

/**
 * Handler for {@link GridRestCommand#LOG} command.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.1c.07042012
 */
public class GridLogCommandHandler extends GridRestCommandHandlerAdapter {
    /** Default log path. */
    private static final String DFLT_PATH = "work/log/gridgain.log";

    /** Folders accessible for log reading. */
    private List<File> accessibleFolders;

    /** @param ctx Context. */
    public GridLogCommandHandler(GridKernalContext ctx) {
        super(ctx);

        String[] accessiblePaths = ctx.config().getRestAccessibleFolders();

        if (accessiblePaths == null) {
            String ggHome = U.getGridGainHome();

            if (ggHome != null)
                accessiblePaths = new String[] {ggHome};
        }

        if (accessiblePaths != null) {
            accessibleFolders = new ArrayList<File>();

            for (String accessiblePath : accessiblePaths)
                accessibleFolders.add(new File(accessiblePath));
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Neither restAccessibleFolders nor GRIDGAIN_HOME properties are not set, will not restrict " +
                    "log files access");
        }
    }

    /** {@inheritDoc} */
    @Override public boolean supported(GridRestCommand cmd) {
        return cmd == LOG;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        String path = req.parameter("path");

        int from = integerValue(req.parameter("from"), -1);
        int to = integerValue(req.parameter("to"), -1);

        if (path == null)
            path = DFLT_PATH;

        BufferedReader reader = null;

        try {
            URL url = U.resolveGridGainUrl(path);

            if (url == null)
                throw new GridException("Log file not found: " + path);

            if (!isAccessible(url))
                throw new GridException("File is not accessible through REST" +
                    " (check restAccessibleFolders configuration property): " + path);

            reader = new BufferedReader(new InputStreamReader(url.openStream()));

            List<String> lines = new LinkedList<String>();

            String line;

            int i = 0;

            while ((line = reader.readLine()) != null) {
                i++;

                if (from != -1 && i - 1 < from)
                    continue;

                if (to != -1 && i - 1 > to)
                    break;

                lines.add(line);
            }

            return new GridFinishedFuture<GridRestResponse>(ctx, new GridRestResponse(lines));
        }
        catch (GridException e) {
            return new GridFinishedFuture<GridRestResponse>(ctx, e);
        }
        catch (IOException e) {
            return new GridFinishedFuture<GridRestResponse>(ctx, e);
        }
        finally {
            U.close(reader, log);
        }
    }

    /**
     * Tries to safely get int value from the given object with possible conversions.
     *
     * @param obj Object to convert.
     * @param dfltVal Default value if conversion is not possible.
     * @return Converted value.
     */
    private int integerValue(Object obj, int dfltVal) {
        int res = dfltVal;

        if (obj instanceof Number)
            res = ((Number)obj).intValue();
        else if (obj instanceof String) {
            try {
                res = Integer.parseInt((String)obj);
            }
            catch (RuntimeException ignored) {
            }
        }

        return res;
    }

    /**
     * Checks whether given url is accessible against configuration.
     *
     * @param url URL to check.
     * @return {@code True} if file is accessible (i.e. located in one of the sub-folders of
     *      {@code restAccessibleFolders} list.
     */
    private boolean isAccessible(URL url) {
        // No check is made if configuration is undefined.
        if (accessibleFolders == null)
            return true;

        File f = new File(url.getFile());

        do {
            if (F.contains(accessibleFolders, f))
                return true;

            f = f.getParentFile();
        }
        while (f != null);

        return false;
    }
}
