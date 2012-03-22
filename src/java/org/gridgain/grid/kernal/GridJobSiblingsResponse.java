// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Job siblings response.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.22032012
 */
public class GridJobSiblingsResponse implements Externalizable {
    /** */
    private Collection<GridJobSibling> siblings;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridJobSiblingsResponse() {
        // No-op.
    }

    public GridJobSiblingsResponse(Collection<GridJobSibling> siblings) {
        this.siblings = siblings;
    }

    /**
     * @return Job siblings.
     */
    public Collection<GridJobSibling> jobSiblings() {
        return siblings;
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        siblings = U.readCollection(in);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeCollection(out, siblings);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSiblingsResponse.class, this);
    }
}
