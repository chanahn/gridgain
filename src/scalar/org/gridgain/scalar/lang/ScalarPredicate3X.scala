// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */
 
package org.gridgain.scalar.lang

import org.gridgain.grid.util.{GridUtils => U}
import org.gridgain.grid.lang._
import org.gridgain.grid._

/**
 * Peer deploy aware adapter for Java's `GridPredicate3X`.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
class ScalarPredicate3X[T1, T2, T3](private val p: (T1, T2, T3) => Boolean) extends GridPredicate3X[T1, T2, T3] {
    assert(p != null)

    peerDeployLike(U.peerDeployAware(p))

    /**
     * Delegates to passed in function.
     */
    @throws(classOf[GridException])
    def applyx(e1: T1, e2: T2, e3: T3): Boolean = {
        p(e1, e2, e3)
    }
}