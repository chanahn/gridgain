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

import org.gridgain.grid.lang.GridClosure3

/**
 * Wrapping Scala function for `GridClosure3`.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.3c.14052012
 */
class ScalarClosure3Function[T1, T2, T3, R](val inner: GridClosure3[T1, T2, T3, R]) extends ((T1, T2, T3) => R) {
    assert(inner != null)

    /**
     * Delegates to passed in grid closure.
     */
    def apply(t1: T1, t2: T2, t3: T3): R = {
        inner.apply(t1, t2, t3)
    }
}