﻿// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Hasher {
    /**
     * <summary>
     * Object marked with this interface becomes available for the consistent hash calculation.</summary>
     */
    public interface IGridClientConsistentHashObject {
        /**
         * <summary>
         * Get object hash code to participate in the consistent hash generation.</summary>
         *
         * <returns>A hash code for the object.</returns>
         */
        int GetHashCode();
    }
}
