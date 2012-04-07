// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using GridGain.Client.Balancer;

    /** <summary>Java client data projection configuration.</summary> */
    public interface IGridClientDataConfiguration {
        /** <summary>Remote cache name.</summary> */
        String Name {
            get;
        }

        /** <summary>Cache affinity to use.</summary> */
        IGridClientDataAffinity Affinity {
            get;
        }

        /** <summary>Node balancer for pinned mode.</summary> */
        IGridClientLoadBalancer PinnedBalancer {
            get;
        }
    }
}
