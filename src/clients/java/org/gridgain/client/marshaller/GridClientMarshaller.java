// Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.marshaller;

import java.io.*;

/**
 * Marshaller for binary protocol messages.
 *
 * @author 2012 Copyright (C) GridGain Systems
 * @version 4.0.0c.24032012
 */
public interface GridClientMarshaller {
    /**
     * Marshals object to byte array.
     *
     * @param obj Object to marshal.
     * @return Byte array.
     * @throws IOException If marshalling failed.
     */
    public byte[] marshal(Object obj) throws IOException;

    /**
     * Unmarshalls object from byte array.
     *
     * @param bytes Byte array.
     * @return Unmarshalled object.
     * @throws IOException If unmarshalling failed.
     */
    public <T> T unmarshal(byte[] bytes) throws IOException;
}
