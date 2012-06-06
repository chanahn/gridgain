// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.functional;

import org.gridgain.grid.typedef.*;
import java.util.*;

/**
 * Demonstrates various functional APIs from {@link org.gridgain.grid.lang.GridFunc} class.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridFunctionSizeExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) {
        // Typedefs:
        // ---------
        // G -> GridFactory
        // CI1 -> GridInClosure
        // CO -> GridOutClosure
        // CA -> GridAbsClosure
        // F -> GridFunc

        // Data initialisation.
        Collection<String> months = Arrays.asList("January", "February", "March", "April", "May", "June", "July",
            "August", "September", "October", "November", "December");

        final int length = 7;

        // Get number of elements which length greater than value of variable.
        int size = F.size(months,
            new P1<String>() { @Override public boolean apply(String s) { return s.length() > length; } }
        );

        // Print result.
        X.println("There are " + size + " elements which length greater than " + length);
    }
}
