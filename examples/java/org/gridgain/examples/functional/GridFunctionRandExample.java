// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.functional;

import org.gridgain.grid.typedef.*;

/**
 * Demonstrates various functional APIs from {@link org.gridgain.grid.lang.GridFunc} class.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridFunctionRandExample {
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

        // Array initialisation.
        String[] names = {
            "Neo", "Trinity", "The Oracle", "Switch", "Tank", "Dozer", "Morpheus", "Cypher", "Mouse"
        };

        // Call random value and print result.
        System.out.print("My favourite Matrix character is: " + F.rand(names));
    }
}
