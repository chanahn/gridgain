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
public class GridFunctionViewExample {
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
        Random rand = new Random();

        final int size = 20;

        Collection<Integer> nums = new ArrayList<Integer>(size);

        // Generate list of random integers.
        for (int i = 0; i < size; i++) {
            nums.add(rand.nextInt(size));
        }

        // Print generated list.
        X.println("Generated list:");

        F.forEach(nums, F.<Integer>print("", " "));

        // Get new unmodifiable collection with elements which value low than half generated list size.
        Collection<Integer> view = F.view(nums, new P1<Integer>() {
            @Override public boolean apply(Integer i) {
                return i < size / 2;
            }
        });

        // Print result.
        X.println("\nResult list:");

        F.forEach(view, F.<Integer>print("", " "));

        // Check for read only.
        try {
            view.add(12);
        }
        catch (Exception ignored) {
            X.println("\nView is read only.");
        }
    }
}
