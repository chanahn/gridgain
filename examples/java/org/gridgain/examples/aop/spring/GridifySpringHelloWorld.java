// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.aop.spring;

import org.gridgain.grid.gridify.*;
import org.gridgain.grid.typedef.*;

/**
 * Example stateful bean that simply prints out its state inside
 * of {@link #sayIt()} method. This bean is initialized from
 * {@code "spring-bean.xml"} Spring configuration file.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridifySpringHelloWorld {
    /** Example state. */
    private String phrase;

    /**
     * Gets example state.
     *
     * @return Example state.
     */
    public String getPhrase() {
        return phrase;
    }

    /**
     * Sets phrase to print.
     *
     * @param phrase Phrase to print.
     */
    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }

    /**
     * Method grid-enabled with {@link Gridify} annotation and will
     * be executed on the grid. It simply prints out the 'phrase'
     * set in this instance) and returns the number of characters
     * in the phrase.
     *
     * @return Number of characters in the {@code 'phrase'} string.
     */
    @Gridify public int sayIt() {
        // Simply print out the argument.
        X.println(">>>");
        X.println(">>> Printing '" + phrase + "' on this node from grid-enabled method.");
        X.println(">>>");

        return phrase.length();
    }
}
