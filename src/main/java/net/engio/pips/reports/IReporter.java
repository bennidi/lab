package net.engio.pips.reports;

import net.engio.pips.lab.Benchmark;

/**
 * Take the benchmark and create some output. This may be everything from logging
 * to console, writing to disk, creating charts...
 *
 * @author bennidi
 *         Date: 3/4/14
 */
public interface IReporter {

    void generate(Benchmark benchmark) throws Exception;
}
