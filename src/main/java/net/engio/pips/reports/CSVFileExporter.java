package net.engio.pips.reports;

import net.engio.pips.data.IDataCollector;
import net.engio.pips.lab.Benchmark;

import java.io.File;
import java.io.PrintWriter;

/**
 * @author bennidi
 *         Date: 2/27/14
 */
public class CSVFileExporter implements IReporter {

    public void generate(Benchmark benchmark) throws Exception {
        String reportDirectory = benchmark.getReportBaseDir();
        File report = new File(reportDirectory + "report.txt");
        PrintWriter writer = new PrintWriter(report);
        try {

            // write report header
            writer.println("###### EXPERIMENT ##########");
            writer.println(benchmark);

            // write data of collectors
            writer.println();
            writer.println("##### COLLECTORS ########");
            for (IDataCollector collector: benchmark.getCollectors()) {
                writer.println(collector);
            }


        } finally {
            writer.close();
        }
    }

}
