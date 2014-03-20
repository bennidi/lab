package net.engio.pips.reports;

import net.engio.pips.data.DataPoint;
import net.engio.pips.data.DataProcessor;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;

/**
 * @author bennidi
 *         Date: 2/25/14
 */
public class TimeSeriesConsumer<N extends Number> extends DataProcessor<N,N>{

    private TimeSeries series;

    private String label;

    public TimeSeriesConsumer(String label) {
       series = new TimeSeries(label);
        this.label = label;
    }

    @Override
    public void receive(DataPoint<N> datapoint) {
        series.addOrUpdate(new FixedMillisecond(datapoint.getTsCreated()), datapoint.getValue());
        emit(datapoint);
    }


    public TimeSeries getSeries() {
        return series;
    }


    public String getLabel() {
        return label;
    }
}
