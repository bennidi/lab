package net.engio.pips.reports;

import net.engio.pips.data.IDataCollector;
import net.engio.pips.data.utils.TimeBasedAggregator;
import net.engio.pips.lab.Benchmark;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
* A series group is used to configure a set of data collectors for being included in a time series
 * chart. Apart from the collectors themselves, aggregates can be configured to generate series
 * like a moving average from the set of collectors.
*
* @author bennidi
*         Date: 3/4/14
*/
public class SeriesGroup {

    public static enum Orientation{
        Left, Right
    }

    private String label;

    private Collection<IDataCollector> collectors = new ArrayList<IDataCollector>();

    private int size;

    private String yAxis = "";

    private Orientation orientation = Orientation.Left;

    public SeriesGroup(String label) {
        this.label = label;
    }

    public SeriesGroup setYAxisOrientation(Orientation orientation){
         this.orientation = orientation;
         return this;
    }

    public Orientation getOrientation() {
        return orientation;
    }

    public SeriesGroup addCollector(IDataCollector collector){
        collectors.add(collector);
        return this;
    }

    public SeriesGroup addCollectors(List<IDataCollector> collectors){
        for(IDataCollector collector : collectors)
            addCollector(collector);
        return this;
    }

    public String getLabel() {
        return label;
    }

    public int getSize() {
        return size;
    }

    public String getyAxis() {
        return yAxis;
    }

    public SeriesGroup setyAxis(String yAxis) {
        this.yAxis = yAxis;
        return this;
    }



    public TimeSeriesCollection createDataSet(Benchmark benchmark){
        TimeSeriesCollection collection = new TimeSeriesCollection();
        TimeBasedAggregator aggregator = new TimeBasedAggregator();
        // create a series from each data collector
        for(IDataCollector collector : collectors){
            // ignore empty data collectors as well as according to sample size
            if(collector == null || collector.size() == 0)continue;
            TimeSeriesConsumer wrapper = new TimeSeriesConsumer(collector.getId());
            collector.feed(wrapper);
            TimeSeries series = wrapper.getSeries();
            collection.addSeries(series);
            if(size < collector.size())size = collector.size();
        }
        return collection;
    }
}
