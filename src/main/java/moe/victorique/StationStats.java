package moe.victorique;

public class StationStats {
    private long min;
    private long max;
    private long sum;
    private long count;

    public StationStats(long initialTemp) {
        this.min = initialTemp;
        this.max = initialTemp;
        this.sum = initialTemp;
        this.count = 1;
    }

    public void update(long temp) {
        min = Math.min(min, temp);
        max = Math.max(max, temp);
        sum += temp;
        count++;
    }

    public StationStats merge(StationStats other) {
        this.min = Math.min(this.min, other.min);
        this.max = Math.max(this.max, other.max);
        this.sum += other.sum;
        this.count += other.count;
        return this;
    }

    public double getMin() {
        return min / 10.0;
    }

    public double getMax() {
        return max / 10.0;
    }

    public double getAverage() {
        return (sum / 10.0) / count;
    }

    @Override
    public String toString() {
        return getMin() + "/" + getAverage() + "/" + getMax();
    }
}
