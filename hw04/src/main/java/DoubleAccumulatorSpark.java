import org.apache.spark.AccumulatorParam;

class DoubleAccumulatorSpark implements AccumulatorParam<Double> {
    @Override
    public Double zero(Double initialValue) {
        return 0.;
    }

    @Override
    public Double addInPlace(Double left, Double right) {
        return left + right;
    }

    @Override
    public Double addAccumulator(Double left, Double right) {
        return left + right;
    }
}