from Config import Config
import handler.Metric as Metric
import handler.Trace as Trace
import handler.Log as Log


if __name__ == "__main__":
    config = Config()
    Metric.collect(config)
    Log.collect(config)
    Trace.collect(config)