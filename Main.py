from Config import Config
import handler.Metric as Metric

if __name__ == "__main__":
    config = Config()
    Metric.collect(config)
    pass