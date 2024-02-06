import random
import time
import math
import typing
import json

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, ValueState


class ExampleEvent:
    def __init__(self, id: int, timestamp: int):
        self.id = id
        self.timestamp = timestamp

    def __str__(self):
        return json.dumps(self.__dict__)


class ExampleEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value.timestamp


class DeduplicationOperator(KeyedProcessFunction):
    def __init__(self, time_range: int):
        self.unique_event: typing.Union[None, ValueState] = None
        self.time_range = time_range

    def open(self, runtime_context: RuntimeContext):
        self.unique_event = (
            runtime_context
            .get_state(ValueStateDescriptor("UniqueEvent", Types.PICKLED_BYTE_ARRAY()))
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if self.unique_event.value() is None:
            self.unique_event.update(value)
            ctx.timer_service().register_event_time_timer(ctx.timestamp() + self.time_range)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        if self.unique_event.value() is not None:
            yield self.unique_event.value()
            self.unique_event.clear()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    rand = random.Random()

    random_numbers = [
        (rand.randint(1, 10), math.floor(index + (time.time() * 1000)))
        for index in range(10000)
    ]

    random_source = env.from_collection(random_numbers)
    random_examples = random_source.map(lambda event: ExampleEvent(event[0], event[1]))
    watermarked_examples = random_examples.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(ExampleEventTimestampAssigner())
    )

    unique_events = (
        watermarked_examples
        .key_by(lambda event: event.id)
        .process(DeduplicationOperator(10000))
    )

    values = unique_events.execute_and_collect()
    for i in values:
        print(i)


if __name__ == '__main__':
    main()
