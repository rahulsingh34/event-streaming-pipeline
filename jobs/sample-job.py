from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction

class SquareMapFunction(MapFunction):
    def map(self, value):
        return value * value

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Create a data stream with some test values
data_stream = env.from_collection([1, 2, 3, 4, 5])

# Apply a map function to square the numbers
squared_stream = data_stream.map(SquareMapFunction())

# Print the output to the console
squared_stream.print()

# Execute the Flink job
env.execute("Simple Flink Test Job")
