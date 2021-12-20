"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

        
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

input_topic_name = "org.chicago.cta.stations"
topic = app.topic(input_topic_name, value_type=Station)

output_topic_name = "org.chicago.cta.stations.table.v1"
out_topic = app.topic(output_topic_name, partitions=1)

table = app.Table(
   name=output_topic_name,
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def process_stations_events(events):
    async for ev in events:
        line = get_color(ev)
        
        table[ev.station_id] = TransformedStation(
            station_id=ev.station_id,
            station_name=ev.stop_name,
            order=ev.order,
            line=line
        )

def get_color(event):
    if event.blue is True:
        return 'blue'
    if event.green is True:
        return 'green'
    if event.red is True:
        return 'red';

    return None




if __name__ == "__main__":
    app.main()
