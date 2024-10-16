import json
from typing import Self, TypedDict

class InfluxDBConf(TypedDict):
    token: str
    url: str
    org: str
    database: str

class GlobalConf(TypedDict):
    influxdb: InfluxDBConf

    @classmethod
    def load(cls, conf_path: str) -> Self:
        with open(conf_path, "r") as conf_file:
            conf_json: GlobalConf = json.load(conf_file)
            return conf_json