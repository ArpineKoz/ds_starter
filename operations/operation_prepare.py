import json
import pandas as pd
from src.prepare_target import AlarmGeneration


print("starting target generation")
### read configs for preparation
with open("configurations/source_configurations.json") as json_data_file:
    data_config = json.load(json_data_file)

with open("configurations/config.json") as json_data_file:
    config = json.load(json_data_file)

### read raw input files
generation_data=pd.read_csv(data_config["raw_datasets"][plant]["generation_data"])
generation_data=pd.read_csv(data_config["raw_datasets"][plant]["weather_data"])

### make transformation
alram_generator_obj=AlarmGeneration(config["alarm_generator_config"])
full_data_with_alarms, daily_data_with_alarms = alram_generator_obj.generate_alarms(generation, weather_data)

### write data
full_data_with_alarms.to_csv(data_config["labeled_datasets"][plant]["full_data"])
daily_data_with_alarms.to_csv(data_config["labeled_datasets"][plant]["daily_data"])


##job finished message
