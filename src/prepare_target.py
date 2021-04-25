##import...




class AlarmGeneration():
    def __init__(self, **kwargs):
        self.datetime=kwargs.get("datetime", "DATE_TIME")
        ##other variables

    def __get_cloudy_measure(self, data):


        return data

    def __get_inverter_daily_efficiency(self, data, datavariable_x, variable_y):


        return data, daily_data
    
    def __get_inverter_efficiency_drops(self, data):


        return daily_data
    
    def __get_inverter_efficiency_trends(self, data):


        return daily_data

    def __get_inverter_efficiency_differences(self, data):


        return daily_data


    def __get_conversion_outliers(self, data):


        return data


    def __get_power_inefficiencies(self, data):

        data, daily_data=self.__get_inverter_daily_efficiency(data, "irr", "DC")
        daily_data=self.__get_inverter_efficiency_drops(daily_data)
        daily_data=self.__get_inverter_efficiency_trends(daily_data)
        daily_data=self.__get_inverter_efficiency_differences(daily_data)


        return data, daily_data


    def __get_power_conversion_inefficiencies(self, data):

        data=self.__get_conversion_outliers(data)
        data, daily_data=self.__get_inverter_daily_efficiency(data, "DC", "AC")
        daily_data=self.__get_inverter_efficiency_drops(daily_data)
        daily_data=self.__get_inverter_efficiency_trends(daily_data)
        daily_data=self.__get_inverter_efficiency_differences(daily_data)


        return data, daily_data
    

    def generate_alarms(self, data):

        ##call power generation alarms
        data_with_alarms, daily_data_with_alarms = self.__get_power_inefficiencies(data)
        ##call DC to AC conversion alarms
        data_with_alarms, daily_power_data_with_alarms = self.__get_power_conversion_inefficiencies(data_with_alarms)

        #join daily_data_with_alarms and daily_power_data_with _alarms

        return data_with_alarms, daily_data_with_alarms