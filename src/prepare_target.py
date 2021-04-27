import pandas as pd


class AlarmGeneration():
    def __init__(self, **kwargs):
        self.datetime = kwargs.get("datetime", "DATE_TIME")
        # other variables

    def __get_cloudy_measure(self, data, col_name):
        # Removing outliers
        clean_data = data.copy()
        for i, j in clean_data.groupby(clean_data.HOUR):
            outlier_condition1 = (j[col_name] != 0) & (
                j[col_name] >= (j[col_name].mean() + 2*j[col_name].std()))
            outlier_condition2 = (j[col_name] != 0) & (
                j[col_name] < (j[col_name].mean() - 2*j[col_name].std()))
            clean_data.drop(clean_data.loc[(clean_data['DAY'].isin(j.loc[outlier_condition1 | outlier_condition2].DAY)) &
                                           (clean_data['HOUR'] == i)].index, inplace=True)
        agg_h_irr_clean = clean_data.groupby(
            clean_data.TIME).agg({col_name: 'max'})
        weather_maxirr = pd.merge(
            data, agg_h_irr_clean, how='inner', on='TIME', suffixes=('', '_max'))
        weather_maxirr['offset_from_max'] = (
            weather_maxirr[col_name+'_max']-weather_maxirr[col_name])**2
        C_day_list = weather_maxirr.groupby('DAY').sum()['offset_from_max']
        #weather_maxirr = pd.merge(weather_maxirr, C_day_list, how='inner', on='DAY')
        # weather_maxirr_new = weather_maxirr.rename(columns={'offset_from_max_y': 'C_day',
        #                                                    'offset_from_max_x': 'offset_from_max'})
        output = pd.DataFrame(C_day_list)
        output.columns = ['cloudiness']
        output.reset_index(inplace=True)
        return output

    def __get_inverter_daily_efficiency(self, data, variable_x, variable_y):

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

        data, daily_data = self.__get_inverter_daily_efficiency(
            data, "irr", "DC")
        daily_data = self.__get_inverter_efficiency_drops(daily_data)
        daily_data = self.__get_inverter_efficiency_trends(daily_data)
        daily_data = self.__get_inverter_efficiency_differences(daily_data)

        return data, daily_data

    def __get_power_conversion_inefficiencies(self, data):

        data = self.__get_conversion_outliers(data)
        data, daily_data = self.__get_inverter_daily_efficiency(
            data, "DC", "AC")
        daily_data = self.__get_inverter_efficiency_drops(daily_data)
        daily_data = self.__get_inverter_efficiency_trends(daily_data)
        daily_data = self.__get_inverter_efficiency_differences(daily_data)

        return data, daily_data

    def generate_alarms(self, data):

        # call power generation alarms
        data_with_alarms, daily_data_with_alarms = self.__get_power_inefficiencies(
            data)
        # call DC to AC conversion alarms
        data_with_alarms, daily_power_data_with_alarms = self.__get_power_conversion_inefficiencies(
            data_with_alarms)

        # join daily_data_with_alarms and daily_power_data_with _alarms

        return data_with_alarms, daily_data_with_alarms
