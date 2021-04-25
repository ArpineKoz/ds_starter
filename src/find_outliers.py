import sys, os
import pandas as pd
from sklearn.linear_model import LinearRegression

class OutlierDetector:

    def __init__(self, weather_data, generation_data):

        
        weather_cols = ['DATE_TIME', 'PLANT_ID', 'SOURCE_KEY', 
                        'AMBIENT_TEMPERATURE', 'MODULE_TEMPERATURE', 'IRRADIATION']
        for col in weather_cols:
            if col not in weather_data.columns:
                raise Exception(f'Weather dataset must contain "{col}" column')
        
        generation_cols = ['DATE_TIME', 'PLANT_ID', 'SOURCE_KEY', 'DC_POWER', 
                            'AC_POWER', 'DAILY_YIELD', 'TOTAL_YIELD']
        for col in generation_cols:
            if col not in generation_data.columns:
                raise Exception(f'Generation dataset must contain "{col}" column')

        self.weather_data = weather_data
        self.generation_data = generation_data
        self.weather_data['DATE_TIME']= pd.to_datetime(self.weather_data['DATE_TIME'])
        self.generation_data['DATE_TIME']= pd.to_datetime(self.generation_data['DATE_TIME'])

        self.weather_data['DAY'] = pd.DatetimeIndex(self.weather_data['DATE_TIME']).dayofyear
        self.weather_data['TIME'] = self.weather_data.DATE_TIME.dt.hour * 60 + weather_data.DATE_TIME.dt.minute
        self.weather_data['Time'] = self.weather_data.DATE_TIME.dt.time
        self.weather_data['HOUR'] = self.weather_data.DATE_TIME.dt.hour

        self.merged_data = pd.merge(self.generation_data, self.weather_data, how='inner', on=['DATE_TIME'], suffixes=('', '_y'))

    def find_cloudiness(self, col_name):
        agg_h_irr = self.weather_data.groupby(self.weather_data.TIME).agg({col_name: 'max'})
        #Removing outliers 
        clean_data=self.weather_data.copy()
        for i, j in  clean_data.groupby(clean_data.HOUR):
            outlier_condition1=(j[col_name]!=0)&(j[col_name]>=(j[col_name].mean()+ 2*j[col_name].std()))
            outlier_condition2=(j[col_name]!=0)&(j[col_name]<(j[col_name].mean()- 2*j[col_name].std()))
            clean_data.drop(clean_data.loc[(clean_data['DAY'].isin(j.loc[outlier_condition1|outlier_condition2].DAY))&
                                        (clean_data['HOUR']==i)].index, inplace=True)
        agg_h_irr_clean = clean_data.groupby(clean_data.TIME).agg({col_name: 'max'})
        weather_maxirr = pd.merge(self.weather_data, agg_h_irr_clean, how='inner', on='TIME', suffixes=('', '_max'))
        weather_maxirr['offset_from_max']=(weather_maxirr[col_name+'_max']-weather_maxirr[col_name])**2
        C_day_list=weather_maxirr.groupby('DAY').sum()['offset_from_max']
        #weather_maxirr = pd.merge(weather_maxirr, C_day_list, how='inner', on='DAY')
        #weather_maxirr_new = weather_maxirr.rename(columns={'offset_from_max_y': 'C_day', 
        #                                                    'offset_from_max_x': 'offset_from_max'})
        output=pd.DataFrame(C_day_list)
        output.columns=['cloudiness']
        output.reset_index(inplace=True)
        return output

    def get_outliers_in_time(self, column, groupby_column, output_column_name=False, outlier_limit=3):
        if output_column_name==False:
            output_column_name=column+"_outliers"
        self.merged_data[output_column_name]=0
        for _, group in  self.merged_data.groupby(self.merged_data[groupby_column]):
            outlier_condition1=group[column]>(group[column].mean() + outlier_limit*group[column].std())
            outlier_condition2=group[column]<(group[column].mean() - outlier_limit*group[column].std())
            self.merged_data.loc[group[outlier_condition1|outlier_condition2].index, output_column_name]=1
        return self.merged_data

    def get_outliers_by_residual(self, x_column, y_column, output_column_name="residual_outliers", anomaly_limit=5):
        self.merged_data[output_column_name]=0
        for i in self.merged_data.SOURCE_KEY.unique():
            inv_data=self.merged_data[self.merged_data.SOURCE_KEY==i]
            for _, day in inv_data.groupby(inv_data.DAY):
                X = self.merged_data[x_column].values.reshape(-1,1)
                y = self.merged_data[y_column].values.reshape(-1,1)
                regressor = LinearRegression(fit_intercept=False)
                regressor.fit(X, y)
                m=regressor.coef_[0]
                residual=(day[y_column]-m*day[x_column])**2 # or we can take the absolute value
                mean_res = residual.mean()
                stand_dev = residual.std()
                self.merged_data.loc[day[(residual>mean_res+anomaly_limit*stand_dev) | (residual<mean_res-anomaly_limit*stand_dev)].index, output_column_name] = 1
        return self.merged_data

    @staticmethod
    def get_conversion_coefficients(data, x_column, y_column, output_column_name="conversion_coefficient"):
        output_df=pd.DataFrame(columns=['SOURCE_KEY', 'DAY', output_column_name])
        for i in data.SOURCE_KEY.unique():
            inv_data=data[data.SOURCE_KEY==i]
            for a, day in inv_data.groupby(inv_data.DAY):
                X = day[x_column].values.reshape(-1,1)
                y = day[y_column].values.reshape(-1,1)
                regressor = LinearRegression(fit_intercept=False)
                regressor.fit(X, y)
                m=regressor.coef_[0][0]
                output_df=output_df.append(pd.DataFrame({'SOURCE_KEY': [i], 'DAY': [a], output_column_name: [m]}), ignore_index=True)
        return output_df

    def negative_trend_by_days(data, column, cloudiness, output_column_name, window=10, limit=-0.75):
        """
            Given a dataframe of inverter daily efficiencies calculates negative trends over a window of days. 
            Returns the dataframe with a new column, with 1 where the negative trend is below a certain limit.
        """
        data=data.merge(cloudiness, on='DAY')
        data['efficiency_trend']=0
        data['weather_trend']=0
        data[output_column_name]=0
        for inv in data.SOURCE_KEY.unique():
            data.loc[data.SOURCE_KEY==inv, 'efficiency_trend']=(data[data.SOURCE_KEY==inv]['DAY']/data[data.SOURCE_KEY==inv]['DAY'].max()).rolling(window=window).corr(data[data.SOURCE_KEY==inv][column]/data[data.SOURCE_KEY==inv][column].max())
            data.loc[data.SOURCE_KEY==inv, 'weather_trend']=(data[data.SOURCE_KEY==inv][column]/data[data.SOURCE_KEY==inv][column].max()).rolling(window=window).corr(data[data.SOURCE_KEY==inv]['cloudiness']/data[data.SOURCE_KEY==inv]['cloudiness'].max())
        data.loc[(data['efficiency_trend']<=limit)&(data['weather_trend']<=0), output_column_name]=1
        ####
        return data.drop(columns=['efficiency_trend', 'weather_trend', 'cloudiness'])

    @staticmethod
    def efficiency_drop_by_day(data, column, outlier_coeff, drop_column_name, jump_column_name, window=20):
        """
            Given a dataframe of inverter daily efficiencies calculates efficiency jumps and falls relative to inverter history.
        """
        if drop_column_name:
            data[drop_column_name]=0
        if jump_column_name:
            data[jump_column_name]=0
        data['efficiency_lower_limit']=0
        data['efficiency_higher_limit']=0
        if window:
            for inv in data.SOURCE_KEY.unique():
                data.loc[data.SOURCE_KEY==inv, 'efficiency_lower_limit']=data[data.SOURCE_KEY==inv][column].rolling(window=window).median()-outlier_coeff*data[data.SOURCE_KEY==inv][column].rolling(window=window).std()
                data.loc[data.SOURCE_KEY==inv, 'efficiency_higher_limit']=data[data.SOURCE_KEY==inv][column].rolling(window=window).median()+outlier_coeff*data[data.SOURCE_KEY==inv][column].rolling(window=window).std()
        else:
            for inv in data.SOURCE_KEY.unique():
                data.loc[data.SOURCE_KEY==inv, 'efficiency_lower_limit']=data[data.SOURCE_KEY==inv][column].median()-outlier_coeff*data[data.SOURCE_KEY==inv][column].std()
                data.loc[data.SOURCE_KEY==inv, 'efficiency_higher_limit']=data[data.SOURCE_KEY==inv][column].median()+outlier_coeff*data[data.SOURCE_KEY==inv][column].std()
        if drop_column_name:
            data.loc[data[column]<data['efficiency_lower_limit'], drop_column_name]=1
        if jump_column_name:
            data.loc[data[column]>data['efficiency_higher_limit'], jump_column_name]=1
        return data.drop(columns=['efficiency_lower_limit', 'efficiency_higher_limit'])

    @staticmethod
    def get_inefficient_inverters_day(data, column, output_column_name, anomaly_limit = 2):
        data[output_column_name] = 0
        for _,d in data.groupby('DAY'):
            col_mean = d[column].mean()
            col_std = d[column].std()
            data.loc[d[(d[column] > col_mean + anomaly_limit * col_std) | 
                (d[column] < col_mean - anomaly_limit * col_std)].index,output_column_name] = 1
        return data

    @staticmethod
    def get_inefficient_inverters_window(data, column, output_column_name, window=7):
        data[output_column_name] = 0
        for inv in data['SOURCE_KEY'].unique():
            data.loc[data['SOURCE_KEY']==inv, output_column_name]=data[data['SOURCE_KEY']==inv][column].rolling(window = window).min()
        return data

def main(weather_path, generation_path):
    if not os.path.isfile(weather_path):
        raise Exception(f"{weather_path} is not a dataset. Please make sure that the specifced path is correct.") 
    if not os.path.isfile(generation_path):
        raise Exception(f"{generation_path} is not a dataset. Please make sure that the specifced path is correct.") 
    weather_df = pd.read_csv(weather_path)
    generation_df = pd.read_csv(generation_path)

    outlier_detector = OutlierDetector(weather_df, generation_df)



if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise Exception("Please specify the weather and generation datasets")
    main(weather_path=sys.argv[1], generation_path=sys.argv[2])