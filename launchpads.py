import pandas as pd
import requests
import json


class LaunchPads:
    url = 'https://api.spacexdata.com/v4/launchpads'
    response = None
    response_text = None
    status_code = None
    response_content = None
    df_table_launchpad_rockets = None
    df_table_launchpads = None

    def __init__(self):
        pass

    def __init__(self):
        self.get_launchpads()
        self.transform_data()

    def get_launchpads(self):
        self.response = requests.get(self.url)
        self.response_text = self.response.text
        self.status_code = self.response.status_code
        self.response_content = self.response.content

    def transform_data(self):
        self.list_of_launchpads = json.loads(self.response_content.decode("utf-8"))
        self.df_launchpads = pd.json_normalize(self.list_of_launchpads)
        self.df_launchpads = self.df_launchpads.rename(columns={'id': 'launchpad_service_id'})
        self.df_launchpads = self.df_launchpads.sort_values(by=['launchpad_service_id'])
        self.df_launchpads['id'] = list(range(1, len(self.df_launchpads['launchpad_service_id']) + 1))
        self.df_launchpads = self.df_launchpads.drop(columns=['launches'])
        self.df_table_launchpad_rockets = self.create_df_table_launchpad_rockets()
        self.df_table_launchpads = self.create_df_table_launchpads()

    def create_df_table_launchpad_rockets(self):
        df_table_launchpad_rockets = self.df_launchpads[['id', 'rockets']]
        df_table_launchpad_rockets = df_table_launchpad_rockets.rename(
            columns={'id': 'launchpad_id', 'rockets': 'rocket'})
        df_table_launchpad_rockets = df_table_launchpad_rockets.explode('rocket', ignore_index=True)
        df_table_launchpad_rockets = df_table_launchpad_rockets.dropna(subset=['rocket'])
        df_table_launchpad_rockets['id'] = list(range(1, len(df_table_launchpad_rockets['launchpad_id']) + 1))
        df_table_launchpad_rockets = df_table_launchpad_rockets[['id', 'launchpad_id', 'rocket']]
        return df_table_launchpad_rockets

    def create_df_table_launchpads(self):
        df_table_launchpads = self.df_launchpads[
            ['id', 'launchpad_service_id', 'name', 'full_name', 'locality', 'region', 'latitude', 'longitude',
             'launch_attempts', 'launch_successes', 'timezone', 'status',
             'details', 'images.large']]
        return df_table_launchpads


if __name__ == '__main__':
    launchpad = LaunchPads()
    print('X')
