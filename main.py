import pandas as pd
import requests
import json


class Launches:
    url = 'https://api.spacexdata.com/v5/launches'
    response = None
    response_text = None
    status_code = None
    response_content = None
    list_of_launches = None
    df_launches = None
    df_table_flickr_links = None

    def __init__(self):
        self.get_launches()
        self.transform_data()

    def get_launches(self):
        self.response = requests.get(self.url)
        self.response_text = self.response.text
        self.status_code = self.response.status_code
        self.response_content = self.response.content

    def transform_data(self):
        self.list_of_launches = json.loads(self.response_content.decode("utf-8"))
        self.df_launches = pd.json_normalize(self.list_of_launches)
        self.df_launches = self.df_launches.rename(columns={'id': 'launch_service_id'})
        self.df_launches = self.df_launches.sort_values(by=['launch_service_id'])
        self.df_launches['id'] = list(range(1, len(self.df_launches['launch_service_id']) + 1))
        self.df_table_flickr_links = self.create_df_table_flickr_links()

    def create_df_table_flickr_links(self):
        df_table_flickr_links = self.df_launches[['id', 'links.flickr.small', 'links.flickr.original']]
        df_table_flickr_links = df_table_flickr_links.explode('links.flickr.original')
        df_table_flickr_links = df_table_flickr_links.dropna(subset=['links.flickr.original'])
        df_table_flickr_links = df_table_flickr_links.drop(columns=['links.flickr.small'])
        df_table_flickr_links['type'] = 'original'
        df_table_flickr_links = df_table_flickr_links.rename(columns={'id': 'launch_id',
                                                                      'links.flickr.original': 'link'})
        df_table_flickr_links['id'] = list(range(1, len(df_table_flickr_links['link']) + 1))
        df_table_flickr_links = df_table_flickr_links[['id', 'launch_id', 'link', 'type']]
        return df_table_flickr_links

    def load_data(self):
        pass


if __name__ == '__main__':
    launches = Launches()
    data = launches.list_of_launches
    print('X')
