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
    df_table_launch_links = None
    df_table_fairing_ships = None
    df_table_fairing_details = None
    df_table_launch_cores = None
    df_table_launch_payloads = None
    df_table_launch_capsules = None
    df_table_launch_ships = None
    df_table_launch_crew = None
    df_table_launch_failures = None
    df_table_launches = None

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
        self.df_table_launch_links = self.create_df_table_launch_links()
        self.df_table_fairing_ships = self.create_df_table_fairing_ships()
        self.df_table_fairing_details = self.create_df_table_fairing_details()
        self.df_table_launch_cores = self.create_df_table_launch_cores()
        self.df_table_launch_payloads = self.create_df_table_launch_payloads()
        self.df_table_launch_capsules = self.create_df_table_launch_capsules()
        self.df_table_launch_ships = self.create_df_table_launch_ships()
        self.df_table_launch_crew = self.create_df_table_launch_crew()
        self.df_table_launch_failures = self.create_df_table_launch_failures()
        self.df_table_launches = self.create_df_table_launches()

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

    def create_df_table_launch_links(self):
        df_table_launch_links = self.df_launches[
            ['id', 'links.patch.small', 'links.patch.large', 'links.reddit.campaign', 'links.reddit.launch',
             'links.reddit.media', 'links.reddit.recovery',
             'links.presskit', 'links.webcast', 'links.youtube_id', 'links.article', 'links.wikipedia']]
        df_table_launch_links = df_table_launch_links.rename(columns={'id': 'launch_id',
                                                                      'links.patch.small': 'small_patch',
                                                                      'links.patch.large': 'large_patch',
                                                                      'links.reddit.campaign': 'reddit_campaign',
                                                                      'links.reddit.launch': 'reddit_launch',
                                                                      'links.reddit.media': 'reddit_media',
                                                                      'links.reddit.recovery': 'reddit_recovery',
                                                                      'links.presskit': 'presskit',
                                                                      'links.webcast': 'webcast',
                                                                      'links.youtube_id': 'youtube_id',
                                                                      'links.article': 'article',
                                                                      'links.wikipedia': 'wikipedia'})
        df_table_launch_links['id'] = list(range(1, len(df_table_launch_links['launch_id']) + 1))
        cols = list(df_table_launch_links.columns)
        cols = [cols[-1]] + cols[:-1]
        df_table_launch_links = df_table_launch_links[cols]
        return df_table_launch_links

    def create_df_table_fairing_ships(self):
        df_table_fairing_ships = self.df_launches[['id', 'fairings.ships']]
        df_table_fairing_ships = df_table_fairing_ships.explode('fairings.ships')
        df_table_fairing_ships = df_table_fairing_ships.rename(columns={'id': 'launch_id', 'fairings.ships': 'ship'})
        df_table_fairing_ships = df_table_fairing_ships.dropna(subset=['ship'])
        df_table_fairing_ships['id'] = list(range(1, len(df_table_fairing_ships['launch_id']) + 1))
        df_table_fairing_ships = df_table_fairing_ships[['id', 'launch_id', 'ship']]
        return df_table_fairing_ships

    def create_df_table_fairing_details(self):
        df_table_fairing_details = self.df_launches[
            ['id', 'fairings.reused', 'fairings.recovery_attempt', 'fairings.recovered']]
        df_table_fairing_details = df_table_fairing_details.rename(
            columns={'id': 'launch_id', 'fairings.reused': 'reused', 'fairings.recovery_attempt': 'recovery_attempt',
                     'fairings.recovered': 'recovered'})
        df_table_fairing_details = df_table_fairing_details.dropna(subset=['reused', 'recovery_attempt', 'recovered'])
        df_table_fairing_details['id'] = list(range(1, len(df_table_fairing_details['launch_id']) + 1))
        cols = list(df_table_fairing_details.columns)
        cols = [cols[-1]] + cols[:-1]
        df_table_fairing_details = df_table_fairing_details[cols]
        return df_table_fairing_details

    def create_df_table_launch_cores(self):
        df_table_launch_cores = self.df_launches[['id', 'cores']]
        df_table_launch_cores = df_table_launch_cores.rename(columns={'id': 'launch_id'})
        df_table_launch_cores = df_table_launch_cores.explode('cores', ignore_index=True)
        df_table_launch_cores = pd.concat([df_table_launch_cores.drop(['cores'], axis=1),
                                           pd.json_normalize(df_table_launch_cores['cores'])], axis=1)
        df_table_launch_cores = df_table_launch_cores.dropna(subset=['core'])
        df_table_launch_cores['id'] = list(range(1, len(df_table_launch_cores['launch_id']) + 1))
        cols = list(df_table_launch_cores.columns)
        cols = [cols[-1]] + cols[:-1]
        df_table_launch_cores = df_table_launch_cores[cols]
        return df_table_launch_cores

    def create_df_table_launch_payloads(self):
        df_table_launch_payloads = self.df_launches[['id', 'payloads']]
        df_table_launch_payloads = df_table_launch_payloads.rename(columns={'id': 'launch_id', 'payloads': 'payload'})
        df_table_launch_payloads = df_table_launch_payloads.explode('payload', ignore_index=True)
        df_table_launch_payloads = df_table_launch_payloads.dropna(subset=['payload'])
        df_table_launch_payloads['id'] = list(range(1, len(df_table_launch_payloads['launch_id']) + 1))
        df_table_launch_payloads = df_table_launch_payloads[['id', 'launch_id', 'payload']]
        return df_table_launch_payloads

    def create_df_table_launch_capsules(self):
        df_table_launch_capsules = self.df_launches[['id', 'capsules']]
        df_table_launch_capsules = df_table_launch_capsules.rename(columns={'id': 'launch_id', 'capsules': 'capsule'})
        df_table_launch_capsules = df_table_launch_capsules.explode('capsule', ignore_index=True)
        df_table_launch_capsules = df_table_launch_capsules.dropna(subset=['capsule'])
        df_table_launch_capsules['id'] = list(range(1, len(df_table_launch_capsules['launch_id']) + 1))
        df_table_launch_capsules = df_table_launch_capsules[['id', 'launch_id', 'capsule']]
        return df_table_launch_capsules

    def create_df_table_launch_ships(self):
        df_table_launch_ships = self.df_launches[['id', 'ships']]
        df_table_launch_ships = df_table_launch_ships.rename(columns={'id': 'launch_id', 'ships': 'ship'})
        df_table_launch_ships = df_table_launch_ships.explode('ship', ignore_index=True)
        df_table_launch_ships = df_table_launch_ships.dropna(subset=['ship'])
        df_table_launch_ships['id'] = list(range(1, len(df_table_launch_ships['launch_id']) + 1))
        df_table_launch_ships = df_table_launch_ships[['id', 'launch_id', 'ship']]
        return df_table_launch_ships

    def create_df_table_launch_crew(self):
        df_table_launch_crew = self.df_launches[['id', 'crew']]
        df_table_launch_crew = df_table_launch_crew.rename(columns={'id': 'launch_id'})
        df_table_launch_crew = df_table_launch_crew.explode('crew', ignore_index=True)
        df_table_launch_crew = pd.concat([df_table_launch_crew.drop(['crew'], axis=1),
                                          pd.json_normalize(df_table_launch_crew['crew'])], axis=1)
        df_table_launch_crew = df_table_launch_crew.dropna(subset=['crew'])
        df_table_launch_crew['id'] = list(range(1, len(df_table_launch_crew['launch_id']) + 1))
        cols = list(df_table_launch_crew.columns)
        cols = [cols[-1]] + cols[:-1]
        df_table_launch_crew = df_table_launch_crew[cols]
        return df_table_launch_crew

    def create_df_table_launch_failures(self):
        df_table_launch_failures = self.df_launches[['id', 'failures']]
        df_table_launch_failures = df_table_launch_failures.rename(columns={'id': 'launch_id'})
        df_table_launch_failures = df_table_launch_failures.explode('failures', ignore_index=True)
        df_table_launch_failures = pd.concat([df_table_launch_failures.drop(['failures'], axis=1),
                                              pd.json_normalize(df_table_launch_failures['failures'])], axis=1)
        df_table_launch_failures = df_table_launch_failures.dropna(subset=['time'])
        df_table_launch_failures['id'] = list(range(1, len(df_table_launch_failures['launch_id']) + 1))
        cols = list(df_table_launch_failures.columns)
        cols = [cols[-1]] + cols[:-1]
        df_table_launch_failures = df_table_launch_failures[cols]
        return df_table_launch_failures

    def create_df_table_launches(self):
        df_table_launches = self.df_launches[
            ['id', 'launch_service_id', 'static_fire_date_utc', 'static_fire_date_unix', 'net', 'window',
             'rocket', 'success', 'details', 'launchpad', 'flight_number', 'name', 'date_utc',
             'date_unix', 'date_local', 'date_precision', 'upcoming',
             'auto_update', 'tbd', 'launch_library_id']]
        return df_table_launches


if __name__ == '__main__':
    launches = Launches()
    print('X')
