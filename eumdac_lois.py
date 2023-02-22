import eumdac
import shutil
import os
from datetime import datetime as dt
from datetime import timedelta


def authorize():
    try:
        consumer_key = 'OZLzfHLwVY68az_e46kBXBfJFWoa'
        consumer_secret = 'EsfltgYk83QzwsG7FfUowspe4Gka'
        credentials = (consumer_key, consumer_secret)
        token = eumdac.AccessToken(credentials)
        print(f"[INFO] This token '{token}' expires {token.expiration}")

    except:
        token = None

    return token


class EUMDAC_LOIS:

    def __init__(self, verbose):
        self.token = authorize()
        if self.token is None:
            print('[ERROR] Authorization failed. Please review credentials')
        self.verbose = verbose
        self.olci_collections = {
            'EO:EUM:DAT:0409': {
                'title': 'OLCI Level 1B Full Resolution - Sentinel-3',
                'resolution': 'FR',
                'level': 'L1B',
                'start_date': '2016-10-20',
                'end_date': 'TODAY',
            },
            'EO:EUM:DAT:0577': {
                'title': 'OLCI Level 1B Full Resolution (version BC002) - Sentinel-3 - Reprocessed',
                'resolution': 'FR',
                'level': 'L1B',
                'start_date': '2016-04-25',
                'end_date': '2019-10-29',
                'baseline': '002'
            },
            'EO:EUM:DAT:0407': {
                'title': 'OLCI Level 2 Ocean Colour Full Resolution - Sentinel-3',
                'resolution': 'FR',
                'level': 'L2',
                'start_date': '2017-07-05',
                'end_date': 'TODAY'
            },
            'EO:EUM:DAT:0592': {
                'title': 'OLCI Level 2 Ocean Colour Full Resolution (version BC002) - Sentinel-3 - Reprocessed',
                'resolution': 'FR',
                'level': 'L2',
                'start_date': '2016-04-25',
                'end_date': '2017-11-29',
                'baseline': '002'
            },
            'EO:EUM:DAT:0556': {
                'title': 'OLCI Level 2 Ocean Colour Full Resolution (version BC003) - Sentinel-3 - Reprocessed',
                'resolution': 'FR',
                'level': 'L2',
                'start_date': '2016-04-25',
                'end_date': '2021-04-28',
                'baseline': '003'
            },
            'EO:EUM:DAT:0410': {
                'title': 'OLCI Level 1B Reduced Resolution - Sentinel-3',
                'resolution': 'RR',
                'level': 'L1B',
                'start_date': '2016-10-20',
                'end_date': 'TODAY'
            },
            'EO:EUM:DAT:0578': {
                'title': 'OLCI Level 1B Reduced Resolution (version BC002) - Sentinel-3 - Reprocessed',
                'resolution': 'RR',
                'level': 'L1B',
                'start_date': '2016-04-25',
                'end_date': '2019-10-29',
                'baseline': '002'
            },
            'EO:EUM:DAT:0408': {
                'title': 'OLCI Level 2 Ocean Colour Reduced Resolution - Sentinel-3',
                'resolution': 'RR',
                'level': 'L2',
                'start_date': '2017-07-05',
                'end_date': 'TODAY'
            },
            'EO:EUM:DAT:0593': {
                'title': 'OLCI Level 2 Ocean Colour Reduced Resolution (version BC002) - Sentinel-3 - Reprocessed',
                'resolution': 'RR',
                'level': 'L2',
                'start_date': '2016-04-25',
                'end_date': '2017-11-29',
                'baseline': '002'
            },
            'EO:EUM:DAT:0557': {
                'title': 'OLCI Level 2 Ocean Colour Reduced Resolution (version BC003) - Sentinel-3 - Reprocessed',
                'resolution': 'RR',
                'level': 'L2',
                'start_date': '2016-04-25',
                'end_date': '2021-04-28',
                'baseline': '003'
            }
        }

        # datastore = eumdac.DataStore(self.token)
        # # self.get_all_available_collections(True)
        # # col = datastore.get_collection('EO:EUM:DAT:0409')
        # for collection in self.olci_collections:
        #     if collection == 'EO_EUM:DAT:0407':
        #         continue
        #     selected_collection = datastore.get_collection(collection)
        #     print(self.olci_collections[collection]['title'])
        #     print(self.olci_collections[collection]['resolution'])
        #     print(self.olci_collections[collection]['level'])
        #     print(self.olci_collections[collection]['start_date'])
        #     print(self.olci_collections[collection]['end_date'])
        #     print(selected_collection.metadata['properties']['date'])
        #     print('----------------------------------')

    def get_olci_collection(self, date, resolution, level, onlynt, onlyreproc):
        date, datestr = self.resolve_date_param(date)
        if date is None:
            return None
        resolution = resolution.upper()
        level = level.upper()
        resolutions = ['RR', 'FR']
        levels = ['L1B', 'L2']
        if resolution not in resolutions:
            print(f'[ERROR] {resolution} is not a valid resolution. Possible values: FR or RR')
            return None
        if level not in levels:
            print(f'[ERROR] {level} is not a valid resolution. Possible values: L1B or L2')
            return None

        baseline = '003'
        if resolution == 'RR':
            baseline = '002'
        collections_out = ['NO', 'NO']  # 0:nrt collection #1:reproc collection
        for collection in self.olci_collections:
            if resolution != self.olci_collections[collection]['resolution']:
                continue
            if level != self.olci_collections[collection]['level']:
                continue
            start_date = dt.strptime(self.olci_collections[collection]['start_date'], '%Y-%m-%d')
            if self.olci_collections[collection]['end_date'] == 'TODAY':
                end_date = dt.now()
            else:
                end_date = dt.strptime(self.olci_collections[collection]['end_date'], '%Y-%m-%d')
            if start_date <= date <= end_date:
                if 'baseline' in self.olci_collections[collection]:  # reproc
                    if baseline == self.olci_collections[collection]['baseline']:
                        collections_out[1] = collection
                else:  # nrt
                    collections_out[0] = collection

        if onlynt:
            if collections_out[0] == 'NO':
                print(f'[WARNING] NR/NT Collection was not available for date: {datestr}')
                return None
            else:
                return collections_out[0]
        if onlyreproc:
            if collections_out[1] == 'NO':
                print(f'[WARNING] REPROC Collection was not available for date: {datestr}')
                return None
            else:
                return collections_out[1]

        if collections_out[0] == 'NO' and collections_out[1] == 'NO':
            print(f'[WARNING] REPROC or NR/NT Collection were not available for date: {datestr}')
            return None
        if collections_out[1] != 'NO':
            return collections_out[1]
        if collections_out[0] != 'NO':
            return collections_out[0]

    def search_olci_by_point(self, date, resolution, level, lat_point, lon_point, hourmin, hourmax):
        list_products = []
        collection_id = self.get_olci_collection(date, resolution, level, False, False)
        products = None
        if collection_id is None:
            return products, list_products, collection_id
        if self.verbose:
            print(f'[INFO] Collection ID: {collection_id}')

        # GEOGRAPHIC AREA
        geo = self.get_geo_from_point(lat_point, lon_point)
        if geo is None:
            return products, list_products, collection_id
        if self.verbose:
            print(f'[INFO] Geographic area: {geo}')

        # DATE
        date, datestr = self.resolve_date_param(date)
        datemin, datemax = self.get_date_min_max_from_date(date, hourmin, hourmax)
        if datemin is None or datemax is None:
            return products, list_products, collection_id
        if self.verbose:
            print(f'[INFO] Search date min: {datemin} Search date max: {datemax}')

        # PRODUCTS
        datastore = eumdac.DataStore(self.token)
        selected_collection = datastore.get_collection(collection_id)
        products = selected_collection.search(geo=geo, dtstart=datemin, dtend=datemax)

        if self.verbose:
            print(f'[INFO] {len(products)} datasets found for the given area of interest')

        if len(products) == 0:
            print(
                f'[WARNING] No product found for S3 {level} {resolution}  for date {datestr}, lat: {lat_point}, lon:{lon_point}')
            return products, list_products, collection_id


        idataset = 1
        for product in products:
            list_products.append(str(product))
            if self.verbose:
                prename = f'[INFO][DATASET {idataset}] '
                print(f'{prename}{str(product)}')
                orbit_type = product.metadata['properties']['acquisitionInformation'][0]['platform']['orbitType']
                print(f'{prename}Orbit type: {orbit_type}')
                print(f'{prename}Instrument: {product.instrument}')
                print(f'{prename}Satellite: {product.satellite}')
                print(f'{prename}Sensing start: {str(product.sensing_start)}')
                print(f'{prename}Sensing end:: {str(product.sensing_end)}')
                print(f'{prename}Size: {str(product.size)}')
                idataset = idataset +1
                print("----------------------------------------")

        return products, list_products, collection_id

    def download_product(self,product,outputdir,overwrite):
        if self.verbose:
            print(f'[INFO] Starting download of product {product}...')
        with product.open() as fsrc, \
                open(os.path.join(outputdir,fsrc.name), mode='wb') as fdst:
            skip = False
            if overwrite:
                skip = os.path.exists(fdst)
            if not skip:
                shutil.copyfileobj(fsrc, fdst)
                if self.verbose:
                    print(f'[INFO] Download of product {product} finished.')
            else:
                if self.verbose:
                    print(f'[INFO] Product {product} already exist. Skipping download.')

    def download_product_from_product_list(self,products,outputdir,overwrite):
        for product in products:
            self.download_product(product,outputdir,overwrite)

    def get_date_min_max_from_date(self, date, hourmin, hourmax):
        if hourmin == -1:
            hourmin = 0
        if hourmax == -1:
            hourmax = 24

        date, datestr = self.resolve_date_param(date)
        datemin = date.replace(hour=hourmin, minute=0, second=0, microsecond=0)
        if hourmax == 24:
            datemax = date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=24)
        else:
            datemax = date.replace(hour=hourmax, minute=0, second=0, microsecond=0)

        return datemin, datemax

    def resolve_date_param(self, date):

        if isinstance(date, str):
            datestr = date
            try:
                date = dt.strptime(datestr, '%Y-%m-%d')
            except:
                print(f'[ERROR] Data {datestr} is not valid. Correct format: YYYY-mm-dd')
                return None, None
        else:
            datestr = date.strftime('%Y-%m-%d')

        return date, datestr

    def get_geo_from_point(self, lat_point, lon_point):
        bbox = [lat_point - 1, lat_point + 1, lon_point - 1, lon_point + 1]
        return self.get_geo_from_bbox(bbox)

    # bbox: latmin,latmax,lonmin,lonmax
    def get_geo_from_bbox(self, bbox):
        lat_min = bbox[0]
        lat_max = bbox[1]
        lon_min = bbox[2]
        lon_max = bbox[3]
        geometry = [[lon_min, lat_min], [lon_min, lat_max], [lon_max, lat_max], [lon_max, lat_min], [lon_min, lat_min]]
        geo = 'POLYGON(({}))'.format(','.join(["{} {}".format(*coord) for coord in geometry]))
        return geo

    def get_all_available_collections(self, onlyid):
        datastore = eumdac.DataStore(self.token)
        collection_dict = {}
        for collection in datastore.collections:
            if onlyid:
                print(str(collection))
            else:
                collection_dict[str(collection)] = collection.title
                print(collection, collection.title)
        return collection_dict
