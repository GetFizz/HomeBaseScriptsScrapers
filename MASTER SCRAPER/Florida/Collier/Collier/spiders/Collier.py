#########   Collier

import json
import csv
from copy import deepcopy

import scrapy

class Collier(scrapy.Spider):
    name = 'Collier'

    url = "https://www.collierappraiser.com/Main_Search/database_rp.asp?FullAddress={}&grid=parcels&expires=202403230800"

    detail_url = "https://www.collierappraiser.com/Main_Search/database_rp.asp?FolioID={}&expires=202403230800"

    data = {
        '_search': 'false',
        'rows': '15',   #        'rows': '15',
        'page': '1',
        'sidx': 'FullStreet asc, StreetNumber ',
        'sord': 'asc',
    }
    cookies = {
        'seach-tab': '1',
        'cookietest': '1',
        'cookie_consent': '20240312232057',
        '__Host-session': 'SASCRSTT/BDHMFBFDKNJDKCKCOOFNKIAE',
        '__Host-cookie_consent': '20240312232057',
        '__Host-read-disclaimer': '20240323161835',
        '__Host-page': '/Main_Search/RecordDetail.html',
        '__Host-data-token': 'b4537e27806dd374367e9f6cc9ba57421c4b9b3ce3ddd88b4bbf68e5812a1b9c',
        '__Host-ts': '20240323173611',
        '__Host-csrf-token': '94d79c871b5ee9a58923faed23302b23df9ee1d9236df51437da990cc9a01557',
    }
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': 'seach-tab=1; cookietest=1; cookie_consent=20240312232057; __Host-session=QCSDRSSS/EJIJICKCIDBGKJKHDBHAIBBH; __Host-cookie_consent=20240312232057; cookietest=1; __Host-read-disclaimer=20240322172252; __Host-page=/Main_Search/Parcels.html; __Host-data-token=834e4913bb6ac8e6feb8b84c767d817db882e1ff3d790c1713d2914861fb789d; __Host-ts=20240322172319; __Host-csrf-token=14ff39f2ed9212e743156c060010bcda0e341854368975c3471b34f22d6d13bb; ; __Host-cookie_consent=20240312232057; __Host-csrf-token=151f7f65c128ed6647906776fc1a02627e5527cb50ce3d80ff06e4e1b6f134d8; __Host-data-token=19f60e355a348f65bf7b3c0331103d516efe3341f987cdef7701f0ea777e4ef6; __Host-page=/disclaimer.html; __Host-read-disclaimer=; __Host-session=QCSDRSSS/DDKJICKCNGJCGGJDINFINEKK; __Host-ts=20240322172835',
        'Origin': 'https://www.collierappraiser.com',
        'Referer': 'https://www.collierappraiser.com/Main_Search/Parcels.html?FullAddress=hamilton%20ln&tab=1',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'X-CSRF-TOKEN': '14ff39f2ed9212e743156c060010bcda0e341854368975c3471b34f22d6d13bb',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    custom_settings = {
        'FEEDS': {
            'Output/collier_parcel_data.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        }  # Zyte API
        ,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000
        },
        "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "ZYTE_API_KEY": "a8b9defdf86b466f9f60716a85f460be",  # Please enter your API Key here
        "ZYTE_API_TRANSPARENT_MODE": True,
        ## "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
    }

    address_abbreviations = {
        "AVENUE": "AVE", "BLUFF": "BLF", "BOULEVARD": "BLVD", "BEND": "BND", "CIRCLE": "CIR", "CRESCENT": "CRES",
        "COURT": "CT", "COVE": "CV", "DRIVE": "DR", "HOLLOW": "HOLW", "HIGHWAY": "HWY", "JUNCTION": "JCT",
        "LANE": "LN", "LOOP": "LOOP", "MOUNTAIN": "MTN", "PARK": "PARK", "PASS": "PASS", "PATH": "PATH",
        "PARKWAY": "PKWY", "PLACE": "PL", "POINT": "PT", "ROAD": "RD", "RUN": "RUN", "STREET": "ST",
        "TERRACE": "TER", "TRACK": "TRAK", "TRACE": "TRCE", "TRAIL": "TRL", "WAY": "WAY", "CROSSING": "XING"        }

    count = 1
    def start_requests(self):
        file_path = "input/collier_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        count = 1
        for address in addresses:
            parts = address.split(maxsplit=1)
            address1 = (parts[0]).lower()
            address2 = parts[1].split() if len(parts) > 1 else []
            for i in range(len(address2)):
                if address2[i].upper() in self.address_abbreviations:
                    address2[i] = self.address_abbreviations[address2[i].upper()]
            address2_abb = ' '.join(address2)
            # search_address = address1 +'%20'+ address2_abb
            search_address = address1 +'%20'+ address2[0]
            search_url = self.url.format(search_address)
            print(count, address1, address2_abb)
            count += 1

            payload = deepcopy(self.data)
            payload['page'] = '1'
            yield scrapy.FormRequest(url=search_url, method='POST', formdata=payload, headers=self.headers,
                meta={'search_address': address, 'search_url':search_url})

    def parse(self, response):
        data = json.loads(response.text)

        if data['total'] == 0:
            search_address = (response.meta.get('search_address')).replace('%20',' ')
            print('No results match for ', search_address)
            with open('Output/collier_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            for property in data['rows']:
                parcel_id = property.get('cell')[1]
                print(self.count, parcel_id)
                self.count += 1
                search_address = response.meta.get('search_address').replace('%20',' ')
                yield scrapy.Request(url=self.detail_url.format(parcel_id), callback=self.parse_detail, headers=self.headers,
                                 meta={'search_address': search_address})

            '''     PAGINATION    '''
            current_page = data['page']
            total_pages = data['total']
            if current_page < total_pages:
                search_url = response.meta.get('search_url')
                payload = deepcopy(self.data)
                payload['page'] = str(current_page + 1)
                yield scrapy.FormRequest(url=search_url, method='POST', formdata=payload, headers=self.headers,
                    meta={'search_address': response.meta.get('search_address'), 'search_url':search_url})


    def parse_detail(self, response):
        data = json.loads(response.text)

        item = dict()
        '''     1 - main_info      '''
        listing = response.meta.get('listing')
        property_info = dict()

        property_info['parcel_id'] = data[0][0]['ParcelID']  if data[0][0]['ParcelID'] else ''

        property_info['location_address'] = data[0][0]['FullAddress'] if data[0][0]['FullAddress'] else ''
        property_info['subdivision'] = ''
        property_info['sec_twp_rng'] = ''
        property_info['census'] = ''
        property_info['property_use_code'] = data[0][0]['UseCode']   if data[0][0]['UseCode'] else ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
        legal_description = ''
        for ld in data[1]:
            legal_description = legal_description + ld.get('Legal')
        property_info['legal_desc'] = legal_description

        property_info['neighborhood'] = ''
        property_info['property_id'] = ''
        property_info['millage_group'] = ''
        property_info['property_class'] = ''
        property_info['affordable_housing'] = ''

        names = (data[0][0]['Name1']).replace('=', '').split('&')

        # Assigning names to variables, with empty strings if not present
        property_info['owner1']  = names[0].strip() if len(names) > 0 else ''
        property_info['owner2']  = names[1].strip() if len(names) > 1 else ''
        property_info['owner3']  = names[2].strip() if len(names) > 2 else ''

        property_info['mailing_address_1'] = data[0][0]['Name2']  if data[0][0]['Name2'] else ''
        property_info['mailing_address_2'] = data[0][0]['Name3']  if data[0][0]['Name3'] else ''
        property_info['mailing_city'] = data[0][0]['City']  if data[0][0]['City'] else ''
        property_info['mailing_state'] = data[0][0]['State']  if data[0][0]['State'] else ''
        property_info['mailing_zipcode'] = data[0][0]['USZip']  if data[0][0]['USZip'] else ''
        property_info['property_address'] = data[0][0]['FullAddress']  if data[0][0]['FullAddress'] else ''
        property_info['property_zipcode'] = data[0][0]['StreetZone']  if data[0][0]['StreetZone'] else ''
        property_info['search_address'] = response.meta.get('search_address').replace('%20',' ')
        property_info['neighborhood_code'] = ''
        property_info['subdivision_code'] = ''
        property_info['taxing_district'] = ''
        property_info['acreage'] = data[0][0]['TotalAcres']  if data[0][0]['TotalAcres'] else ''
        property_info['mileage'] = data[0][0]['MillageArea']  if data[0][0]['MillageArea'] else ''
        property_info['homestead_exemption'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info

        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = data[0][0]['ParcelID']
        listing['location_address'] = data[0][0]['FullAddress']
        listing['owner1'] = data[0][0]['Name1']
        self.save_to_csv(listing)


        '''     2 - land      '''
        item['land'] = []
        if data[4]:
            LAND_INFO = []
            for lands in data[4]:
                land_info = dict()
                land_info['land_use'] = ''
                land_info['num_of_units'] = lands.get('TOTALUNITS') if lands.get('TOTALUNITS') else ''
                land_info['unit_type'] = lands.get('CALCCODE') if lands.get('CALCCODE') else ''
                land_info['frontage'] = ''
                land_info['depth'] = ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []

        '''     3 - buildings      -      No data available on site          '''
        item['buildings'] = []


        '''     4 - valuations      '''
        if data[5]:
            VALUATION = []
            valuation_count = 1
            for values in data[5]:
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''
                certified_values['year'] = values.get('ROLLTYPE') if values.get('ROLLTYPE') else ''
                certified_values['land'] = data[0][0]['LandAssessedAmount']  if data[0][0]['LandAssessedAmount'] else ''
                certified_values['building'] = ''
                certified_values['extra_feature'] = ''
                certified_values['just'] =  data[0][0]['TotalJustAmount']  if data[0][0]['TotalJustAmount'] else ''
                certified_values['assessed'] =  data[0][0]['CurrYearAssessedAmount']  if data[0][0]['CurrYearAssessedAmount'] else ''
                certified_values['exemptions'] =  data[0][0]['FULLHMSTDExemptAmount']  if data[0][0]['FULLHMSTDExemptAmount'] else ''
                certified_values['taxable'] =  data[0][0]['GrossTaxableAmount']  if data[0][0]['GrossTaxableAmount'] else ''
                certified_values['cap'] = ''
                certified_values['market_sqft'] = ''
                certified_values['assessed_sqft'] = ''
                certified_values['taxable_sqft'] = ''
                certified_values['land_change'] = ''
                certified_values['building_change'] = ''
                certified_values['extra_feature_change'] = ''
                certified_values['just_change'] = ''
                certified_values['assessed_change'] = ''
                certified_values['taxable_change'] = ''
                valuation_count += 1
                VALUATION.append(certified_values)
            item['valuations'] = VALUATION
        else:
            item['valuations'] = []


        '''     5 - extra_features      '''
        if data[3]:
            EXTRA_FEATURES_list = []
            x_features_count = 1
            for extra_features in data[3]:
                extra_f = dict()
                extra_f['id'] = x_features_count
                x_features_count += 1
                extra_f['real_estate_id'] = ''
                extra_f['building_number'] = extra_features.get('SEQNO') if extra_features.get('SEQNO') else ''
                extra_f['desc'] = extra_features.get('BLDGCLASS') if extra_features.get('BLDGCLASS') else ''
                extra_f['units'] = extra_features.get('BASEAREA') if extra_features.get('BASEAREA') else ''
                extra_f['unit_type'] = ''
                extra_f['year'] = extra_features.get('YRBUILT') if extra_features.get('YRBUILT') else ''
                EXTRA_FEATURES_list.append(extra_f)
            item['extra_features'] = EXTRA_FEATURES_list
        else:
            item['extra_features'] = []

        '''     6 - transactions      '''
        if data[2]:
            SALES_LIST = []
            sales_count = 1
            for sales in data[2]:
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.get('SaleDate') if sales.get('SaleDate') else ''
                sales_info['document_number'] = sales.get('SaleBookPage') if sales.get('SaleBookPage') else ''
                sales_info['qualification_code'] = ''
                sales_info['grantor'] = ''
                sales_info['grantee'] = ''
                sales_info['document_type'] = ''
                sales_info['price'] = sales.get('SaleAmount') if sales.get('SaleAmount') else ''
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []

        '''     7 - permits         -      No data available on Website             '''
        if data[6]:
            PERMITS_LIST = []
            for permit in data[6]:
                permit_info = dict()
                permit_info['application'] = permit.get('permitno') if permit.get('permitno') else ''
                permit_info['property_type'] = ''
                permit_info['property_owner'] = ''
                permit_info['application_date'] = ''
                permit_info['valuation'] = ''
                permit_info['parcel_id'] = ''
                permit_info['subcontractor'] = ''
                permit_info['contractor'] = ''
                permit_info['permit_type'] = permit.get('permittype') if permit.get('permittype') else ''
                permit_info['issue_date'] = permit.get('codate') if permit.get('codate') else ''
                PERMITS_LIST.append(permit_info)
            item['permits'] = PERMITS_LIST
        else:
            item['permits'] = []

        '''     8 - flood_zones     -      No data available on Website             '''
        item['flood_zones'] = []

        yield item


    def save_to_csv(self, data):
        with open('Output/collier_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass

