import csv
import json
import re

import scrapy
from copy import deepcopy
from scrapy.selector import Selector

class Manatee(scrapy.Spider):
    name = 'Manatee'

    url = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-parcel-search-results.php'
    data = {        'SearchQ': '[{"name":"RollType[]","value":"REAL PROPERTY"},{"name":"Address","value":""}]', }
    cookies = {
        '_ga': 'GA1.1.1891973576.1709742319',
        'PHPSESSID': 'p4h5haopbgau1r1ia7mf4roair',
        'TS01a003af': '01769c62af6f5ab72bcaf83739a7dea2d244a57a4ab4c706d31fbc004731b80d3a704cf40c1b1ee7993ffc4caee7a31c7bfeb47f94',
        '_ga_DSQ4WLY7ZG': 'GS1.1.1709901661.2.1.1709901740.0.0.0',
    }
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': '_ga=GA1.1.1891973576.1709742319; PHPSESSID=p4h5haopbgau1r1ia7mf4roair; TS01a003af=01769c62af6f5ab72bcaf83739a7dea2d244a57a4ab4c706d31fbc004731b80d3a704cf40c1b1ee7993ffc4caee7a31c7bfeb47f94; _ga_DSQ4WLY7ZG=GS1.1.1709901661.2.1.1709901740.0.0.0',
        'Origin': 'https://www.manateepao.gov',
        'Referer': 'https://www.manateepao.gov/search/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    main_info = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-owner.php'
    main_info_data = {        'data': '{"parid":"1903400602","ownerType":"","parcel_type":"real_property"}',      }

    sales_info = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-sales.php?parid={}'
    land_info = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-land.php?parid={}'
    value_info = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-value-history.php?parid={}'
    building_info = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-buildings.php?parid={}'
    x_features_info = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-features.php?parid={}'
    permits_info = 'https://www.manateepao.gov/wp-content/themes/frontier-child/models/pao-model-permits.php?parid={}'

    custom_settings = {
        'FEEDS': {
            'Output/manatee_parcel_data.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        }  # Zyte API
        # ,
        # "DOWNLOAD_HANDLERS": {
        #     "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        #     "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        # },
        # "DOWNLOADER_MIDDLEWARES": {
        #     "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000
        # },
        # "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        # "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        # "ZYTE_API_KEY": "a8b9defdf86b466f9f60716a85f460be",  # Please enter your API Key here
        # "ZYTE_API_TRANSPARENT_MODE": True,
        # "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
    }

    def start_requests(self):
        file_path = "input/manatee_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        for address in addresses:
            payload = deepcopy(self.data)
            searchq_list = json.loads(payload['SearchQ'])
            for item in searchq_list:
                if item['name'] == 'Address':
                    item['value'] = address
            payload['SearchQ'] = json.dumps(searchq_list)

            yield scrapy.FormRequest(url=self.url, formdata=payload, headers=self.headers, method='POST', callback=self.parse,
                                     meta={'search_address':address})

    def parse(self, response):
        data = json.loads(response.body)
        if data['rows']:
            for Row in data['rows']:
                payload = deepcopy(self.main_info_data)
                data_list = json.loads(payload['data'])
                data_list['parid'] = str(Row[0])
                payload['data'] = json.dumps(data_list)
                yield scrapy.FormRequest(url=self.main_info, callback=self.main_info_detail, method="POST", headers=self.headers,
                                     formdata=payload, meta={'search_address':response.meta.get('search_address')})
        else:
            search_address = response.meta.get('search_address')
            with open('Output/manatee_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

    def main_info_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        parid = response.xpath("//*[contains(text(),'Parcel ID')]/parent::div/following-sibling::div[1]/b/text()").get('').strip()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/parent::div/following-sibling::div[1]/b/text()").get('').strip()

        if response.xpath("//*[contains(text(),'Ownership')]/following-sibling::div[1]/a/text()").get('').strip():
            ownership = response.xpath("//*[contains(text(),'Ownership')]/following-sibling::div[1]/a/text()").get('').strip()
        else:
            ownership = response.xpath("//*[contains(text(),'Ownership')]/following-sibling::div[1]/text()").get('').strip()

        property_info['owner1'] = ownership
        owners = ownership.split(';')
        property_info['owner1'] = owners[0].strip()  # Remove leading and trailing whitespaces
        property_info['owner2'] = owners[1].strip() if len(owners) > 1 else ''  # Assign None if there is no second owner
        property_info['owner3'] = owners[2].strip() if len(owners) > 2 else ''  # Assign None if there is no third owner

        mailing_add = response.xpath("//*[contains(text(),'Mailing Address')]/following-sibling::div[1]/text()").get('').strip()
        property_info['mailing_address1'] = mailing_add
        property_info['mailing_address2'] = ''

        address_part = mailing_add.split(', ')[-1]
        city_state_zip = address_part.split(' ')
        property_info['mailing_city'] = ' '.join(city_state_zip[:-2])
        if len(city_state_zip) >= 2:
            property_info['mailing_state'] = city_state_zip[-2]
        else:
            property_info['mailing_state'] = ''
        property_info['mailing_zipcode'] = city_state_zip[-1]

        property_add = response.xpath("//*[contains(text(),'Situs Address')]/following-sibling::div[1]/a/text()").get('').strip()
        property_info['location_address'] = property_add
        property_info['property_address'] = property_add
        address_part = property_add.split(', ')[-1]
        zip = address_part.split(' ')
        property_info['property_zipcode'] = zip[-1]

        property_info['search_address'] = response.meta.get('search_address')
        property_info['parcel_desc'] = response.xpath("//*[contains(text(),'Short Description')]/following-sibling::div[1]/text()").get('').strip()
        property_info['taxing_district'] = response.xpath("//*[contains(text(),'Tax District')]/following-sibling::div[1]/a/text()").get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Sec/Twp/Rge')]/following-sibling::div[1]/text()").get('').strip()
        property_info['neighborhood'] = response.xpath("//*[contains(text(),'Neighborhood')]/following-sibling::div[1]/a/text()").get('').strip()
        property_info['subdivision'] = response.xpath("//*[contains(text(),'Subdivision:')]/following-sibling::div[1]/a/text()").get('').strip()
        property_info['legal_description'] = response.xpath("//*[contains(text(),'Short Description')]/following-sibling::div[1]/text()").get('').strip()

        property_info['census'] = ''
        property_info['property_use_code'] = ''
        property_info['property_class'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['property_id'] = ''
        property_info['millage_group'] = ''
        property_info['affordable_housing'] = ''
        property_info['neighborhood_code'] = ''
        property_info['subdivision_code'] = ''
        property_info['acreage'] = ''
        property_info['mileage'] = ''
        property_info['homestead_exemption'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info

        listing = dict()
        listing['parcel_id'] = parid
        listing['location_address'] = property_add
        listing['owner'] = ownership
        self.save_to_csv(listing)

        yield scrapy.Request(url=self.sales_info.format(parid), callback=self.sales_detail, headers=self.headers,
                             meta={'item': item, 'parid':parid})

    def sales_detail(self, response):
        item = response.meta.get('item')
        data = json.loads(response.body)
        '''     2 - transactions      '''
        if data['rows']:
            SALES_TRANSFERS_INFO = []
            sales_count = 1
            for values in data['rows']:
                Sales_Transfers_values = dict()
                Sales_Transfers_values['id'] = sales_count
                sales_count += 1
                Sales_Transfers_values['real_estate_id'] = ''
                Sales_Transfers_values['transfer_date'] = values[0] if values[0] else ''
                Sales_Transfers_values['document_number'] = (values[1] if values[1] else '')+'-'+(values[2] if values[2] else '')
                Sales_Transfers_values['qualification_code'] = values[5] if values[5] else ''
                Sales_Transfers_values['grantor'] = ''
                Sales_Transfers_values['grantee'] = values[7] if values[7] else ''
                Sales_Transfers_values['document_type'] = ''
                Sales_Transfers_values['price'] = values[6] if values[6] else ''

                SALES_TRANSFERS_INFO.append(Sales_Transfers_values)
            item['transactions'] = SALES_TRANSFERS_INFO
        else:
            item['transactions'] = []

        parid = response.meta.get('parid')
        yield scrapy.Request(url=self.land_info.format(parid), callback=self.land_detail, headers=self.headers,
                             meta={'item': item, 'parid':response.meta.get('parid')})

    def land_detail(self, response):
        item = response.meta.get('item')
        data = json.loads(response.body)
        '''     3 - land      '''
        if data['rows']:
            item['land'] = []
            LAND_INFO = []
            for values in data['rows']:
                Land_values = dict()
                Land_values['land_use'] = ''
                Land_values['num_of_units'] = values[7] if values[7] else ''
                Land_values['unit_type'] = values[1] if values[1] else ''
                Land_values['frontage'] = values[2] if values[2] else ''
                Land_values['depth'] = values[4] if values[4] else ''
                LAND_INFO.append(Land_values)
            item['land'] = LAND_INFO

        parid = response.meta.get('parid')
        yield scrapy.Request(url=self.value_info.format(parid), callback=self.value_detail, headers=self.headers,
                             meta={'item': item, 'parid': response.meta.get('parid')})

    def value_detail(self, response):
        item = response.meta.get('item')
        data = json.loads(response.body)
        '''     4 - valuations      '''
        if data['rows']:
            VALUES_INFO = []
            count = 1
            for values in data['rows']:
                values_value = dict()
                values_value['id'] = count
                count += 1
                values_value['real_estate_id'] = ''
                values_value['year'] = values[0] if values[0] else ''
                values_value['land'] = values[2] if values[2] else ''
                values_value['building'] = ''
                values_value['extra_feature'] = ''
                values_value['just'] = values[4] if values[4] else ''
                values_value['assessed'] = values[5] if values[5] else ''
                values_value['exemptions'] = values[1] if values[1] else ''
                values_value['taxable'] = values[7] if values[7] else ''
                values_value['cap'] = ''
                values_value['market_sqft'] = ''
                values_value['assessed_sqft'] = ''
                values_value['taxable_sqft'] = ''
                values_value['land_change'] = ''
                values_value['building_change'] = ''
                values_value['extra_feature_change'] = ''
                values_value['just_change'] = ''
                values_value['assessed_change'] = ''
                values_value['taxable_change'] = ''

                VALUES_INFO.append(values_value)
            item['valuations'] = VALUES_INFO
        else:
            item['valuations'] = []

        parid = response.meta.get('parid')
        yield scrapy.Request(url=self.building_info.format(parid), callback=self.building_detail, headers=self.headers,
                             meta={'item': item, 'parid': response.meta.get('parid')})

    def building_detail(self, response):
        item = response.meta.get('item')
        data = json.loads(response.body)
        '''     5 - buildings      '''
        if data['rows']:
            BUILDING_INFO = []
            buildings_count = 1
            for values in data['rows']:
                buildings_values = dict()
                buildings_values['id'] = buildings_count
                buildings_count += 1
                buildings_values['real_estate_id'] = ''
                buildings_values['building_no'] = values[1] if values[1] else ''
                buildings_values['beds'] = ''
                buildings_values['baths'] = ''
                buildings_values['stories'] = values[5] if values[5] else ''
                buildings_values['half_baths'] = ''
                buildings_values['built_year'] = values[3] if values[3] else ''
                buildings_values['ac'] = ''
                buildings_values['heat'] = ''
                buildings_values['floor_cover'] = ''
                buildings_values['frame_type'] = values[11] if values[11] else ''
                buildings_values['interior_walls'] = ''
                buildings_values['roof_cover'] = values[10] if values[10] else ''
                buildings_values['exterior_walls'] = values[9] if values[9] else ''
                buildings_values['gross_building_area'] = ''
                buildings_values['living_area'] = ''
                BUILDING_INFO.append(buildings_values)
            item['buildings'] = BUILDING_INFO
        else:
            item['buildings'] = []

        parid = response.meta.get('parid')
        yield scrapy.Request(url=self.x_features_info.format(parid), callback=self.x_features_detail, headers=self.headers,
                             meta={'item': item, 'parid': response.meta.get('parid')})

    def x_features_detail(self, response):
        item = response.meta.get('item')
        data = json.loads(response.body)
        '''     6 - extra_features      '''
        if data['rows']:
            X_FEATURES_INFO = []
            x_count = 1
            for values in data['rows']:
                extra_features_values = dict()
                extra_features_values['id'] = x_count
                x_count += 1
                extra_features_values['real_estate_id'] = ''
                extra_features_values['building_number'] = values[2] if values[2] else ''
                extra_features_values['desc'] = values[3] if values[3] else ''
                extra_features_values['units'] = values[10] if values[10] else ''
                extra_features_values['unit_type'] = values[0] if values[0] else ''
                extra_features_values['year'] = values[5] if values[5] else ''
                X_FEATURES_INFO.append(extra_features_values)
            item['extra_features'] = X_FEATURES_INFO
        else:
            item['extra_features'] = []

        parid = response.meta.get('parid')
        yield scrapy.Request(url=self.permits_info.format(parid), callback=self.permits_detail, headers=self.headers,
                             meta={'item': item, 'parid': response.meta.get('parid')})

    def permits_detail(self, response):
        item = response.meta.get('item')
        data = json.loads(response.body)
        '''     7 - permits      '''
        if data['rows']:
            PERMITS_INFO = []
            for values in data['rows']:
                Permits_values = dict()
                Permits_values['application'] = values[0] if values[0] else ''
                Permits_values['property_type'] = values[3] if values[3] else ''
                Permits_values['property_owner'] = ''
                Permits_values['application_date'] = values[7] if values[7] else ''
                Permits_values['valuation'] = values[5] if values[5] else ''
                Permits_values['parcel_id'] = ''
                Permits_values['subcontractor'] = ''
                Permits_values['contractor'] = values[4] if values[4] else ''
                Permits_values['permit_type'] = values[2] if values[2] else ''
                Permits_values['issue_date'] = values[1] if values[1] else ''
                PERMITS_INFO.append(Permits_values)
            item['permits'] = PERMITS_INFO
        else:
            item['permits'] = []

        '''     8 - flood_zones      '''
        item['flood_zones'] = []

        yield item

    def save_to_csv(self, data):
        with open('Output/manatee_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id','location_address','owner']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
