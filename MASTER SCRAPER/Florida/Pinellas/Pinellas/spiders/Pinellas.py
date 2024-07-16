###############    Pinellas
import csv
import json
import scrapy
from copy import deepcopy
from scrapy.selector import Selector

class Pinellas(scrapy.Spider):
    name = 'Pinellas'
    prefix = 'https://www.pcpao.gov'

    url = 'https://www.pcpao.gov/dal/quicksearch/searchProperty'
    cookies = {
        'nocache': '1',
        'SSESS264c7a5e6fca796a222dd358f047e63f': 'X3Qq4YOZ05hQJoJIGOf5qHgyovD7GYgk4UOnyyG0_jU',
        '_gid': 'GA1.2.1935708743.1709742605',
        'has_js': '1',
        'font-default': 'font-default-4',
        '__session:0.2201388956677348:': 'https:',
        '_ga': 'GA1.1.200886660.1709742603',
        '_ga_NCMFJYCN0B': 'GS1.1.1709811229.4.1.1709811473.0.0.0',
    }
    headers = {
        'authority': 'www.pcpao.gov',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': 'nocache=1; SSESS264c7a5e6fca796a222dd358f047e63f=X3Qq4YOZ05hQJoJIGOf5qHgyovD7GYgk4UOnyyG0_jU; _gid=GA1.2.1935708743.1709742605; has_js=1; font-default=font-default-4; __session:0.2201388956677348:=https:; _ga=GA1.1.200886660.1709742603; _ga_NCMFJYCN0B=GS1.1.1709811229.4.1.1709811473.0.0.0',
        'origin': 'https://www.pcpao.gov',
        'referer': 'https://www.pcpao.gov/quick-search?qu=1',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    data = {
        # # 'draw': '5',
        # 'columns[0][data]': '0',        'columns[0][name]': '',        'columns[0][searchable]': 'true',        'columns[0][orderable]': 'false',         'columns[0][search][value]': '',         'columns[0][search][regex]': 'false',
        # 'columns[1][data]': '1',        'columns[1][name]': '',        'columns[1][searchable]': 'true',        'columns[1][orderable]': 'true',        'columns[1][search][value]': '',         'columns[1][search][regex]': 'false',
        # 'columns[2][data]': '2',        'columns[2][name]': '',        'columns[2][searchable]': 'true',        'columns[2][orderable]': 'true',        'columns[2][search][value]': '',        'columns[2][search][regex]': 'false',
        # 'columns[3][data]': '3',        'columns[3][name]': '',        'columns[3][searchable]': 'true',        'columns[3][orderable]': 'true',         'columns[3][search][value]': '',        'columns[3][search][regex]': 'false',
        # 'columns[4][data]': '4',        'columns[4][name]': '',        'columns[4][searchable]': 'true',        'columns[4][orderable]': 'true',        'columns[4][search][value]': '',        'columns[4][search][regex]': 'false',
        # 'columns[5][data]': '5',        'columns[5][name]': '',        'columns[5][searchable]': 'true',        'columns[5][orderable]': 'true',        'columns[5][search][value]': '',        'columns[5][search][regex]': 'false',
        # 'columns[6][data]': '6',        'columns[6][name]': '',        'columns[6][searchable]': 'true',        'columns[6][orderable]': 'true',        'columns[6][search][value]': '',        'columns[6][search][regex]': 'false',
        # 'columns[7][data]': '7',        'columns[7][name]': '',        'columns[7][searchable]': 'true',        'columns[7][orderable]': 'true',        'columns[7][search][value]': '',        'columns[7][search][regex]': 'false',
        # 'columns[8][data]': '8',        'columns[8][name]': '',        'columns[8][searchable]': 'true',        'columns[8][orderable]': 'true',        'columns[8][search][value]': '',        'columns[8][search][regex]': 'false',
        # 'columns[9][data]': '9',        'columns[9][name]': '',        'columns[9][searchable]': 'true',        'columns[9][orderable]': 'false',        'columns[9][search][value]': '',        'columns[9][search][regex]': 'false',
        # 'columns[10][data]': '10',        'columns[10][name]': '',        'columns[10][searchable]': 'true',        'columns[10][orderable]': 'true',        'columns[10][search][value]': '',        'columns[10][search][regex]': 'false',
        'search[value]': '',        'search[regex]': 'false',        'url': 'https://www.pcpao.gov',
        'input': 'Bay Pines Boulevard',        'searchsort': 'address',
        'start': '0',
        'length': '100',
    }
    custom_settings = {
        'FEEDS': {
            'Output/pinellas_parcel_data.json': {
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
        file_path = "input/pinellas_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)
        for address in addresses:
            payload = deepcopy(self.data)
            payload['input'] = address
            payload['start'] = str(0)
            record_count = 0
            yield scrapy.FormRequest(url=self.url, formdata=payload, headers=self.headers, method='POST', callback=self.parse,
                                     meta={'record_count':record_count, 'payload':payload,'search_address':address})

    def parse(self, response):
        data = json.loads(response.body)
        total_count = data['recordsTotal']
        record_count = response.meta.get('record_count')
        
        if data['data']:
            for Row in data['data']:
                item = dict()
                record_count +=  1
                parcel_url = ''
                name = (Selector(text=Row[1].strip())).css('a')
                if name:
                    item['owner1'] = name.css('::text').get()
                    parcel_url = name.css('::attr(href)').get()
                parcel_id = (Selector(text=Row[3].strip())).css('a')
                if parcel_id:
                    item['parcel_id'] = parcel_id.css('::text').get()
                item['location_address'] = Row[4].strip()
                item['property_use_code'] = Row[6].strip()
                tax_dist = (Selector(text=Row[5].strip())).css('span')
                if tax_dist:
                    item['taxing_district'] = tax_dist.css('::text').get()
                short_legal = (Selector(text=Row[7].strip())).css('span')
                if short_legal:
                    item['legal_description'] = short_legal.css('::text').get()
                item['search_address'] = response.meta.get('search_address')

                self.save_to_csv(item)
                yield scrapy.Request(url=parcel_url, callback=self.parse_detail, headers=self.headers, meta={'property_info':item})

            '''         PAGINATION              '''
            if record_count < total_count:
                payload = response.meta.get('payload')
                payload['start'] = str(record_count)
                yield scrapy.FormRequest(url=self.url, formdata=payload, headers=self.headers, method='POST', callback=self.parse,
                      meta={'record_count':record_count, 'payload':payload,'search_address':response.meta.get('address')})

        else:
            search_address = response.meta.get('search_address')
            with open('Output/pinellas_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = response.meta.get('property_info')

        owners = response.xpath('//*[contains(text()," Owner Name")]/following-sibling::div[1]/label/span/text()').getall()
        property_info['owner1'] = owners[0]
        property_info['owner2'] = ''
        property_info['owner3'] = ''
        if len(owners) == 2:
            property_info['owner2'] = owners[1]
        if len(owners) == 3:
            property_info['owner3'] = owners[2]

        mailing_add = response.xpath('//*[contains(text()," Mailing Address")]/following-sibling::label[1]/div/text()').getall()
        property_info['mailing_address_1'] = ', '.join(mailing_add)
        property_info['mailing_address_2'] = ''
        property_info['mailing_city'], rest = mailing_add[-1].split(', ', 1)  # Split at the first ', ' occurrence
        property_info['mailing_state'], property_info['mailing_zipcode'] = rest.split(' ', 1)
        loc_add = response.xpath('//*[contains(text()," Site Address")]/following-sibling::label[1]/text()').getall()
        property_info['location_address'] = ', '.join(loc_add)
        property_info['legal_description'] = response.xpath('//*[contains(text()," Legal Description")]/following-sibling::div[1]/input[contains(@id,"legal_full_desc")]/@value').get('').strip()
        property_info['census'] = response.css('#tblParcelInformation tbody tr td:nth-child(3) span ::text').get('').strip()

        no_prop_class = response.xpath('//*[contains(text(),"No Property Exemptions or Classifications found")]')
        if no_prop_class:
            property_info['property_class'] = ''
        else:
            property_info['property_class'] = response.css('#tblParcelUse tbody tr td ::text').get('').strip()
        property_info['subdivision'] = ''
        property_info['sec_twp_rng'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
        property_info['property_id'] = ''
        property_info['millage_group'] = ''
        property_info['neighborhood'] = ''
        property_info['affordable_housing'] = ''
        property_info['property_address'] = ''
        property_info['property_zipcode'] = ''
        property_info['neighborhood_code'] = ''
        property_info['subdivision_code'] = ''
        property_info['acreage'] = ''
        property_info['mileage'] = ''
        property_info['homestead_exemption'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info

        '''     2 - land      '''
        no_land_check = response.xpath("//*[contains(text(),'No Lands on Record.')]/text()").get('').strip()
        if no_land_check:
            item['land'] = []
        else:
            LAND_INFO = []
            Land = response.xpath("//*[contains(@id,'tblLandInformation')]/tbody")
            for values in Land.css('tr'):
                Land_values = dict()
                Land_values['land_use'] = values.css('td:nth-child(1) a ::text').get('').strip()
                Land_values['num_of_units'] = values.css('td:nth-child(4) ::text').get('').strip()
                Land_values['unit_type'] = values.css('td:nth-child(3) ::text').get('').strip()
                Land_values['frontage'] = ''
                Land_values['depth'] = ''
                LAND_INFO.append(Land_values)
            item['land'] = LAND_INFO


        '''     3 - Buildings         '''
        no_buildings_check = response.xpath("//*[contains(text(),' Buildings')]/following-sibling::h2/text()").get('').strip()
        if int(no_buildings_check) == 0:
            item['buildings'] = []
        elif int(no_buildings_check) > 0:
            BUILDING_INFO = []
            buildings_count = 1
            buildings_check = response.css('#structural_1')
            for values in buildings_check:
                buildings_values = dict()
                buildings_values['id'] = buildings_count
                buildings_count += 1
                buildings_values['real_estate_id'] = ''
                buildings_values['building_no'] = ''
                buildings_values['beds'] = ''
                buildings_values['stories'] = values.xpath(".//td[contains(text(),'Unit Stories:')]/following-sibling::td/text()").get('').strip()
                buildings_values['baths'] = ''
                buildings_values['half_baths'] = ''
                buildings_values['built_year'] = values.xpath(".//td[contains(text(),'Year Built:')]/following-sibling::td/text()").get('').strip()
                buildings_values['ac'] = values.xpath(".//td[contains(text(),'Cooling')]/following-sibling::td/text()").get('').strip()
                buildings_values['heat'] = values.xpath(".//td[contains(text(),'Heating')]/following-sibling::td/text()").get('').strip()
                buildings_values['floor_cover'] = values.xpath(".//td[contains(text(),'Floor Finish')]/following-sibling::td/text()").get('').strip()
                buildings_values['frame_type'] = values.xpath(".//td[contains(text(),'Frame')]/following-sibling::td/text()").get('').strip()
                buildings_values['interior_walls'] = values.xpath(".//td[contains(text(),'Interior Finish')]/following-sibling::td/text()").get('').strip()
                buildings_values['roof_cover'] = values.xpath(".//td[contains(text(),'Roof Cover')]/following-sibling::td/text()").get('').strip()
                buildings_values['exterior_walls'] = values.xpath(".//td[contains(text(),'Exterior Walls:')]/following-sibling::td/text()").get('').strip()
                buildings_values['gross_building_area'] = values.xpath(".//*[contains(text(),'Total Area SF')]/parent::td/following-sibling::td[2]/b/text()").get('').strip()
                buildings_values['living_area'] = values.xpath(".//*[contains(text(),'Total Area SF')]/parent::td/following-sibling::td[1]/b/text()").get('').strip()
                BUILDING_INFO.append(buildings_values)
            item['buildings'] = BUILDING_INFO


        '''     4 - valuations      '''
        values_check = response.css('#tblValueHistory')
        if values_check:
            VALUES_INFO = []
            count = 1
            Values = response.css('#tblValueHistory tbody')
            for values in Values.css('tr'):
                values_value = dict()
                values_value['id'] = count
                count += 1
                values_value['real_estate_id'] = ''
                values_value['year'] = values.css('td:nth-child(1) ::text').get('').strip()
                values_value['land'] = ''
                values_value['building'] = ''
                values_value['extra_feature'] = ''
                values_value['just'] = values.css('td:nth-child(3) ::text').get('').strip()
                values_value['assessed'] = values.css('td:nth-child(4) ::text').get('').strip()
                values_value['exemptions'] = values.css('td:nth-child(2) ::text').get('').strip()
                values_value['taxable'] = values.css('td:nth-child(5) ::text').get('').strip()
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


        '''     5 - transactions      '''
        sales_check = response.css('#tblSalesHistory')
        if sales_check:
            SALES_TRANSFERS_INFO = []
            sales_count = 1
            Sales_Transfers = response.css('#tblSalesHistory tbody')
            for values in Sales_Transfers.css('tr'):
                Sales_Transfers_values = dict()
                Sales_Transfers_values['id'] = sales_count
                sales_count += 1
                Sales_Transfers_values['real_estate_id'] = ''
                Sales_Transfers_values['transfer_date'] = values.css('td:nth-child(1) ::text').get('').strip()
                Sales_Transfers_values['document_number'] = values.css('td:nth-child(7) ::text').get('').strip()
                Sales_Transfers_values['qualification_code'] = values.css('td:nth-child(3) ::text').get('').strip()
                Sales_Transfers_values['grantor'] = values.css('td:nth-child(5) ::text').get('').strip()
                Sales_Transfers_values['grantee'] = values.css('td:nth-child(6) ::text').get('').strip()
                Sales_Transfers_values['document_type'] = ''
                Sales_Transfers_values['price'] = values.css('td:nth-child(2) ::text').get('').strip()
                SALES_TRANSFERS_INFO.append(Sales_Transfers_values)
            item['transactions'] = SALES_TRANSFERS_INFO
        else:
            item['transactions'] = []


        '''     6 - extra_features    '''
        no_extra_features_check = response.xpath("//*[contains(text(),'No Extra Features on Record.')]/text()").get('').strip()
        if no_extra_features_check:
            item['extra_features'] = []
        else:
            X_FEATURES_INFO = []
            x_count = 1
            extra_features = response.xpath("//*[contains(@id,'tblExtraFeatures')]/tbody")
            for values in extra_features.css('tr'):
                extra_features_values = dict()
                extra_features_values['id'] = x_count
                x_count += 1
                extra_features_values['real_estate_id'] = ''
                extra_features_values['building_number'] = ''
                extra_features_values['desc'] = values.css('td:nth-child(1) ::text').get('').strip()
                extra_features_values['units'] = values.css('td:nth-child(3) ::text').get('').strip()
                extra_features_values['unit_type'] = values.css('td:nth-child(2) ::text').get('').strip()
                extra_features_values['year'] = values.css('td:nth-child(6) ::text').get('').strip()
                X_FEATURES_INFO.append(extra_features_values)
            item['extra_features'] = X_FEATURES_INFO


        '''     7 - permit          '''
        no_permits_check = response.xpath("//*[contains(text(),'No Permits on Record.')]/text()").get('').strip()
        if no_permits_check:
            item['permits'] = []
        else:
            PERMITS_INFO = []
            Permits = response.xpath("//*[contains(@id,'tblPermit')]/tbody")
            for values in Permits.css('tr'):
                Permits_values = dict()
                Permits_values['application'] = values.css('td:nth-child(1) p a ::text').get('').strip()
                Permits_values['property_type'] = values.css('td:nth-child(2) ::text').get('').strip()
                Permits_values['property_owner'] = ''
                Permits_values['application_date'] = ''
                Permits_values['valuation'] = values.css('td:nth-child(4) ::text').get('').strip()
                Permits_values['parcel_id'] = ''
                Permits_values['subcontractor'] = ''
                Permits_values['contractor'] = ''
                Permits_values['permit_type'] = ''
                Permits_values['issue_date'] = values.css('td:nth-child(3) ::text').get('').strip()
                PERMITS_INFO.append(Permits_values)
            item['permits'] = PERMITS_INFO


        '''     8 - flood_zones      -          Data not available on site                 '''
        item['flood_zones'] = []

        # print(item)
        yield item


    def save_to_csv(self, data):
        with open('Output/pinellas_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id','location_address','owner1','property_use_code','taxing_district','search_address','legal_description']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass
