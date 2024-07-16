########        Miami_Dade
import json
import csv
import scrapy

class Miami_Dade(scrapy.Spider):
    name = 'Miami_Dade'

    url = ("https://www.miamidade.gov/Apps/PA/PApublicServiceProxy/PaServicesProxy.ashx?Operation=GetAddress&"
           "clientAppName=PropertySearch&myUnit=&from={}&myAddress={}&to={}")

    detail_url = ("https://www.miamidade.gov/Apps/PA/PApublicServiceProxy/PaServicesProxy.ashx?Operation=GetPropertySearchByFolio"
           "&clientAppName=PropertySearch&folioNumber={}")

    headers = {
        'authority': 'www.miamidade.gov',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        # 'cookie': '_ga=GA1.1.1228939002.1710267968; NSC_xxxy.njbnjebef.hpw_TTM_Efgbvmu=ffffffff09303f0345525d5f4f58455e445a4a42378b; arp_scroll_position=300; _ga_S336V3M935=GS1.1.1710964085.8.1.1710964496.0.0.0; NSC_xxxy.njbnjebef.hpw_TTM_Efgbvmu=ffffffff09303f0345525d5f4f58455e445a4a42378b',
        'referer': 'https://www.miamidade.gov/Apps/PA/PropertySearch/',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    custom_settings = {
        'FEEDS': {
            'Output/miami_dade_parcel_data.json': {
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
        "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
    }

    def start_requests(self):
        file_path = "input/miami_dade_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        count = 1
        for address in addresses:
            print(count, address)
            count += 1
            search_address = address.replace(' ','%20')
            From =  1
            To = 200
            yield scrapy.Request(url=self.url.format(From, search_address, To), callback=self.parse,   headers=self.headers,
                             meta={'search_address': search_address,'From': From, 'To': To,'property_count':0})

    def parse(self, response):
        property_count = response.meta.get('property_count')
        data = json.loads(response.body)

        if 'Invalid Address, Could not standardize this address ' in data['Message']:
            search_address = (response.meta.get('search_address')).replace('%20',' ')
            print('No results match for ', search_address)
            with open('Output/miami_dade_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            for property in data['MinimumPropertyInfos']:
                property_count += 1
                listing = dict()
                listing['parcel_id'] = property.get('Strap')
                listing['location_address'] = property.get('SiteAddress')
                listing['owner1'] = property.get('Owner1')
                self.save_to_csv(listing)
                listing['owner2'] = property.get('Owner2')
                listing['owner3'] = property.get('Owner3')

                property_id =  (property.get('Strap')).replace('-','')
                search_address = response.meta.get('search_address').replace('%20',' ')
                yield scrapy.Request(url=self.detail_url.format(property_id), callback=self.parse_detail, headers=self.headers,
                                 meta={'search_address': search_address, 'listing':listing})

            '''     PAGINATION    '''
            total_properties = data['Total']
            if property_count < total_properties:
                search_address = response.meta.get('search_address')
                From =  response.meta.get('From') + 200
                To = response.meta.get('To') + 200
                yield scrapy.Request(url=self.url.format(From, search_address, To), callback=self.parse,   headers=self.headers,
                    meta={'search_address': search_address,'From': From, 'To': To, 'property_count':property_count})


    def parse_detail(self, response):
        data = json.loads(response.body)
        item = dict()

        '''     1 - main_info      '''
        listing = response.meta.get('listing')
        property_info = dict()

        property_info['parcel_id'] = data['PropertyInfo']['FolioNumber']
        property_info['location_address'] = listing['location_address']
        property_info['subdivision'] = data['PropertyInfo']['SubdivisionDescription']
        property_info['sec_twp_rng'] = ''
        property_info['census'] = ''
        property_info['property_use_code'] = data['PropertyInfo']['DORCode']
        property_info['waterfront_code'] = ''
        property_info['municipality'] = data['PropertyInfo']['Municipality']
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
        property_info['legal_desc'] = data['LegalDescription']['Description']
        property_info['neighborhood'] = data['PropertyInfo']['NeighborhoodDescription']
        property_info['property_id'] = ''
        property_info['millage_group'] = ''
        property_info['property_class'] = ''
        property_info['affordable_housing'] = ''

        property_info['owner1'] = listing['owner1']
        property_info['owner2'] = listing['owner2']
        property_info['owner3'] = listing['owner3']
        property_info['mailing_address_1'] = data['MailingAddress']['Address1']
        property_info['mailing_address_2'] = data['MailingAddress']['Address2']
        property_info['mailing_city'] = data['MailingAddress']['City']
        property_info['mailing_state'] = data['MailingAddress']['State']
        property_info['mailing_zipcode'] = data['MailingAddress']['ZipCode']
        property_info['property_address'] = data['SiteAddress'][0]['Address']
        property_info['property_zipcode'] = data['SiteAddress'][0]['Zip']
        property_info['search_address'] = response.meta.get('search_address').replace('%20','  ')
        property_info['neighborhood_code'] = data['PropertyInfo']['Neighborhood']
        property_info['subdivision_code'] = data['PropertyInfo']['Subdivision']
        property_info['taxing_district'] = ''
        property_info['acreage'] = data['PropertyInfo']['BuildingActualArea']
        property_info['mileage'] = ''
        property_info['homestead_exemption'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info


        '''     2 - land      '''
        item['land'] = []
        if data['Land']['Landlines']:
            LAND_INFO = []
            for lands in data['Land']['Landlines']:
                land_info = dict()
                land_info['land_use'] = lands.get('LandUse') if lands.get('LandUse') else ''
                land_info['num_of_units'] = lands.get('Units') if lands.get('Units') else ''
                land_info['unit_type'] = lands.get('UnitType') if lands.get('UnitType') else ''
                land_info['frontage'] = lands.get('FrontFeet') if lands.get('FrontFeet') else ''
                land_info['depth'] = lands.get('Depth') if lands.get('Depth') else ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []

        '''     3 - buildings      '''
        item['buildings'] = []
        if data['Building']['BuildingInfos']:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in data['Building']['BuildingInfos']:
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = buildings.get('BuildingNo') if buildings.get('BuildingNo') else ''
                building_info['beds']  = ""
                building_info['baths']  = ""
                building_info['stories']  = ""
                building_info['half_baths'] = ''
                building_info['built_year']  = buildings.get('Actual') if buildings.get('Actual') else ''
                building_info['ac']  = ""
                building_info['heat'] = ""
                building_info['floor_cover']  = ""
                building_info['frame_type'] = ''
                building_info['interior_walls']  = ""
                building_info['roof_cover']  = ""
                building_info['exterior_walls']  = ""
                building_info['gross_building_area'] = buildings.get('GrossArea') if buildings.get('GrossArea') else ''
                building_info['living_area'] = buildings.get('EffectiveArea') if buildings.get('EffectiveArea') else ''
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []

        '''     4 - valuations      '''
        if data['Assessment']['AssessmentInfos']:
            VALUATION = []
            valuation_count = 1
            for values in data['Assessment']['AssessmentInfos']:
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''
                certified_values['year'] = values.get('Year') if values.get('Year') else ''
                certified_values['land'] = values.get('LandValue') if values.get('LandValue') else ''
                certified_values['building'] = values.get('BuildingOnlyValue') if values.get('BuildingOnlyValue') else ''
                certified_values['extra_feature'] = values.get('ExtraFeatureValue') if values.get('ExtraFeatureValue') else ''
                certified_values['just'] = values.get('TotalValue') if values.get('TotalValue') else ''
                certified_values['assessed'] = values.get('AssessedValue') if values.get('AssessedValue') else ''
                certified_values['exemptions'] = ''
                certified_values['taxable'] = ''
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
        if data['ExtraFeature']['ExtraFeatureInfos']:
            EXTRA_FEATURES_list = []
            x_features_count = 1
            for extra_features in data['ExtraFeature']['ExtraFeatureInfos']:
                extra_f = dict()
                extra_f['id'] = x_features_count
                x_features_count += 1
                extra_f['real_estate_id'] = ''
                extra_f['building_number'] = ''
                extra_f['desc'] = extra_features.get('Description') if extra_features.get('Description') else ''
                extra_f['units'] = extra_features.get('Units') if extra_features.get('Units') else ''
                extra_f['unit_type'] = ''
                extra_f['year'] = extra_features.get('ActualYearBuilt') if extra_features.get('ActualYearBuilt') else ''
                EXTRA_FEATURES_list.append(extra_f)
            item['extra_features'] = EXTRA_FEATURES_list
        else:
            item['extra_features'] = []


        '''     6 - transactions      '''
        if data['SalesInfos']:
            SALES_LIST = []
            sales_count = 1
            for sales in data['SalesInfos']:
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.get('DateOfSale') if sales.get('DateOfSale') else ''
                sales_info['document_number'] = sales.get('DocumentStamps') if sales.get('DocumentStamps') else ''
                sales_info['qualification_code'] = sales.get('QualifiedFlag') if sales.get('QualifiedFlag') else ''
                sales_info['grantor'] = sales.get('GrantorName1') if sales.get('GrantorName1') else ''
                sales_info['grantee'] = sales.get('GranteeName1') if sales.get('GranteeName1') else ''
                sales_info['document_type'] = sales.get('SaleInstrument') if sales.get('SaleInstrument') else ''
                sales_info['price'] = sales.get('SalePrice') if sales.get('SalePrice') else ''
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []

        '''     7 - permits         -      No data available on Website             '''
        item['permits'] = []

        '''     8 - flood_zones     -      No data available on Website             '''
        item['flood_zones'] = []

        yield item

    def save_to_csv(self, data):
        with open('Output/miami_dade_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
