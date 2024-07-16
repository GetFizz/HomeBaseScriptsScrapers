#######         DeSoto
import json
import re
import csv
import scrapy
from copy import deepcopy

class DeSoto(scrapy.Spider):
    name = 'DeSoto'
    prefix = 'https://www.desotopa.com'
    url = "https://www.desotopa.com/gis/recordSearch_2_Results/"
    cookies = {
        'DesotoPA_gisTools_showCustomZoomIn_chk': 'false',
        'ASPSESSIONIDSUCTRSBS': 'GHIDKBKCDKCECKGDOEPADNBP',
    }
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'DesotoPA_gisTools_showCustomZoomIn_chk=false; ASPSESSIONIDSUCTRSBS=GHIDKBKCDKCECKGDOEPADNBP',
        'origin': 'https://www.desotopa.com',
        'referer': 'https://www.desotopa.com/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'iframe',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }

    data = {
        'StreetName': '',
        'button_RecordSearch': '1',        'OwnerName': '',        'StreetNumber': '',        'StreetType': '',
        'HeatedSF_To': '',        'YearBuilt_From': '',        'PIN': '',        'Parcel_Key': '',        'Section': '',
        'Township': '',        'Range': '',        'Acre_GT': '',        'Acre_LT': '',
        'Subd': '',        'Use_Code': '',        'LEGAL': '',        'HeatedSF_From': '',
        'YearBuilt_To': '',        'BldgType': '',        'XFOBcode': '',        'SaleDateFrom': '',
        'SaleDateTo': '',        'SalePriceFrom': '',        'SalePriceTo': '',        'Sale_Vimp': '',
        'SaleBook': '',        'SalePage': '',        'submit_RecordSearch': 'Run Search >>',        'GoToPage': '',
        'SearchMenu': '',        'backCheck': '1',        'clientWidth': '',        'clientHeight': '',
    }
    next_page_data = {
        'iTotalPage': '',
        'iRecordCount': '',
        'SearchResults_File': '',
        'DisplayPage': '',
        'GoToNextPage': '',

        'PageCount': '',        'CurrentPage': '',
        'OrderBy': 'Street_Name, StreetNum_Search, PIN',        'ResultsMenu': '',        'Map_Rec': '',
        'Show_Rec': '',        'LastSaleOnly': '',        'ShowDownloadButton': '',        'PARCEL_Buffer': '',
        'PARCEL_Buffer_Label': '',        'clientOrientation': '',        'clientWidth': '',        'clientHeight': '',
    }

    parcel_url = 'https://www.desotopa.com/gis/recordSearch_3_Details/'
    parcel_data = {
        'OrderBy': '',    'SearchResults_File': '',   'Show_Rec': '',        'iTotalPage': '',        'iRecordCount': '',
        'clientWidth': '', 'clientHeight': '',        'bHandoff_salePage': '',        'TabView': '',
        'currentTab': '',  'PageCount': '',        'CurrentPage': '',        'DisplayPage': '',
        'clientOrientation': 'window.orientation', 'save': '', 'tempPIN': '', 'zoomPIN': '', 'mapCenter': '',
        'mapZoom': '', 'bHandoff': '', 'bHandoff_PIN': '','bHandoff_saleBook': '',
    }


    custom_settings = {
        'FEEDS': {
            'Output/desoto_parcel_data.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        },  # Zyte API

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
        # # "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
    }

    def start_requests(self):
        file_path = "input/desoto_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        for index, search_address in enumerate(addresses):
            priority = len(addresses) - index
            print(index, search_address)
            payload = deepcopy(self.data)
            payload['StreetName'] = search_address
            page_no = 0
            Show_Rec = 1
            yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                                     priority=priority, meta={'search_address':search_address,
                      'page_no':page_no,'Show_Rec':Show_Rec, 'cookiejar':index,'priority':priority})

    def parse(self, response):
        if response.xpath("//*[contains(text(),'No Matching Records Found!')]"):
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/desoto_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            Show_Rec = response.meta['Show_Rec']
            for parcel in response.css('.resultstable tr')[1:]:
                parcel_id = ' '.join(element.strip() for element in parcel.css('.pointer ::text').getall())
                print(Show_Rec, parcel_id)

                payload = deepcopy(self.parcel_data)
                payload['SearchResults_File'] = response.xpath("//input[contains(@name,'SearchResults_File')]/@value").get('').strip()
                payload['OrderBy'] = response.xpath("//input[contains(@name,'OrderBy')]/@value").get('').strip()
                payload['iTotalPage'] = response.xpath("//input[contains(@name,'iTotalPage')]/@value").get('').strip()
                payload['iRecordCount'] = response.xpath("//input[contains(@name,'iRecordCount')]/@value").get('').strip()
                payload['CurrentPage'] = response.xpath("//input[contains(@name,'CurrentPage')]/@value").get('').strip()
                payload['DisplayPage'] = response.xpath("//input[contains(@name,'DisplayPage')]/@value").get('').strip()
                payload['Show_Rec'] = str(Show_Rec)
                Show_Rec += 1
                yield scrapy.FormRequest(url=self.parcel_url, formdata=payload, method='POST', callback=self.parse_detail, headers=self.headers,
                    priority=response.meta['priority'],
                    meta={'search_address': response.meta['search_address'],'cookiejar': response.meta['cookiejar']
                          ,'priority':response.meta['priority']})


            '''     PAGINATION       '''
            if response.xpath("//input[contains(@name,'GoToNextPage')]"):
                payload = deepcopy(self.next_page_data)
                payload['SearchResults_File'] = response.xpath("//input[contains(@name,'SearchResults_File')]/@value").get('').strip()
                payload['iRecordCount'] = response.xpath("//input[contains(@name,'iRecordCount')]/@value").get('').strip()
                payload['GoToNextPage'] = response.xpath("//input[contains(@name,'GoToNextPage')]/@value").get('').strip()
                page_no = response.meta['page_no'] + 1
                payload['DisplayPage'] = str(page_no)
                yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                                        priority=response.meta['priority'],
                meta={'search_address': response.meta['search_address'],'page_no':page_no, 'Show_Rec':Show_Rec,
                      'cookiejar':response.meta['cookiejar'],'priority':response.meta['priority']})


    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()

        property_info['parcel_id'] = response.xpath("//td[contains(text(),'Parcel:')]/following-sibling::td[2]/b/text()").get('').strip()
        property_address = response.xpath("//td[contains(text(),'Site')]/following-sibling::td[1]/text()").get('').strip()
        property_info['location_address'] = property_address.strip()
        property_info['property_address'] = property_address.strip()
        zip_code_pattern = r'\b\d{5}\b'
        zip_code_match = re.search(zip_code_pattern, property_address)
        if zip_code_match:
            property_info['property_zipcode'] = zip_code_match.group()
        else:
            property_info['property_zipcode'] = ''

        owner_info = response.xpath("//td[contains(text(),'Owner')]/following-sibling::td[1]/text()").getall()
        property_info['owner1'] = response.xpath("//td[contains(text(),'Owner')]/following-sibling::td[1]/b/text()").get('').strip().replace('&', '')
        if '&' in response.xpath("//td[contains(text(),'Owner')]/following-sibling::td[1]/b/text()").get('').strip():
            property_info['owner2'] = owner_info[-3].strip()
        else:
            property_info['owner2'] = ''
        if '&' in owner_info[-3].strip():
            property_info['owner3'] = owner_info[3].strip()
        else:
            property_info['owner3'] = ''

        property_info['mailing_address_1'] = owner_info[-2].strip()
        property_info['mailing_address_2'] = ''
        csz = owner_info[-1].strip()
        print('City State and Zipcode is ', csz)
        pattern = r"^(.+?)\s+([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$"
        match = re.match(pattern, csz)
        if match:
            property_info['mailing_city'] = match.group(1)
            property_info['mailing_state'] = match.group(2)
            property_info['mailing_zipcode'] = match.group(3)
        else:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = '','',''

        property_info['sec_twp_rng'] = response.xpath("//td[contains(text(),'S/T/R')]/following-sibling::td[1]/text()").get('').strip()
        property_info['taxing_district'] = response.xpath("//td[contains(text(),'Tax District')]/following-sibling::td[1]/text()").get('').strip()
        property_info['property_use_code'] = response.xpath("//td[contains(text(),'Use Code')]/following-sibling::td[1]/text()").get('').strip()
        l_desc = response.xpath("//td[contains(text(),'Desc')]/following-sibling::td[1]/table/tr/td/text()").getall()
        property_info['legal_description'] = ''.join(element.strip() for element in l_desc)  # description
        property_info['search_address'] = response.meta.get('search_address')
        ''' Empty fields'''
        property_info['subdivision'] = ''
        property_info['neighborhood'] = ''
        property_info['census'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
        property_info['property_id'] = ''
        property_info['millage_group'] = ''
        property_info['property_class'] = ''
        property_info['affordable_housing'] = ''
        property_info['neighborhood_code'] = ''
        property_info['subdivision_code'] = ''
        property_info['acreage'] = ''
        property_info['mileage'] = ''
        property_info['homestead_exemption'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''
        item['main_info'] = property_info


        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.xpath("//td[contains(text(),'Parcel:')]/following-sibling::td[2]/b/text()").get('').strip()
        listing['location_address'] = property_address.strip()
        listing['owner1'] =  response.xpath("//td[contains(text(),'Owner')]/following-sibling::td[1]/b/text()").get('').strip().replace('&', '')
        self.save_to_csv(listing)


        '''     2 - land      '''
        land_info_check = response.css('#parcelDetails_LandTable tr:nth-child(2) td')
        if land_info_check.css('::text').get('').strip() == 'N O N E':
            item['land'] = []
        else:
            LAND_INFO = []
            for lands in response.css('#parcelDetails_LandTable tr')[1:-1]:
                land_info = dict()
                land_info['land_use'] = lands.css('td:nth-child(2) ::text').get('').strip()
                units = lands.css('td:nth-child(3) ::text').get('').strip()
                unit, unit_type = units.split(maxsplit=1)
                land_info['num_of_units'] = unit.strip()
                land_info['unit_type'] = unit_type.strip()
                land_info['frontage'] = ''
                land_info['depth'] = ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO


        '''     3 - buildings      '''
        building_info_check = response.css('#parcelDetails_BldgTable tr:nth-child(2) td')
        if building_info_check.css('::text').get('').strip() == 'N O N E':
            item['buildings'] = []
        else:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in response.css('#parcelDetails_BldgTable table tr')[1:-1]:
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = ''
                building_info['beds']  = ''
                building_info['baths']  = ''
                building_info['stories']  = ''
                building_info['half_baths'] = ''
                building_info['built_year']  = buildings.css('td:nth-child(3) ::text').get('').strip()
                building_info['ac']  = ''
                building_info['heat'] = ''
                building_info['floor_cover']  = ''
                building_info['frame_type'] = ''
                building_info['interior_walls']  = ''
                building_info['roof_cover']  = ''
                building_info['exterior_walls']  = ''
                building_info['gross_building_area'] = buildings.css('td:nth-child(5) ::text').get('').strip()
                building_info['living_area'] = buildings.css('td:nth-child(4) ::text').get('').strip()
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list


        '''     4 - valuations           -  Will do it at the end        '''
        values_check = response.xpath("//b[contains(text(),'Property & Assessment Values')]")
        if values_check:
            VALUES_INFO = []
            valuation_count = 1
            for values in response.xpath("//b[contains(text(),'Property & Assessment Values')]/parent::td[1]/parent::tr[1]/following-sibling::tr[1]/td"):
                values_value = dict()
                values_value['id'] = valuation_count
                valuation_count += 1
                values_value['real_estate_id'] = ''
                values_value['year'] = values.css('tr:nth-child(1) td b::text').get('').strip()
                values_value['land'] = values.xpath(".//td[contains(text(),'Mkt Land')]/following-sibling::td[1]/text()").get('').strip()
                values_value['building'] = values.xpath(".//td[contains(text(),'Building')]/following-sibling::td[1]/text()").get('').strip()
                values_value['extra_feature'] = values.xpath(".//td[contains(text(),'XFOB')]/following-sibling::td[1]/text()").get('').strip()
                values_value['just'] = values.xpath(".//td[contains(text(),'Just')]/following-sibling::td[1]/text()").get('').strip()
                values_value['assessed'] = values.xpath(".//td[contains(text(),'Assessed')]/following-sibling::td[1]/text()").get('').strip()
                values_value['exemptions'] = values.xpath(".//td[contains(text(),'Exempt')]/following-sibling::td[1]/table/tr/td/text()").get('').strip()
                taxable = values.xpath(".//td[contains(text(),'Total')]/following-sibling::td[1]/text()").get('')
                values_value['taxable'] = ''.join(element.strip().replace('\t', ' ').replace('\n','') for element in taxable)  # description
                values_value['cap'] = values.xpath(".//td[contains(text(),'SOH/10%')]/following-sibling::td[1]/text()").get('').strip()
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


        '''     5 - extra_features          '''
        no_extra_features_check = response.css('#parcelDetails_XFOBTable tr:nth-child(2) td')
        if no_extra_features_check.css('::text').get('').strip() == 'N O N E':
            item['extra_features'] = []
        else:
            X_FEATURES_INFO = []
            x_id = 1
            for values in response.css('#parcelDetails_XFOBTable table tr')[1:]:
                extra_features_values = dict()
                extra_features_values['id'] = x_id
                x_id += 1
                extra_features_values['real_estate_id'] = values.css('td:nth-child(1) ::text').get('').strip()
                extra_features_values['building_number'] = ''
                extra_features_values['desc'] = values.css('td:nth-child(2) ::text').get('').strip()
                extra_features_values['units'] = values.css('td:nth-child(5) ::text').get('').strip()
                extra_features_values['unit_type'] = ''
                extra_features_values['year'] = values.css('td:nth-child(3) ::text').get('').strip()
                X_FEATURES_INFO.append(extra_features_values)
            item['extra_features'] = X_FEATURES_INFO


        '''     6 - transactions      '''
        sales =  response.css('#parcelDetails_SalesTable tr:nth-child(2) td')
        if sales.css('::text').get('').strip() == 'N O N E':
            item['transactions'] = []
        else:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css('#parcelDetails_SalesTable table tr')[1:]:
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('td:nth-child(1) ::text').get('').strip()
                sales_info['document_number'] = sales.css('td:nth-child(3) a::text').get('').strip()
                sales_info['qualification_code'] = sales.css('td:nth-child(6) ::text').get('').strip()
                sales_info['grantor'] = ''
                sales_info['grantee'] = ''
                sales_info['document_type'] = sales.css('td:nth-child(4) ::text').get('').strip()
                sales_info['price'] = sales.css('td:nth-child(2) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST


        '''     7 - permits         -      No data available on Website             '''
        item['permits'] = []

        '''     8 - flood_zones     -      No data available on Website             '''
        item['flood_zones'] = []


        # print(item)
        yield item


    def save_to_csv(self, data):
        with open('Output/desoto_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass
