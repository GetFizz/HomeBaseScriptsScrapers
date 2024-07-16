#####       Pasco

import csv
import re
from copy import deepcopy
import scrapy
from scrapy.utils.response import open_in_browser
class Pasco(scrapy.Spider):
    name = 'Pasco'
    prefix = 'https://search.pascopa.com/'
    url = 'https://search.pascopa.com/default.aspx?mprs=2&src=Q&pid=add&key=DMG&add2={}&add=Submit&recs=1000&pg={}'
    # url = 'https://search.pascopa.com/default.aspx?mprs=2&src=Q&pid=add&key=DMG&add2=Bay+Avenue&add=Submit&sf=3&so=1&recs=10&pg=3'

    headers = {
        'authority': 'search.pascopa.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        # 'cookie': '_ga=GA1.2.2108802508.1710161703; _gid=GA1.2.1824796525.1710161703',
        'referer': 'https://search.pascopa.com/',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    custom_settings = {
        'FEEDS': {
            'Output/pasco_parcel_data.json': {
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
        # "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
    }

    def start_requests(self):
        file_path = "input/pasco_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        for address in addresses:
            page_no = 1
            yield scrapy.Request(url=self.url.format(address, page_no), callback=self.parse, headers=self.headers,
                                 meta={'search_address': address, 'page_no':page_no})

    def parse(self, response):
        if response.xpath("//*[contains(text(),'No Parcels match your query')]"):
            search_address = response.meta.get('search_address')
            with open('Output/pasco_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            for row_div in response.css('table.results>tbody>tr'):
                parcel_url = self.prefix + row_div.css('td:nth-child(2) ::attr(href)').get('').strip()
                parcel_id = row_div.css('td:nth-child(2) ::text').get('').strip()
                yield scrapy.Request(url=parcel_url, callback=self.parse_detail, headers=self.headers,
                                     meta={'search_address':response.meta.get('search_address')})
            # yield scrapy.Request(url='https://search.pascopa.com/parcel.aspx?parcel=36-26-21-0020-00000-2410', callback=self.parse_detail,
            #                      headers=self.headers, meta={'search_address':response.meta.get('search_address')})

            if response.xpath("//a[contains(text(),'Next')]"):
                page_no = response.meta.get('page_no') + 1
                address = response.meta.get('search_address')
                yield scrapy.Request(url=self.url.format(address, page_no), callback=self.parse, headers=self.headers,
                                     meta={'search_address': address, 'page_no':page_no})

    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::div[1]/span/text()").get('').strip()

        mailing_add = response.css('#lblMailingAddress ::text').getall()
        mailing_address = ', '.join(element.strip() for element in mailing_add[-2:])
        property_info['mailing_address_1'] = mailing_address
        property_info['mailing_address_2'] = ''
        pattern = re.compile(r'\b([^,]+),\s*([A-Za-z]{2})\s*(\d{5})\b')
        matches = pattern.findall(mailing_address)
        if matches:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = matches[0]

        owner_match = re.match(r'^([^\d,]+)', ' '.join(element.strip() for element in mailing_add))
        if owner_match:
            owner_names = owner_match.group(1).strip()
            owner_list = owner_names.split('&')
            property_info['owner1'] = owner_list[0].strip()
            property_info['owner2'] = owner_list[1].strip() if len(owner_list) > 1 else ''
        property_info['owner3'] = ''

        phy_add = response.css('#lblPhysicalAddress ::text').get('').strip()
        property_info['location_address'] = phy_add.replace('\xa0',' ')
        property_info['property_address'] = phy_add.replace('\xa0',' ')
        zipcode_matches = re.finditer(r'\b(\d{5}(?:-\d{4})?)\b', phy_add)
        last_zipcode = None
        for match in zipcode_matches:
            last_zipcode = match.group(1)
        property_info['property_zipcode'] = last_zipcode

        property_info['legal_description'] = (response.css('#lblLegalDescription ::text').get('').strip()).replace('[...]','')
        if response.css('#lblCDD ::text').get('').strip() == 'N/A':
            property_info['taxing_district'] = ''
        else:
            property_info['taxing_district'] = response.css('#lblCDD ::text').get('').strip()

        property_info['subdivision'] = ''
        property_info['sec_twp_rng'] = ''
        property_info['census'] = ''
        property_info['property_use_code'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
        property_info['neighborhood'] = ''
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
        listing['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::div[1]/span/text()").get('').strip()
        listing['location_address'] = phy_add.replace('\xa0',' ')
        listing['owner1'] = property_info['owner1']
        listing['owner2'] = property_info['owner2']
        self.save_to_csv(listing)


        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(@id,'tblLandLines')]")
        if land_info_check:
            LAND_INFO = []
            for lands in response.css('#tblLandLines>tr')[1:]:
                land_info = dict()
                land_info['land_use'] = lands.css('td:nth-child(2) ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(6) ::text').get('').strip()
                land_info['unit_type'] = lands.css('td:nth-child(7) ::text').get('').strip()
                land_info['frontage'] = ''
                land_info['depth'] = ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []


        '''     3 - buildings      '''
        building_info_check = response.xpath("//*[contains(@id,'tblSubLines')]").get('').strip()
        if building_info_check:
            BUILDING_INFO_list = []
            building_count = 1
            buildings_details = response.css('#buildingDetailTable')
            for buildings in response.css('#tblSubLines tr')[1:]:
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = buildings.css('td:nth-child(2)').get('').strip()
                building_info['building_no'] = buildings.css('td:nth-child(1)').get('').strip()
                building_info['beds']  = ''
                building_info['baths']  = buildings_details.xpath(".//*[contains(text(),'Baths')]/following-sibling::div/span/text()").get('').strip()
                building_info['stories']  = buildings_details.xpath(".//*[contains(text(),'Stories')]/following-sibling::div/span/text()").get('').strip()
                building_info['half_baths'] = ''
                building_info['built_year']  = buildings_details.xpath(".//*[contains(text(),'Year Built')]/following-sibling::div/span/text()").get('').strip()
                building_info['ac']  = buildings_details.xpath(".//*[contains(text(),'A/C')]/following-sibling::div/span/text()").get('').strip()
                building_info['heat'] = buildings_details.xpath(".//*[contains(text(),'Heat')]/following-sibling::div/span/text()").get('').strip()
                building_info['floor_cover']  = buildings_details.xpath(".//*[contains(text(),'Flooring 1')]/following-sibling::div/span/text()").get('').strip()
                building_info['frame_type']  = ''
                building_info['interior_walls']  = buildings_details.xpath(".//*[contains(text(),'Interior Wall 1')]/following-sibling::div/span/text()").get('').strip()
                building_info['roof_cover']  = buildings_details.xpath(".//*[contains(text(),'Roof Cover')]/following-sibling::div/span/text()").get('').strip()
                building_info['exterior_walls']  = buildings_details.xpath(".//*[contains(text(),'Exterior Wall 1')]/following-sibling::div/span/text()").get('').strip()
                building_info['gross_building_area'] = buildings.css('td:nth-child(4)').get('').strip()
                building_info['living_area'] = ''
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []


        '''     4 - valuations      '''
        valuation_check = response.xpath("//*[contains(@id,'parcelValueTable')]")
        if valuation_check:
            VALUATION = []
            valuation_count = 1
            for values in response.xpath("//*[contains(@id,'parcelValueTable')]"):
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''
                certified_values['year'] = ''
                certified_values['building'] = values.xpath(".//*[contains(text(),'Building')]/following-sibling::td/span/text()").get('').strip()
                certified_values['land'] = values.css('#lblValueLand ::text').get('').strip()
                certified_values['extra_feature'] = values.css('#lblValueExtraFeatures ::text').get('').strip()
                certified_values['just'] = values.css('#lblValueJust ::text').get('').strip()
                certified_values['assessed'] = values.xpath(".//*[contains(text(),'Assessed')]/following-sibling::td/span/text()").get('').strip()
                certified_values['exemptions'] = values.xpath(".//*[contains(text(),'Homestead Exemption')]/following-sibling::td/span/text()").get('').strip()
                certified_values['taxable'] = values.css('#lblValueCountyTaxable ::text').get('').strip()
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
        extra_features_check = response.xpath("//*[contains(@id,'tblXFLines')]/tr")[1:]
        if extra_features_check:
            EXTRA_FEATURES_list = []
            for extra_features in response.css('#tblXFLines>tr')[1:]:
                extra_f = dict()
                extra_f['id'] = extra_features.css('td:nth-child(1) ::text').get('').strip()
                extra_f['real_estate_id'] = extra_features.css('td:nth-child(2) ::text').get('').strip()
                extra_f['building_number'] = ''
                extra_f['desc'] = extra_features.css('td:nth-child(3) ::text').get('').strip()
                extra_f['units'] = extra_features.css('td:nth-child(5) ::text').get('').strip()
                extra_f['unit_type'] = ''
                extra_f['year'] = extra_features.css('td:nth-child(4) ::text').get('').strip()
                EXTRA_FEATURES_list.append(extra_f)
            item['extra_features'] = EXTRA_FEATURES_list
        else:
            item['extra_features'] = []


        '''     6 - transactions      '''
        sales = response.xpath("//*[contains(@id,'tblSaleLines')]/tr")[1:]
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css('#tblSaleLines>tr')[1:]:
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('td:nth-child(1) ::text').get('').strip()
                sales_info['document_number'] = sales.css('td:nth-child(2) a::text').get('').strip()
                sales_info['qualification_code'] = ''
                sales_info['grantor'] = ''
                sales_info['grantee'] = ''
                sales_info['document_type'] = sales.css('td:nth-child(3) ::text').get('').strip()
                sales_info['price'] = sales.css('td:nth-child(6) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []

        '''     7 - permits         -      No data available on Website             '''
        permits = response.xpath("//*[contains(text(),'permits')]/text()").get('').strip()
        if permits:
            item['permits'] = permits
        else:
            item['permits'] = []

        '''     8 - flood_zones     -      No data available on Website             '''
        flood_zones = response.xpath("//*[contains(text(),'flood zones')]/text()").get('').strip()
        if flood_zones:
            item['flood_zones'] = flood_zones
        else:
            item['flood_zones'] = []

        yield item


    def save_to_csv(self, data):
        with open('Output/pasco_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1','owner2']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass
