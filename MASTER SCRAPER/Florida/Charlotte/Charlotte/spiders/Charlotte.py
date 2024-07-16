#########       Charlotte
import json
import re
import csv
import scrapy
from copy import deepcopy

class Charlotte(scrapy.Spider):
    name = 'Charlotte'
    prefix = 'https://www.ccappraiser.com/'
    url = "https://www.ccappraiser.com/RPSearchQuery.asp"
    cookies = {
        'ASPSESSIONIDSUDBRQTR': 'IAFMHJJAEDCEMEPFIOPBNJAP',
        'arp_scroll_position': '0',
    }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': '__utmc=167135148; __utmz=167135148.1710863724.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=167135148.214680821.1710863724.1710875384.1710881475.4; ASPSESSIONIDSUDBRQTR=CPLAIJJAICCFHJAPEHOOMOGB; arp_scroll_position=426',
        'Origin': 'https://www.ccappraiser.com',
        'Referer': 'https://www.ccappraiser.com/RPSearchEnter.asp',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }

    data = {
        'PropertyAddressStreetName': '6th',
        'ParcelID_number': '',        'PropertyAddressNumber': '',
        'owner': '',        'ShortLegal': '',        'CurrentLandUse': 'Any',
        'LandUseCode': '',        'BuildingUseCode': '',        'PADZip': '',        'OwnerCountry': '',
        'gen': 'T', 'tax': 'T', 'bld': 'T', 'lnd': 'T', 'sal': 'T', 'oth': 'T', 'leg': 'T',
        'Run Real Property Search': 'Run Search',
    }

    custom_settings = {
        'FEEDS': {
            'Output/charlotte_parcel_data.json': {
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
        file_path = "input/charlotte_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        count = 1
        for index, address in enumerate(addresses):
            print(count, address)
            count += 1
            yield scrapy.Request(
                url='https://www.ccappraiser.com/RPSearchEnter.asp?',
                meta={'search_address':address, 'cookiejar': index},
                callback=self.parse_search,
                dont_filter=True
            )

    def parse_search(self, response):
        search_address = response.meta['search_address']
        payload = deepcopy(self.data)
        payload['PropertyAddressStreetName'] = search_address
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                                 meta={'search_address':search_address, 'cookiejar': response.meta['cookiejar']})

    def parse(self, response):
        if response.xpath("//*[contains(text(),'Sorry, your selection returned no properties.')]"):
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/charlotte_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            for row_div in response.css('.w3-border-blue a'):
                parcel_url = self.prefix + row_div.css('::attr(href)').get('').strip()
                parcel_id = row_div.css('::text').get('').strip()
                yield scrapy.Request(url=parcel_url,  callback=self.parse_detail,  headers=self.headers,
                                     meta={'search_address':response.meta.get('search_address'), 'parcel_id':parcel_id,
                                           'cookiejar': response.meta['cookiejar']})

            '''     PAGINATION       '''
            next_page = response.xpath("//strong[contains(text(),'Next')]/parent::a/@href").get('').strip()
            if next_page:
                next_page_url = self.prefix + next_page
                payload = deepcopy(self.data)
                payload['PropertyAddressStreetName'] = response.meta.get('search_address')
                yield scrapy.FormRequest(url=next_page_url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                            meta={'search_address': response.meta.get('search_address'), 'cookiejar': response.meta['cookiejar']})

    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.meta.get('parcel_id')
        property_add = response.xpath("//*[contains(text(),'Property Address:')]/parent::div/following-sibling::div[1]/text()").getall()
        address_string = ', '.join(element.strip() for element in property_add[:2])
        property_address = re.sub(r'\s+', ' ',address_string.replace("\r", "").replace("\n", "").replace("\t", "")).strip()
        property_info['location_address'] = property_address.strip()
        property_info['property_address'] = property_address.strip()

        property_zip = response.xpath("//*[contains(text(),'Property City & Zip:')]/parent::div/following-sibling::div[1]/text()").get('').strip()
        zip_code_pattern = r'\b\d{5}\b'
        zip_code_match = re.search(zip_code_pattern, property_zip)
        if zip_code_match:
            property_info['property_zipcode'] = zip_code_match.group()
        else:
            property_info['property_zipcode'] = ''

        owner_info = response.xpath("//*[contains(text(),'Owner:')]/following-sibling::div[1]/text()").getall()
        owners = owner_info[0].split(" & ")
        # Extract owner1, owner2, owner3
        property_info['owner1'] = owners[0].strip()
        property_info['owner2'] = owners[1].strip() if len(owners) > 1 else ''
        property_info['owner3'] = owners[2].strip() if len(owners) > 2 else ''
        address_parts = owner_info[-2:]
        mailing_address = ', '.join(element.strip() for element in address_parts)

        property_info['mailing_address_1'] = mailing_address.replace('\xa0','')
        property_info['mailing_address_2'] = ''
        pattern = re.compile(r'\b([^,]+),\s*([A-Za-z]{2})\s*(\d{5})\b')
        matches = pattern.findall(owner_info[-1])
        if matches:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = matches[0]

        legal_description = response.xpath("//*[contains(text(),'Long Legal:')]/parent::div/text()").getall()
        property_info['legal_description'] = legal_description[-1].strip()

        n_code = response.xpath("//*[contains(text(),'Market Area / Neighborhood / Subneighborhood:')]/parent::div/following-sibling::div/text()").get('').strip()
        match = re.search(r'/(?P<desired_portion>\d+)/', n_code)
        if match:
            property_info['neighborhood_code'] = match.group('desired_portion')
        else:
            property_info['neighborhood_code'] = ''

        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Section/Township/Range:')]/parent::div/following-sibling::div/text()").get('').strip()
        property_info['waterfront_code'] = response.xpath("//*[contains(text(),'Waterfront:')]/parent::div/following-sibling::div[1]/text()").get('').strip()
        property_info['taxing_district'] = response.xpath("//*[contains(text(),'Taxing District:')]/parent::div/following-sibling::div[1]/text()").get('').strip()
        property_info['zoning_code'] = response.xpath("//*[contains(text(),' Zoning Code: ')]/parent::strong/parent::div/following-sibling::div[1]/a/text()").get('').strip()
        property_info['search_address'] = response.meta.get('search_address')

        property_info['millage_group'] = ''
        property_info['mileage'] = ''
        property_info['acreage'] = ''
        property_info['homestead_exemption'] = ''
        property_info['subdivision'] = ''
        property_info['census'] = ''
        property_info['property_id'] = ''
        property_info['neighborhood'] = ''
        property_info['property_use_code'] = ''
        property_info['municipality'] = ''
        property_info['parcel_desc'] = ''
        property_info['property_class'] = ''
        property_info['affordable_housing'] = ''
        property_info['subdivision_code'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''
        item['main_info'] = property_info


        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.meta.get('parcel_id')
        listing['location_address'] = property_address
        listing['owner1'] = owners[0].strip()
        self.save_to_csv(listing)


        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(text(),'Land Information')]/following-sibling::tr[2]")
        if land_info_check:
            LAND_INFO = []
            for lands in response.xpath("//*[contains(text(),'Land Information')]/following-sibling::tr")[1:]:
                land_info = dict()
                land_info['land_use'] = lands.css('td:nth-child(3) ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(6) ::text').get('').strip()
                land_info['unit_type'] = lands.css('td:nth-child(5) ::text').get('').strip()
                land_info['frontage'] = ''
                land_info['depth'] = ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []


        '''     3 - buildings      '''
        building_info_check = response.xpath("//*[contains(text(),'Building Information')]/following-sibling::tr[2]")
        if building_info_check:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in response.xpath("//*[contains(text(),'Building Information')]/following-sibling::tr")[1:]:
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = buildings.css('td:nth-child(1) ::text').get('').strip()
                building_info['beds']  = buildings.css('td:nth-child(9) ::text').get('').strip()
                building_info['baths']  = ''
                building_info['stories']  = ''
                building_info['half_baths'] = ''
                building_info['built_year']  = buildings.css('td:nth-child(5) ::text').get('').strip()
                building_info['ac']  = ''
                building_info['heat'] = ''
                building_info['floor_cover']  = ''
                building_info['frame_type'] = ''
                building_info['interior_walls']  = ''
                building_info['roof_cover']  = ''
                building_info['exterior_walls']  = ''
                building_info['gross_building_area'] = buildings.css('td:nth-child(13) ::text').get('').strip()
                building_info['living_area'] = ''
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []


        '''     4 - valuations     -      No data available on Website   '''
        item['valuations'] = []

        '''     5 - extra_features     -      No data available on Website   '''
        item['extra_features'] = []

        '''     6 - transactions      '''
        sales = response.xpath("//*[contains(text(),'Sales Information')]/following-sibling::div[1]/table/tr")
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.xpath("//*[contains(text(),'Sales Information')]/following-sibling::div[1]/table/tr"):
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('td:nth-child(1) ::text').get('').strip()
                sales_info['document_number'] = sales.css('td:nth-child(2) a::text').get('').strip()
                sales_info['qualification_code'] = sales.css('td:nth-child(6) a::text').get('').strip()
                sales_info['grantor'] = ''
                sales_info['grantee'] = ''
                sales_info['document_type'] = ''
                sales_info['price'] = sales.css('td:nth-child(4) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []


        '''     7 - permits         -      No data available on Website             '''
        item['permits'] = []


        '''     8 - flood_zones     -      No data available on Website             ''' 'FEMA Flood Zone'
        flood_zones = response.xpath("//*[contains(text(),'FEMA Flood Zone')]/following-sibling::tr[2]")
        if flood_zones:
            FLOOD_INFO = []
            flood_count = 1
            for floods in response.xpath("//*[contains(text(),'FEMA Flood Zone')]/following-sibling::tr")[1:]:
                flood_info = dict()
                flood_info['id'] = flood_count
                flood_count += 1
                flood_info['real_estate_id'] = ''
                flood_info['firm_panel'] = floods.css('td:nth-child(1) ::text').get('').strip()
                flood_info['floodway'] = floods.css('td:nth-child(2) ::text').get('').strip()
                flood_info['sfha'] = floods.css('td:nth-child(3) ::text').get('').strip()
                flood_info['zone'] = floods.css('td:nth-child(4) ::text').get('').strip()
                flood_info['community'] = floods.css('td:nth-child(7) ::text').get('').strip()
                flood_info['base_flood_elevation'] = floods.css('td:nth-child(8) ::text').get('').strip()
                flood_info['cfha'] = ''
                FLOOD_INFO.append(flood_info)
            item['flood_zones'] = FLOOD_INFO
        else:
            item['flood_zones'] = []

        yield item


    def save_to_csv(self, data):
        with open('Output/charlotte_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)

