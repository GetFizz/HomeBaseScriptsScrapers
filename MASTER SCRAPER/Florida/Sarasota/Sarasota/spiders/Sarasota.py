############        Sarasota

import csv
import re
import scrapy
from copy import deepcopy

class Sarasota(scrapy.Spider):
    name = 'Sarasota'
    prefix = 'https://www.sc-pa.com'
    url = "https://www.sc-pa.com/propertysearch/Result"
    url1 = 'https://www.sc-pa.com/propertysearch/Result?qid={}&Page={}&PageSize=1000'

    cookies = {
        '_ga': 'GA1.1.659984222.1709376555',
        'ASP.NET_SessionId': 'pfacchqqhfid35pdswhcmybw',
        '_ga_GT1JFP7Q9G': 'GS1.1.1711577790.13.0.1711577790.0.0.0',
        'arp_scroll_position': '100',
    }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': '_ga=GA1.1.659984222.1709376555; ASP.NET_SessionId=pfacchqqhfid35pdswhcmybw; _ga_GT1JFP7Q9G=GS1.1.1711577790.13.0.1711577790.0.0.0; arp_scroll_position=100',
        'Origin': 'https://www.sc-pa.com',
        'Referer': 'https://www.sc-pa.com/propertysearch',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

    custom_settings = {
        'FEEDS': {
            'Output/sarasota_parcel_data.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        }
    }

    def start_requests(self):
        file_path = "input/sarasota_properties.csv"
        with open(file_path, 'r', newline='', encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['Link to Property Detail Page']
                if address:
                    yield scrapy.Request(url=address, callback=self.parse, headers=self.headers)


    def parse(self, response):
        item = dict()

        property_info = dict()
        property_info['parcel_id'] = str(re.search(r'/(\d+)/$', response.url).group(1))
        property_info['location_address'] = response.xpath('//li[contains(text(),"Situs Address:")]/following-sibling::li[1]/text()').get('').strip()
        property_info['owner1'] = response.xpath('//li[contains(text(),"Ownership")]/following-sibling::li[1]/text()').get('').strip()
        self.save_to_csv(property_info)

        property_info['municipality'] = response.xpath('//strong[contains(text(),"Municipality")]/parent::li/text()').get('').strip()
        property_info['property_use_code'] = response.xpath('//strong[contains(text(),"Property Use")]/parent::li/text()').get('').strip()
        property_info['subdivision'] = response.xpath('//strong[contains(text(),"Subdivision")]/parent::li/text()').get('').strip()
        property_info['acreage'] = response.xpath('//strong[contains(text(),"Land Area")]/parent::li/text()').get('').strip()
        property_info['waterfront_code'] = response.xpath('//strong[contains(text(),"Waterfront")]/parent::li/text()').get('').strip()
        property_info['parcel_desc'] = response.xpath('//strong[contains(text(),"Parcel Description:")]/parent::li/text()').get('').strip()
        property_info['search_address'] = response.xpath('//li[contains(text(),"Situs Address:")]/following-sibling::li[1]/text()').get('').strip()

        property_info['sec_twp_rng'] = response.xpath('//strong[contains(text(),"Sec/Twp/Rge")]/parent::li/text()').get('').strip()
        property_info['census'] = response.xpath('//strong[contains(text(),"Census")]/parent::li/text()').get('').strip()
        property_info['zoning_code'] = response.xpath('//strong[contains(text(),"Zoning")]/parent::li/text()').get('').strip()

        mailing_add = response.xpath("//li[contains(@class,'app-links small')]/preceding-sibling::li[1]/text()").get('').strip()
        property_info['mailing_address_1'] = mailing_add
        property_info['mailing_address_2'] = response.xpath('//strong[contains(text(),"mailing_address_2")]/parent::li/text()').get('').strip()
        pattern = r"(?P<city>.+?),\s*([A-Z]{2}),?\s*(?P<zipcode>\d{5}(?:-\d{4})?)$"
        parts = mailing_add.split(',')
        property_info['mailing_city'] = parts[1].strip() if len(parts) == 4 else ''
        match = re.search(pattern, mailing_add)
        if match:
            property_info['mailing_state'] = match.group(2)
            property_info['mailing_zipcode'] = match.group("zipcode")
        else:
            property_info['mailing_state'], property_info['mailing_zipcode'] = '',''

        owner_info = response.css(".resultl li ::text").getall()
        property_info['owner1'] = owner_info[1].strip()
        index = None
        for i, element in enumerate(owner_info):
            if mailing_add in element:
                index = i
                break
        if index-2 == 1:
            property_info['owner2'] = owner_info[2].strip()
            property_info['owner3'] = ''
        elif index-2 == 2:
            property_info['owner2'] = owner_info[2].strip()
            property_info['owner3'] = owner_info[3].strip()
        else:
            property_info['owner2'] = ''
            property_info['owner3'] = ''

        property_address = response.xpath("//li[contains(text(),'Situs Address:')]/following-sibling::li[1]/text()").get('').strip()
        property_info['property_address'] = property_address.strip()
        pattern = r'\b\d{5}(?:-\d{4})?\b$'
        match = re.search(pattern, property_address)
        property_info['property_zipcode'] = match.group() if match else ""

        ##  Values not available on site
        property_info['property_id'] = response.xpath('//strong[contains(text(),"property_id")]/parent::li/text()').get('').strip()
        property_info['millage_group'] = response.xpath('//strong[contains(text(),"millage_group")]/parent::li/text()').get('').strip()
        property_info['legal_description'] = response.xpath('//strong[contains(text(),"legal_description")]/parent::li/text()').get('').strip()
        property_info['neighborhood'] = response.xpath('//strong[contains(text(),"neighborhood")]/parent::li/text()').get('').strip()
        property_info['property_class'] = response.xpath('//strong[contains(text(),"property_class")]/parent::li/text()').get('').strip()
        property_info['affordable_housing'] = response.xpath('//strong[contains(text(),"affordable_housing")]/parent::li/text()').get('').strip()
        property_info['neighborhood_code'] = response.xpath('//strong[contains(text(),"neighborhood_code")]/parent::li/text()').get('').strip()
        property_info['subdivision_code'] = response.xpath('//strong[contains(text(),"subdivision_code")]/parent::li/text()').get('').strip()
        property_info['taxing_district'] = response.xpath('//strong[contains(text(),"taxing_district")]/parent::li/text()').get('').strip()
        property_info['mileage'] = response.xpath('//strong[contains(text(),"mileage")]/parent::li/text()').get('').strip()
        property_info['homestead_exemption'] = response.xpath('//strong[contains(text(),"homestead_exemption")]/parent::li/text()').get('').strip()
        property_info['homestead_exemption_grant_year'] = response.xpath('//strong[contains(text(),"homestead_exemption_grant_year")]/parent::li/text()').get('').strip()
        property_info['pool'] = response.xpath('//strong[contains(text(),"pool")]/parent::li/text()').get('').strip()
        item['main_info'] = property_info


        '''     land     -      No land data available on site       '''
        item['land'] = []


        '''     Buildings         '''
        no_buildings_check = response.xpath("//*[contains(text(),'Vacant Land')]/text()").get('').strip()
        if no_buildings_check:
            item['buildings'] = []
        else:
            BUILDING_INFO = []
            buildings_check = response.xpath("//span[contains(text(),'Buildings')]/following-sibling::table[1]/tbody")
            building_count = 1
            for values in buildings_check.css('tr'):
                buildings_values = dict()
                buildings_values['id'] = building_count
                building_count += 1
                buildings_values['real_estate_id'] = ''
                buildings_values['building_no'] = values.css('td:nth-child(2) ::text').get('').strip()
                buildings_values['beds'] = values.css('td:nth-child(3) ::text').get('').strip()
                buildings_values['baths'] = values.css('td:nth-child(4) ::text').get('').strip()
                buildings_values['half_baths'] = values.css('td:nth-child(5) ::text').get('').strip()
                buildings_values['built_year'] = values.css('td:nth-child(6) ::text').get('').strip()
                buildings_values['ac'] = ''
                buildings_values['heat'] = ''
                buildings_values['stories'] = values.css('td:nth-child(10) ::text').get('').strip()
                buildings_values['floor_cover'] = ''
                buildings_values['frame_type'] = ''
                buildings_values['interior_walls'] = ''
                buildings_values['roof_cover'] = ''
                buildings_values['exterior_walls'] = ''
                buildings_values['gross_building_area'] = values.css('td:nth-child(8) ::text').get('').strip()
                buildings_values['living_area'] = values.css('td:nth-child(9) ::text').get('').strip()
                BUILDING_INFO.append(buildings_values)
            item['buildings'] = BUILDING_INFO


        '''     Extra Features      '''
        no_extra_features_check = response.xpath("//*[contains(text(),'There are no extra features associated with this parcel')]/text()").get('').strip()
        if no_extra_features_check:
            item['extra_features'] = []
        else:
            X_FEATURES_INFO = []
            extra_features = response.xpath("//span[contains(text(),'Extra Features')]/following-sibling::table[1]/tbody")
            for values in extra_features.css('tr'):
                extra_features_values = dict()
                extra_features_values['id'] = values.css('td:nth-child(1) ::text').get('').strip()
                extra_features_values['real_estate_id'] = ''
                extra_features_values['building_number'] = values.css('td:nth-child(2) ::text').get('').strip()
                extra_features_values['desc'] = values.css('td:nth-child(3) ::text').get('').strip()
                extra_features_values['units'] = values.css('td:nth-child(4) ::text').get('').strip()
                extra_features_values['unit_type'] = values.css('td:nth-child(5) ::text').get('').strip()
                extra_features_values['year'] = values.css('td:nth-child(6) ::text').get('').strip()
                X_FEATURES_INFO.append(extra_features_values)
            item['extra_features'] = X_FEATURES_INFO


        '''     Values      '''
        values_check = response.xpath("//span[contains(text(),'Values')]")
        if values_check:
            VALUES_INFO = []
            valuation_count = 1
            Values = response.xpath("//span[contains(text(),'Values')]/following-sibling::table[1]/tbody")
            for values in Values.css('tr'):
                values_value = dict()
                values_value['id'] = valuation_count
                valuation_count += 1
                values_value['real_estate_id'] = ''
                values_value['year'] = (values.css('td:nth-child(1) ::text').get('').strip()).replace('*\xa0','')
                values_value['land'] = values.css('td:nth-child(2) ::text').get('').strip()
                values_value['building'] = values.css('td:nth-child(3) ::text').get('').strip()
                values_value['extra_feature'] = values.css('td:nth-child(4) ::text').get('').strip()
                values_value['just'] = values.css('td:nth-child(5) ::text').get('').strip()
                values_value['assessed'] = values.css('td:nth-child(6) ::text').get('').strip()
                values_value['exemptions'] = values.css('td:nth-child(7) ::text').get('').strip()
                values_value['taxable'] = values.css('td:nth-child(8) ::text').get('').strip()
                values_value['cap'] = values.css('td:nth-child(9) ::text').get('').strip()
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


        '''     Sales & Transfers / transactions      '''
        no_sales_transfers_check = response.xpath("//*[contains(text(),'There are no sales or transfers associated with this parcel')]/text()").get('').strip()
        if no_sales_transfers_check:
            item['transactions'] = []
        else:
            SALES_TRANSFERS_INFO = []
            sales_count = 1
            Sales_Transfers = response.xpath("//span[contains(text(),'Sales & Transfers')]/following-sibling::table[1]/tbody")
            for values in Sales_Transfers.css('tr'):
                Sales_Transfers_values = dict()
                Sales_Transfers_values['id'] = sales_count
                sales_count += 1
                Sales_Transfers_values['real_estate_id'] = ''
                Sales_Transfers_values['transfer_date'] = values.css('td:nth-child(1) ::text').get('').strip()
                Sales_Transfers_values['document_number'] = values.css('td:nth-child(3) ::text').get('').strip()
                Sales_Transfers_values['qualification_code'] = values.css('td:nth-child(4) ::text').get('').strip()
                Sales_Transfers_values['grantor'] = values.css('td:nth-child(5) ::text').get('').strip()
                Sales_Transfers_values['grantee'] = ''
                Sales_Transfers_values['document_type'] = values.css('td:nth-child(6) ::text').get('').strip()
                Sales_Transfers_values['price'] = values.css('td:nth-child(2) ::text').get('').strip()
                SALES_TRANSFERS_INFO.append(Sales_Transfers_values)
            item['transactions'] = SALES_TRANSFERS_INFO


        '''     permits      -      No permits data available on site       '''
        item['permits'] = []



        '''     flood_zones      '''
        flood_zones_check = response.xpath("//*[contains(text(),'FEMA Flood Zone (Data provided by Sarasota County Government')]/text()").get('').strip()
        if flood_zones_check:
            FLOOD_ZONES_INFO = []
            flood_count = 1
            Flood_Zones = response.xpath("//span[contains(text(),'FEMA Flood Zone (Data provided by Sarasota County Government')]/following-sibling::table[1]/tbody")
            for values in Flood_Zones.css('tr'):
                Flood_Zones_values = dict()
                Flood_Zones_values['id'] = flood_count
                flood_count += 1
                Flood_Zones_values['real_estate_id'] = ''
                Flood_Zones_values['firm_panel'] = values.css('td:nth-child(1) ::text').get('').strip()
                Flood_Zones_values['floodway'] = values.css('td:nth-child(2) ::text').get('').strip()
                Flood_Zones_values['sfha'] = values.css('td:nth-child(3) ::text').get('').strip()
                Flood_Zones_values['zone'] = values.css('td:nth-child(4) ::text').get('').strip()
                Flood_Zones_values['community'] = values.css('td:nth-child(5) ::text').get('').strip()
                Flood_Zones_values['base_flood_elevation'] = values.css('td:nth-child(6) ::text').get('').strip()
                Flood_Zones_values['cfha'] = values.css('td:nth-child(7) ::text').get('').strip()
                FLOOD_ZONES_INFO.append(Flood_Zones_values)
            item['flood_zones'] = FLOOD_ZONES_INFO
        else:
            item['flood_zones'] = []

        yield item


    def save_to_csv(self, data):
        with open('Output/sarasota_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id','location_address','owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
