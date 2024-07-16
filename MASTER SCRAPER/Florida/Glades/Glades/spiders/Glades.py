###########         Glades
import re
import csv
import scrapy
from copy import deepcopy
from twocaptcha import TwoCaptcha
from urllib.parse import quote
class Glades(scrapy.Spider):
    name = 'Glades'
    prefix = 'https://qpublic.schneidercorp.com'
    url = "https://qpublic.schneidercorp.com/Application.aspx?AppID=818&LayerID=14562&PageTypeID=2&PageID=6420"

    cookies = {
        '_ga': 'GA1.1.1729783499.1705487862',
        '_ga_7ZQ1FTE1SG': 'deleted',
        'ASP.NET_SessionId': 'wd0q4pphmhnqng4sckxqprqa',
        'MODULES1109': '36095',
        'MODULESVISIBILE1109': '36041%7C36376',
        '_ga_7ZQ1FTE1SG': 'GS1.1.1712343552.9.1.1712343664.0.0.0',
        'cf_clearance': '6SBE7S1XeFXPUN1JG0_Y84K9nnCk9u7rltyYdmtDdqQ-1712343667-1.0.1.1-V_LCt5WjVVtoVsy1Hoc40V1ZzGB5kPWPOsxzKDYiHH.qMrLca6dD92XDK1jKWwWZS_MLyVbwGDv_lO6FOKIZUw',
        'arp_scroll_position': '100',
    }
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '_ga=GA1.1.1729783499.1705487862; _ga_7ZQ1FTE1SG=deleted; ASP.NET_SessionId=wd0q4pphmhnqng4sckxqprqa; MODULES1109=36095; MODULESVISIBILE1109=36041%7C36376; _ga_7ZQ1FTE1SG=GS1.1.1712343552.9.1.1712343664.0.0.0; cf_clearance=6SBE7S1XeFXPUN1JG0_Y84K9nnCk9u7rltyYdmtDdqQ-1712343667-1.0.1.1-V_LCt5WjVVtoVsy1Hoc40V1ZzGB5kPWPOsxzKDYiHH.qMrLca6dD92XDK1jKWwWZS_MLyVbwGDv_lO6FOKIZUw; arp_scroll_position=100',
        'origin': 'https://qpublic.schneidercorp.com',
        'referer': 'https://qpublic.schneidercorp.com/Application.aspx?AppID=818&LayerID=14562&PageTypeID=2&PageID=6420',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"123.0.6312.106"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="123.0.6312.106", "Not:A-Brand";v="8.0.0.0", "Chromium";v="123.0.6312.106"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }
    params = {
        'AppID': '818',
        'LayerID': '14562',
        'PageTypeID': '2',
        'PageID': '6420',
    }
    data = {    # __VIEWSTATE,   __VIEWSTATEGENERATOR   ,   ctlBodyPane$ctl01$ctl01$txtAddress
        '__EVENTTARGET': 'ctlBodyPane$ctl01$ctl01$btnSearch',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '569DB96F',
        'ctlBodyPane$ctl00$ctl01$txtName': '',
        'ctlBodyPane$ctl00$ctl01$txtNameExact': '',
        'ctlBodyPane$ctl01$ctl01$txtAddress': '7th Street',
        'ctlBodyPane$ctl01$ctl01$txtAddressExact': '',
        'ctlBodyPane$ctl02$ctl01$txtParcelID': '',
        'ctlBodyPane$ctl03$ctl01$txtName': '',
        'ctlBodyPane$ctl04$ctl01$ddlTownship': '',
        'ctlBodyPane$ctl04$ctl01$ddlSection': '',
        'ctlBodyPane$ctl05$ctl01$txtBook': '',
        'ctlBodyPane$ctl05$ctl01$txtPage': '',
    }

    TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key
    captcha_data = {    # __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        '__EVENTTARGET': 'btnSubmit',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '',
        '__EVENTVALIDATION': '',
        'g-recaptcha-response': '',
    }

    custom_settings = {
        'FEEDS': {
            'Output/glades_parcel_data.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        },  # Zyte API

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
        file_path = "input/glades_address_list.csv"
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
            yield scrapy.Request(
                url=self.url,                callback=self.parse,               # headers=self.headers,
                priority=priority,
                meta={'search_address': search_address, 'cookiejar':index, 'priority':priority},
                dont_filter=True,
            )

    def parse(self, response):  # __VIEWSTATE,   __VIEWSTATEGENERATOR   ,   ctlBodyPane$ctl01$ctl01$txtAddress
        payload = deepcopy(self.data)
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['ctlBodyPane$ctl01$ctl01$txtAddress'] = response.meta['search_address']
        priority = response.meta['priority']
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, #headers=self.headers,
                                 priority=priority, meta={'search_address':response.meta['search_address'],
                                'priority': priority,  'cookiejar': response.meta['cookiejar']})

    def parse_pages(self, response):
        if response.xpath("//*[contains(text(),'No results match your search criteria')]"):
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/glades_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            count = 0
            for row_div in response.css('#ctlBodyPane_ctl00_ctl01_gvwParcelResults>tbody>tr'):
                count += 1
                parcel_url = self.prefix + row_div.css('.normal-font-label ::attr(href)').get('').strip()

                '''         Handling 2-Captcha                     '''
                encoded_url = 'https://qpublic.schneidercorp.com/ValidateUser.aspx?url=' + quote(parcel_url, safe='')
                print(count, encoded_url)
                priority = response.meta['priority']
                yield scrapy.Request(url=encoded_url, callback=self.captcha_parse,  # headers=self.site_key_headers,
                    priority=priority, meta={'search_address': response.meta['search_address'],
                      'parcel_url': parcel_url, 'encoded_url': encoded_url,'cookiejar': response.meta['cookiejar']
                        , 'priority':priority})


    def captcha_parse(self, response):
        site_key = response.css('.g-recaptcha ::attr(data-sitekey)').get('').strip()
        parcel_url = response.meta.get('parcel_url')
        TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key
        solver = TwoCaptcha(self.TwoCaptcha_Key)

        result = solver.recaptcha(sitekey=site_key, url=parcel_url)
        desired_token = result['code']

        payload = deepcopy(self.captcha_data)
        # __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['__EVENTVALIDATION'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['g-recaptcha-response'] = desired_token
        encoded_url = response.meta.get('encoded_url')
        priority = response.meta['priority']
        yield scrapy.FormRequest(url=encoded_url, formdata=payload, method='POST', callback=self.parse_detail, # headers=self.headers,
                                 priority=priority, meta={'search_address': response.meta['search_address'],
                                'priority':priority,'cookiejar': response.meta['cookiejar']})


    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/parent::td[1]/following-sibling::td[1]/div/span[1]/text()").get('').strip()
        property_address = ' '.join(element.strip() for element in response.xpath("//*[contains(text(),'Location Address')]/parent::td[1]/following-sibling::td[1]/div/span[1]/text()").getall())  # description
        property_info['property_address'] = property_address.strip()
        zip_code_pattern = r'\b\d{5}\b'
        zip_code_match = re.search(zip_code_pattern, property_address)
        if zip_code_match:
            property_info['property_zipcode'] = zip_code_match.group()
        else:
            property_info['property_zipcode'] = ''

        property_info['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed')]/text()").get('').strip().replace('+','')
        property_info['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName2_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner3'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName3_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()

        mailing_add = response.css('#ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_lblOwnerAddress ::text').getall()
        mailing_address = ', '.join(element.strip() for element in mailing_add)  # description
        property_info['mailing_address_1'] = mailing_address.strip()
        property_info['mailing_address_2'] = ''

        if mailing_add:
            csz = mailing_add[-2].strip()
            pattern = r"^(.+?)\s+([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$"
            match = re.match(pattern, csz)
            if match:
                property_info['mailing_city'] = match.group(1)
                property_info['mailing_state'] = match.group(2)
                property_info['mailing_zipcode'] = match.group(3)
            else:
                property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = '','',''

        property_info['legal_description'] = response.xpath("//*[contains(text(),'Brief Tax Description')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Sec/Twp/Rng')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
        property_info['mileage'] = response.xpath("//*[contains(text(),'Millage Rate')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
        property_info['acreage'] = response.xpath("//*[contains(text(),'Acreage')]/parent::td/following-sibling::td[1]/div/span/text()").get('').strip()
        property_info['homestead_exemption'] = response.xpath("//*[contains(text(),'Homestead')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
        property_info['location_address'] = property_address.strip()
        property_info['property_use_code'] = response.xpath("//*[contains(text(),'Property Use Code')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
        property_info['taxing_district'] = response.xpath("//*[contains(text(),'Tax District')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
        property_info['search_address'] = response.meta.get('search_address')

        ''' Empty fields    '''
        property_info['subdivision'] = ''
        property_info['census'] = ''
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
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''
        item['main_info'] = property_info

        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/parent::td[1]/following-sibling::td[1]/div/span[1]/text()").get('').strip()
        listing['location_address'] = property_address.strip()
        listing['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed')]/text()").get('').strip().replace('+','')
        self.save_to_csv(listing)


        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_lblName')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = []
            for lands in response.css('#ctlBodyPane_ctl02_ctl01_grdLand_grdFlat>tbody>tr'):
                land_info = dict()
                land_info['land_use'] = lands.css('th ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(2) ::text').get('').strip()
                land_info['unit_type'] = lands.css('td:nth-child(3) ::text').get('').strip()
                land_info['frontage'] = lands.css('td:nth-child(4) ::text').get('').strip()
                land_info['depth'] = lands.css('td:nth-child(5) ::text').get('').strip()
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []


        '''     3 - buildings      '''
        building_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_lblName')]/text()").get('').strip()
        if building_info_check:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in response.css('#ctlBodyPane_ctl03_mSection .block-row'):
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = ''
                building_info['beds']  = buildings.xpath(".//*[contains(text(),'Bedrooms')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['baths']  = buildings.xpath(".//*[contains(text(),'Bathrooms')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['stories']  = buildings.xpath(".//*[contains(text(),'Stories')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['half_baths'] = ''
                building_info['built_year']  = buildings.xpath(".//*[contains(text(),'Actual Year Built')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['ac']  = buildings.xpath(".//*[contains(text(),'Air Conditioning')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                heat = buildings.xpath(".//*[contains(text(),'Heat')]/parent::td[1]/following-sibling::td[1]/div/span/text()").getall()
                if len(heat) > 1:
                    building_info['heat'] = heat[1].strip()
                else:
                    building_info['heat'] = ''
                building_info['floor_cover']  = buildings.xpath(".//*[contains(text(),'Floor Cover')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['frame_type']  = buildings.xpath(".//*[contains(text(),'Frame Type')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['interior_walls']  = buildings.xpath(".//*[contains(text(),'Interior Walls')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['roof_cover']  = buildings.xpath(".//*[contains(text(),'Roof Cover')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['exterior_walls']  = buildings.xpath(".//*[contains(text(),'Exterior Walls')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['gross_building_area'] = buildings.xpath(".//*[contains(text(),'Total Area')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                building_info['living_area'] = buildings.xpath(".//*[contains(text(),'Heated Area')]/parent::td[1]/following-sibling::td[1]/div/span/text()").get('').strip()
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []



        '''     4 - valuations      '''
        valuation_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl07_lblName')]/text()").get('').strip()
        if valuation_check:
            VALUATION = []
            valuation_count = 1
            for values in response.xpath("//*[contains(@id,'ctlBodyPane_ctl07_ctl01_grdValuation')]/thead/tr/th"):
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''
                certified_values['year'] = values.css("::text").get('').strip()
                certified_values['land'] = response.xpath(f"//*[contains(text(),'Land Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['building'] = response.xpath(f"//*[contains(text(),'Building Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['extra_feature'] = response.xpath(f"//*[contains(text(),'Extra Features Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['just'] = response.xpath(f"//*[contains(text(),'Just (Market) Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['assessed'] = response.xpath(f"//*[contains(text(),'Assessed Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['exemptions'] = response.xpath(f"//*[contains(text(),'Exempt Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['taxable'] = response.xpath(f"//*[contains(text(),'Taxable Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['cap'] = response.xpath(f"//*[contains(text(),'Maximum Save Our Homes Portability')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
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
        extra_features_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl04_lblName')]/text()").get('').strip()
        if extra_features_check:
            EXTRA_FEATURES_list = []
            x_features_count = 1
            for extra_features in response.css('#ctlBodyPane_ctl04_ctl01_grdSales_grdFlat>tbody>tr'):
                extra_f = dict()
                extra_f['id'] = x_features_count
                x_features_count += 1
                extra_f['real_estate_id'] = ''
                extra_f['building_number'] = ''
                extra_f['desc'] = extra_features.css('td:nth-child(2) ::text').get('').strip()
                extra_f['units'] = extra_features.css('td:nth-child(4) ::text').get('').strip()
                extra_f['unit_type'] = ''
                extra_f['year'] = extra_features.css('td:nth-child(5) ::text').get('').strip()
                EXTRA_FEATURES_list.append(extra_f)
            item['extra_features'] = EXTRA_FEATURES_list
        else:
            item['extra_features'] = []


        '''     6 - transactions      '''
        sales = response.xpath("//*[contains(@id,'ctlBodyPane_ctl05_lblName')]/text()").get('').strip()
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css('#ctlBodyPane_ctl05_ctl01_grdSales>tbody>tr'):
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('td:nth-child(2) ::text').get('').strip()
                sales_info['document_number'] = sales.css('td:nth-child(5) a::text').get('').strip()
                sales_info['qualification_code'] = sales.css('td:nth-child(6) ::text').get('').strip()
                sales_info['grantor'] = sales.css('td:nth-child(8) span::text').get('').strip()
                sales_info['grantee'] = sales.css('td:nth-child(9) span::text').get('').strip()
                sales_info['document_type'] = sales.css('td:nth-child(4) ::text').get('').strip()
                sales_info['price'] = sales.css('td:nth-child(3) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []


        '''     7 - permits       -      No data available on Website             '''
        item['permits'] = []

        '''     8 - flood_zones     -      No data available on Website             '''
        item['flood_zones'] = []


        # print(item)
        yield item

    def save_to_csv(self, data):
        with open('Output/glades_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass
