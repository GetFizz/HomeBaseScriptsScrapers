#########   Hendry

import re
import csv
import scrapy
from copy import deepcopy
from twocaptcha import TwoCaptcha
from urllib.parse import quote

class Hendry(scrapy.Spider):
    name = 'Hendry'
    prefix = 'https://beacon.schneidercorp.com'
    url = "https://beacon.schneidercorp.com/Application.aspx?AppID=1105&LayerID=27399&PageTypeID=2&PageID=11144"
    cookies = {
        '_ga': 'GA1.1.1729783499.1705487862',
        'ASP.NET_SessionId': 'qh542ueivl3vxojgvkdxzkoi',
        '_ga_7ZQ1FTE1SG': 'GS1.1.1710503852.51.1.1710504170.0.0.0',
        'cf_clearance': 'Sh6xxZDcF6ffyvlkm0KoPjGeBGJpyQS8Jsu_vc0N2Uo-1710504171-1.0.1.1-mn1HbsWy7vhyVrOfPXJMWnNtdnJa7yRHstZB03cmLEnmPW4UpBsjNEWsPK0rA9RMsbqgczuo_DudwrXUt1Twxg',
    }
    headers = {
        'authority': 'beacon.schneidercorp.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '_ga=GA1.1.1729783499.1705487862; ASP.NET_SessionId=qh542ueivl3vxojgvkdxzkoi; _ga_7ZQ1FTE1SG=GS1.1.1710503852.51.1.1710504170.0.0.0; cf_clearance=Sh6xxZDcF6ffyvlkm0KoPjGeBGJpyQS8Jsu_vc0N2Uo-1710504171-1.0.1.1-mn1HbsWy7vhyVrOfPXJMWnNtdnJa7yRHstZB03cmLEnmPW4UpBsjNEWsPK0rA9RMsbqgczuo_DudwrXUt1Twxg',
        'origin': 'https://beacon.schneidercorp.com',
        'referer': 'https://beacon.schneidercorp.com/Application.aspx?AppID=1105&LayerID=27399&PageTypeID=2&PageID=11144',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"122.0.6261.129"',
        'sec-ch-ua-full-version-list': '"Chromium";v="122.0.6261.129", "Not(A:Brand";v="24.0.0.0", "Google Chrome";v="122.0.6261.129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }
    data = {    #   __EVENTTARGET,      __VIEWSTATE,        __VIEWSTATEGENERATOR,       ctlBodyPane$ctl01$ctl01$txtAddressExact
        '__EVENTTARGET': 'ctlBodyPane$ctl01$ctl01$btnSearchExact',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': 'Hw6LHlQljWO+4QFuusmYKttf7zxt4eD5UwsUv9+liDtdYl/aVUS+kN0lPcQU8pZ//EXIuM7tqP+/Da3NK+TSuYWZnlY1LGn/DAlfbEBW+Uz6gfTPWPgTBrRyAFhc+qbJJA2M6MR4cK2EBZhHY/LQeKWCHMNVV7AxV550AuPk69CUtfdIlzZZTrs2B8umq+IYTLrHBRz9REQEt+GTp7vEdERVoEmPscnYMIL11ow8H98IjA2g',
        '__VIEWSTATEGENERATOR': '569DB96F',
        'ctlBodyPane$ctl00$ctl01$txtName': '',
        'ctlBodyPane$ctl00$ctl01$txtNameExact': '',
        'ctlBodyPane$ctl01$ctl01$txtAddress': '',
        'ctlBodyPane$ctl01$ctl01$txtAddressExact': '10th st',
        'ctlBodyPane$ctl02$ctl01$txtParcelID': '',
        'ctlBodyPane$ctl03$ctl01$txtAlternateID': '',
        'ctlBodyPane$ctl03$ctl01$txtAlternateIDExactMatch': '',
        'ctlBodyPane$ctl04$ctl01$txtName': '',
        'ctlBodyPane$ctl05$ctl01$txtSecTwpRng': '',
        'ctlBodyPane$ctl06$ctl01$srch1$txtInput': '',
        'ctlBodyPane$ctl07$ctl01$ddlNeighborhood': '',
        'ctlBodyPane$ctl08$ctl01$ddlSubdivision': '',
    }

    TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key
    captcha_data = {    # __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        '__EVENTTARGET': 'btnSubmit',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': 'EGVOfTudZpm+/pCX7LSlKFztkpE3fJUd0ZWPoHsRHBHoKgCcpGxGlxQg7Z4c9Ob7ierhx6owFjxXetKchOGmvZZPi3A=',
        '__VIEWSTATEGENERATOR': 'A1A9A2FD',
        '__EVENTVALIDATION': '16Poi9monyASTxCBFEuDy+uoI6yGviq8hE4ctkC+/SvryUoBlxWeuYPELcnYM/Pn/UBLPIfkWrZSNlColjhdr8rhCsiTrFWX6Lt21UmAqD5Wm1D0',
        'g-recaptcha-response': '03AFcWeA5uMaU23HpMl9o1CIrJzpDTXgsfZBP4bNJ_DSsq7OWhOHQpZmEYSGYQj8-FWNDMrzozLMOfC1FRDjd6wGaAEgAns7LkTyCOUK3KP83czrkRTZDGot94KL7b-2MMqV-9NR5xgztyXwWht0r6bEY-0OOv657_lf-OrR9gixw5x48zhOTcxKu1E3TWEujYQAHgqCaQ9ir6jFodIuMipF1UvsMzU2fEYxoIUKDFuC9_Hp4ZQFAq1WtlXa916pG35rXTtnTIm28vVWFCJ9CHUIlKEUOY6_JeryDrIEqNb21mgCp3HjWN6f0GiT5ZM5QMa4-R21fDwfsPsFazYsjZJrkDB0hT5ldzVkNzU0F-mmF85wyPNPw0sND8hb3A1p_F9VFXYHbCsyb4sz49VDd-BmNTZZsm2qoFD4srr-BcJKaOk4oPsWxIDkvbRe9L2dEypd5MNaBAA6WK3wcte1zRLyiNODwkjBd2PV1anm65mUYdxTWnepfhP1vpwoM6DYyyoV73hMXjYs91HQM7-Fm8H97IC-LCHEVUMo_wWmxqPDfvu3SmdGRlO22rvnaZ5qaa4r6wBUrR4BOIqDcfiJmkCQ69hBSpm07KPDjsjZLL3dZ4qbrQ0J_-BmTKza34Zva-0KYfLHK97w_2U0gT1eWTMtn4kADZQM6BVs_VG4wVa704gHh29k7jeRA',
    }

    custom_settings = {
        'FEEDS': {
            'Output/hendry_parcel_data.json': {
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
        file_path = "input/hendry_address_list.csv"
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
                url='https://beacon.schneidercorp.com/Application.aspx?AppID=1105&LayerID=27399&PageTypeID=2&PageID=11144',
                callback=self.parse,                headers=self.headers, priority=priority,
                meta={'search_address': search_address, 'cookiejar':index,'priority':priority}, dont_filter=True,
            )

    def parse(self, response):
        payload = deepcopy(self.data)
        # payload['__EVENTTARGET'] = response.css('#__EVENTTARGET::attr(value)').get()
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['ctlBodyPane$ctl01$ctl01$txtAddressExact'] = response.meta.get('search_address')

        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, headers=self.headers,
                                 priority=response.meta['priority'], meta={'search_address':response.meta['search_address'],
                      'cookiejar': response.meta['cookiejar'],'priority':response.meta['priority']})

    def parse_pages(self, response):
        if response.xpath("//*[contains(text(),'No results match your search criteria')]"):
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/hendry_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            count = 0
            for row_div in response.css('#ctlBodyPane_ctl00_ctl01_gvwParcelResults>tbody>tr'):
                count += 1
                parcel_url = self.prefix + row_div.css('.normal-font-label ::attr(href)').get('').strip()
                encoded_url = 'https://beacon.schneidercorp.com/ValidateUser.aspx?url=' + quote(parcel_url, safe='')
                print(count, encoded_url)
                yield scrapy.Request(url=encoded_url, callback=self.captcha_parse,  # headers=self.site_key_headers,
                            priority=response.meta['priority'], meta={'search_address': response.meta['search_address'],
                            'parcel_url': parcel_url, 'encoded_url': encoded_url,
                            'cookiejar': response.meta['cookiejar'],'priority':response.meta['priority']})

    def captcha_parse(self, response):
        site_key = response.css('.g-recaptcha ::attr(data-sitekey)').get('').strip()
        parcel_url = response.meta.get('parcel_url')
        TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key
        solver = TwoCaptcha(self.TwoCaptcha_Key)

        result = solver.recaptcha(sitekey=site_key, url=parcel_url)
        desired_token = result['code']

        payload = deepcopy(self.captcha_data)
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['__EVENTVALIDATION'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['g-recaptcha-response'] = desired_token
        encoded_url = response.meta.get('encoded_url')
        yield scrapy.FormRequest(url=encoded_url, formdata=payload, method='POST', callback=self.parse_detail, # headers=self.headers,
                    priority=response.meta['priority'], meta = {'search_address': response.meta['search_address'],
                    'cookiejar': response.meta['cookiejar'],'priority':response.meta['priority']})


    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::td/span/text()").get('').strip()

        location_add = response.xpath("//*[contains(text(),'Location Address')]/following-sibling::td/span/text()").getall()
        location_address = ', '.join(element.strip() for element in location_add)
        property_info['property_address'] = location_address

        zip_code_pattern = r'\b\d{5}\b'
        zip_code_match = re.search(zip_code_pattern, location_address)
        if zip_code_match:
            property_info['property_zipcode'] =  zip_code_match.group()
        else:
            property_info['property_zipcode'] = ''

        property_info['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName2_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner3'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName2_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        mailing_add = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_rptOwner_ctl00_lblOwnerAddress')]/text()").getall()
        mailing_address = ', '.join(element.strip() for element in mailing_add)
        property_info['mailing_address_1'] = mailing_address
        property_info['mailing_address_2'] = ''
        pattern = re.compile(r'\b([^,]+),\s*([A-Za-z]{2})\s*(\d{5})\b')
        matches = pattern.findall(mailing_address)
        if matches:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = matches[0]

        property_info['property_id'] = response.xpath("//*[contains(text(),'Prop ID')]/following-sibling::td/span/text()").get('').strip()
        property_info['neighborhood'] = response.xpath("//*[contains(text(),'Neighborhood/Area')]/following-sibling::td/span/text()").get('').strip()
        property_info['subdivision'] = response.xpath("//*[contains(text(),'Subdivision')]/following-sibling::td/span/text()").get('').strip()
        property_info['legal_description'] = response.xpath("//*[contains(text(),'Brief Legal Description')]/following-sibling::td/span/text()").get('').strip()
        property_info['property_use_code'] = response.xpath("//*[contains(text(),'Property Use Code')]/following-sibling::td/span/text()").get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Sec/Twp/Rng')]/following-sibling::td/span/text()").get('').strip()
        property_info['taxing_district'] = response.xpath("//*[contains(text(),'Tax District')]/following-sibling::td/span/text()").get('').strip()
        property_info['mileage'] = response.xpath("//*[contains(text(),'Millage Rate')]/following-sibling::td/span/text()").get('').strip()
        property_info['acreage'] = response.xpath("//*[contains(text(),'Acreage')]/following-sibling::td/span/text()").get('').strip()
        property_info['homestead_exemption'] = response.xpath("//*[contains(text(),'Homestead')]/parent::td/following-sibling::td/span/text()").get('').strip()
        property_info['location_address'] = location_address
        property_info['search_address'] = response.meta.get('search_address')

        property_info['census'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
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
        listing['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::td/span/text()").get('').strip()
        listing['location_address'] = property_info['location_address']
        listing['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        self.save_to_csv(listing)

        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(text(),'Land Information')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = []
            for lands in response.css('#ctlBodyPane_ctl07_ctl01_gvwLand>tbody>tr'):
                land_info = dict()
                land_info['land_use'] = lands.css('td:nth-child(1) ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(4) ::text').get('').strip()
                land_info['unit_type'] = response.css('#ctlBodyPane_ctl07_ctl01_gvwLand>thead>tr>th:nth-child(4) ::text').get('').strip()
                land_info['frontage'] = lands.css('td:nth-child(5) ::text').get('').strip()
                land_info['depth'] = lands.css('td:nth-child(6) ::text').get('').strip()
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []


        '''     3 - buildings      '''
        building_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl08_lblName')]/text()").get('').strip()
        if building_info_check:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in response.css('#ctlBodyPane_ctl08_mSection .block-row'):
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = ''
                building_info['beds']  = buildings.xpath(".//*[contains(text(),'Bedrooms')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['baths']  = buildings.xpath(".//*[contains(text(),'Bathrooms')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['stories']  = buildings.xpath(".//*[contains(text(),'Stories')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['half_baths'] = ''
                building_info['built_year']  = buildings.xpath(".//*[contains(text(),'Actual Year Built')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['ac']  = buildings.xpath(".//*[contains(text(),'Air Conditioning')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['heat'] = buildings.xpath(".//*[contains(text(),'Heat Index')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['floor_cover']  = buildings.xpath(".//*[contains(text(),'Floor Cover')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['frame_type'] = ''
                building_info['interior_walls']  = buildings.xpath(".//*[contains(text(),'Interior Walls')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['roof_cover']  = buildings.xpath(".//*[contains(text(),'Roof Cover')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['exterior_walls']  = buildings.xpath(".//*[contains(text(),'Exterior Walls')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['gross_building_area'] = ''
                building_info['living_area'] = ''
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []

        '''     4 - valuations      '''
        valuation_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_lblName')]/text()").get('').strip()
        if valuation_check:
            VALUATION = []
            valuation_count = 1
            for values in response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_ctl01_grdValuation')]/thead/tr/th"):
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''
                certified_values['year'] = values.css("::text").get('').strip()
                certified_values['building'] = ''
                certified_values['land'] = response.xpath(f"//*[contains(text(),'Land Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['extra_feature'] = ''
                certified_values['just'] = response.xpath(f"//*[contains(text(),'Just Market Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['assessed'] = response.xpath(f"//*[contains(text(),'Assessed Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['exemptions'] = response.xpath(f"//*[contains(text(),'Exempt Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['taxable'] = response.xpath(f"//*[contains(text(),'Taxable Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
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


        '''     5 - extra_features   -      No data available on Website     '''
        item['extra_features'] = []

        '''     6 - transactions      '''
        sales = response.xpath("//*[contains(@id,'ctlBodyPane_ctl11_lblName')]/text()").get('').strip()
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css('#ctlBodyPane_ctl11_ctl01_grdSales>tbody>tr'):
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('th ::text').get('').strip()
                sales_info['document_number'] = sales.css('td:nth-child(4) a::text').get('').strip()
                sales_info['qualification_code'] = sales.css('td:nth-child(5) ::text').get('').strip()
                sales_info['grantor'] = sales.css('td:nth-child(7) span::text').get('').strip()
                sales_info['grantee'] = sales.css('td:nth-child(8) span::text').get('').strip()
                sales_info['document_type'] = sales.css('td:nth-child(3) ::text').get('').strip()
                sales_info['price'] = sales.css('td:nth-child(2) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []


        '''     7 - permits         -      No data available on Website             '''
        permits = response.xpath("//*[contains(@id,'ctlBodyPane_ctl13_lblName')]/text()").get('').strip()
        if permits:
            PERMITS_LIST = []
            for permit in response.css('#ctlBodyPane_ctl13_ctl01_grdPermits>tbody>tr'):
                permit_info = dict()
                permit_info['application'] = permit.css('th ::text').get('').strip()
                permit_info['property_type'] = permit.css('td:nth-child(2) ::text').get('').strip()
                permit_info['property_owner'] = ''
                permit_info['application_date'] = ''
                permit_info['valuation'] = permit.css('td:nth-child(6) ::text').get('').strip()
                permit_info['parcel_id'] = ''
                permit_info['subcontractor'] = ''
                permit_info['contractor'] = ''
                permit_info['permit_type'] = ''
                permit_info['issue_date'] = permit.css('td:nth-child(5) ::text').get('').strip()
                PERMITS_LIST.append(permit_info)
            item['permits'] = PERMITS_LIST
        else:
            item['permits'] = []


        '''     8 - flood_zones     -      No data available on Website             '''
        flood_zones = response.xpath("//*[contains(text(),'flood zones')]/text()").get('').strip()
        if flood_zones:
            item['flood_zones'] = []
        else:
            item['flood_zones'] = []

        # print(item)
        yield item

    def save_to_csv(self, data):
        with open('Output/hendry_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass
