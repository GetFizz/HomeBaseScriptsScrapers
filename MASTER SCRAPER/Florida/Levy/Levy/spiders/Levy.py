#####       Levy

import re
import csv
import scrapy
from copy import deepcopy
from scrapy.utils.response import open_in_browser
from twocaptcha import TwoCaptcha
from urllib.parse import quote

class Levy(scrapy.Spider):
    name = 'Levy'
    prefix = 'https://qpublic.schneidercorp.com'
    url = "https://qpublic.schneidercorp.com/Application.aspx?AppID=930&LayerID=18185&PageTypeID=2&PageID=8125"
    cookies = {
        '_ga': 'GA1.1.1729783499.1705487862',
        'MODULES828': '',
        'MODULESVISIBILE828': '43481%7C43486',
        'ASP.NET_SessionId': 'ykh3jukt4n0iwtk44efxe30k',
        '_ga_7ZQ1FTE1SG': 'GS1.1.1710696893.60.1.1710696907.0.0.0',
        'cf_clearance': 'X5GeNzKpmkV4FCB5mG4Nwhm5jw33rtZswmlQD.qOiSI-1710696910-1.0.1.1-.Fv1AAj7WpE5QAh2gd20ZtIRlPtqYVDfeUEE6LjBZIFNhP2T1cfTo57tDYft27cdSv1ZbR.HOhMVjo.8qEgyYg',
    }
    headers = {
        'authority': 'qpublic.schneidercorp.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '_ga=GA1.1.1729783499.1705487862; MODULES828=; MODULESVISIBILE828=43481%7C43486; ASP.NET_SessionId=ykh3jukt4n0iwtk44efxe30k; _ga_7ZQ1FTE1SG=GS1.1.1710696893.60.1.1710696907.0.0.0; cf_clearance=X5GeNzKpmkV4FCB5mG4Nwhm5jw33rtZswmlQD.qOiSI-1710696910-1.0.1.1-.Fv1AAj7WpE5QAh2gd20ZtIRlPtqYVDfeUEE6LjBZIFNhP2T1cfTo57tDYft27cdSv1ZbR.HOhMVjo.8qEgyYg',
        'origin': 'https://qpublic.schneidercorp.com',
        'referer': 'https://qpublic.schneidercorp.com/Application.aspx?AppID=930&LayerID=18185&PageTypeID=2&PageID=8125',
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
    data = { #   __VIEWSTATE,    __EVENTARGUMENT,    __VIEWSTATEGENERATOR,   ctlBodyPane$ctl01$ctl01$txtAddress
        '__EVENTTARGET': 'ctlBodyPane$ctl01$ctl01$btnSearch',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': 'py266xE6kpFLDAWBEW82O56unU29FQ67UhyBWCrYHxpKlM4yOwfbb5bc/VCAFm2etxePPKWdsM52id4XKSdlJa2CfWPz4lwN38pQy2yfDyYM6qAa5+lfGcgjdHFTkkZqWKU+WCfnsFiS6JgfSC9cZRytqGU8IeVtRVYFnXe436jLHtAThjEEvRCs+rmO5m7BsZl+TkypAOmluuxPkBHU/f0jqRTfVIjK5aUGK13umEKem+ud',
        '__VIEWSTATEGENERATOR': '569DB96F',
        'ctlBodyPane$ctl00$ctl01$txtName': '',
        'ctlBodyPane$ctl00$ctl01$txtNameExact': '',
        'ctlBodyPane$ctl01$ctl01$txtAddress': '110th Avenue Northeast',
        'ctlBodyPane$ctl01$ctl01$txtAddressExact': '',
        'ctlBodyPane$ctl01$ctl01$txtStreetNumber': '',
        'ctlBodyPane$ctl01$ctl01$txtStreetName': '',
        'ctlBodyPane$ctl01$ctl01$txtUnitNumber': '',
        'ctlBodyPane$ctl02$ctl01$txtParcelID': '',
        'ctlBodyPane$ctl03$ctl01$txtName': '',
    }

    captcha_data = {    # __EVENTTARGET, __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        '__EVENTTARGET': 'btnSubmit',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': 'TxhxGHRm9bgd3osUYNUyniIyiTOQZE6AdJtySc8tRzQetrBqjtOiiVhPm3oMxDCpHwT1OIkfZpKljZA+zCbK33YJEbk=',
        '__VIEWSTATEGENERATOR': 'A1A9A2FD',
        '__EVENTVALIDATION': 'KtQBjw4I1yuUOzBM3Yb6gziM+r9euHvIqtbM/AVjSb9H4Kys/de3mSifr+94wC1qyNBigUAWdHBDU24kkFvlX3cGy+Hksd/LxAgMxxh1TMU1l7JS',
        'g-recaptcha-response': '03AFcWeA69xdYq8DLRmKNh6ZgGenyjh9_A-pMid72x9gnfmlgLC92bc7CxiOCRboqqZxWx4aN5dcIXFMe3GNs8so3N3gavI38JhodFLq8w-B7hnrL6IdSfkBOVIkPVKgBcgA18qq0NxFhs21RFVj-u9GH958Ch5MdZbnn2lvVDFaCwW5FpLI2TKrHsjjIKtMG4G-Pw43GAmzUE5utBybqpcNJTTKraAFaa0cmbCNB3TzjLUANZBrLyKtqTR4CRjL9wL3ZrfcpWSFXJ5TD0ADjLuWAHr6HKLW60S8oyBc6dmRe6JYZzmXZVe12XORqDIq0PEnbTIUNi5rVqrxrzgN148Qy2yHwODPd63eGMlGJg6zbSLLFvGhcd7Yqe8uS7aZKtmInNfozsQcp_rOmCbHMb68dfDw0bjZ8WtjbqPPXTafRRdoV_AQ-fjCBfbb2pA0l9xOpuYwrNwVMLCVQmAkXjk90lG80Bs-dsS0Id1AHw2MS5f7RLhnSbY8fnLmv5bhgXshcGvoJwFfc9jbEr8m86s-qtiFfYq7JOCNSJa6L4pqY2Xz718Svjhg0yEx8GbFg3dv6lUZFd1VbRpZzOn3zgYQBRdyV73Sbp8PBJXYeFEOxJ0SX1EvW8j1fBqwM2XgzDyvpVLsma0buAsG4VqtPmSBxcwLLfWHO_Z-_g-2SFCYWElDY7_r1qrxvvms1AKYDn8jhYp5mset38',
    }

    custom_settings = {
        'FEEDS': {
            'Output/levy_parcel_data.json': {
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

    TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key


    def start_requests(self):
        file_path = "input/levy_address_list.csv"
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
                url='https://qpublic.schneidercorp.com/Application.aspx?AppID=930&LayerID=18185&PageTypeID=2&PageID=8125',
                callback=self.parse,                #headers=self.headers,
                meta={'search_address': address, 'cookiejar': index},                dont_filter=True,
            )

    def parse(self, response):
        # open_in_browser(response)
        payload = deepcopy(self.data)
        # payload['__EVENTARGUMENT'] = response.css('#__EVENTARGUMENT::attr(value)').get()
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['ctlBodyPane$ctl01$ctl01$txtAddress'] = response.meta.get('search_address')
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, #headers=self.headers,
                                 meta={'search_address':response.meta.get('search_address'), 'cookiejar': response.meta['cookiejar']})

    def parse_pages(self, response):

        if response.xpath("//*[contains(text(),'No results match your search criteria')]"):
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/levy_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            for row_div in response.css('#ctlBodyPane_ctl00_ctl01_gvwParcelResults>tbody>tr'):
                parcel_url = self.prefix + row_div.css('.normal-font-label ::attr(href)').get('').strip()
                encoded_url = 'https://qpublic.schneidercorp.com/ValidateUser.aspx?url=' + quote(parcel_url, safe='')
                yield scrapy.Request(url=encoded_url, callback=self.captcha_parse,  # headers=self.site_key_headers,
                                     meta={'search_address': response.meta.get('search_address'),
                                           'parcel_url': parcel_url, 'encoded_url': encoded_url, 'cookiejar': response.meta['cookiejar']})

    def captcha_parse(self, response):
        site_key = response.css('.g-recaptcha ::attr(data-sitekey)').get('').strip()
        parcel_url = response.meta.get('parcel_url')
        TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key
        solver = TwoCaptcha(self.TwoCaptcha_Key)

        result = solver.recaptcha(sitekey=site_key, url=parcel_url)
        desired_token = result['code']

        payload = deepcopy(self.captcha_data)
        # payload['__EVENTTARGET'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['__EVENTVALIDATION'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['g-recaptcha-response'] = desired_token
        encoded_url = response.meta.get('encoded_url')
        yield scrapy.FormRequest(url=encoded_url, formdata=payload, method='POST', callback=self.parse_detail,
                                 # headers=self.headers,
                                 meta={'search_address': response.meta.get('search_address'), 'cookiejar': response.meta['cookiejar']})

    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::td/span/text()").get('').strip()

        # property_info['location_address'] = ', '.join(element.strip() for element in location_add)
        property_add = response.xpath("//*[contains(text(),'Location Address')]/following-sibling::td/span/text()").getall()
        property_address = ', '.join(element.strip() for element in property_add)
        property_info['property_address'] = property_address

        zip_code_pattern = r'\b\d{5}(?:-\d{4})?\b'  # Regular expression pattern for zip code
        zip_codes = re.findall(zip_code_pattern, property_address)
        if zip_codes:
            property_info['property_zipcode'] = zip_codes[-1]
        else:
            property_info['property_zipcode'] = ''

        property_info['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_ctl01_lstPrimaryOwner_ctl00_lblPrimaryOwnerName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_ctl01_lstPrimaryOwner_ctl02_lblPrimaryOwnerName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner3'] = ''

        mailing_add = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_ctl01_lblOwnerAddress_lblSuppressed')]/text()").getall()
        mailing_address = ', '.join(element.strip() for element in mailing_add)
        property_info['mailing_address_1'] = mailing_address
        property_info['mailing_address_2'] = ''
        pattern = re.compile(r'\b([^,]+),\s*([A-Za-z]{2})\s*(\d{5})\b')
        matches = pattern.findall(mailing_address)
        if matches:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = matches[0]
        else:
            property_info['mailing_city'] = ''
            property_info['mailing_state'] = ''
            property_info['mailing_zipcode'] = ''

        property_info['neighborhood'] = response.css('#ctlBodyPane_ctl02_ctl01_lblNeigborhood ::text').get('').strip()
        property_info['legal_description'] = response.xpath("//*[contains(text(),'Legal Description')]/parent::td/following-sibling::td/span/text()").get('').strip()
        property_info['property_use_code'] = response.css('#ctlBodyPane_ctl02_ctl01_lblUsage ::text').get('').strip()
        property_info['subdivision'] = response.css('#ctlBodyPane_ctl02_ctl01_lblSubdivision ::text').get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Sec/Twp/Rng')]/following-sibling::td/span/text()").get('').strip()
        property_info['taxing_district'] = response.css('#ctlBodyPane_ctl02_ctl01_lblDistrict ::text').get('').strip()
        property_info['mileage'] = response.xpath("//*[contains(text(),'Millage Rate')]/following-sibling::td/span/text()").get('').strip()
        property_info['acreage'] = response.xpath("//*[contains(text(),'Acreage')]/following-sibling::td/span/text()").get('').strip()
        property_info['homestead_exemption'] = response.xpath("//*[contains(text(),'Homestead')]/following-sibling::td/span/text()").get('').strip()
        property_info['location_address'] = property_address

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
        property_info['property_id'] = ''
        property_info['subdivision_code'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info

        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::td/span/text()").get('').strip()
        listing['location_address'] = property_address
        listing['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_ctl01_lstPrimaryOwner_ctl00_lblPrimaryOwnerName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        self.save_to_csv(listing)

        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl10_lblName')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = []
            for lands in response.css('#ctlBodyPane_ctl10_ctl01_gvwLandLine>tbody>tr'):
                land_info = dict()
                land_info['land_use'] = lands.css('th ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(4) ::text').get('').strip()
                land_info['unit_type'] = lands.css('td:nth-child(5) ::text').get('').strip()
                land_info['frontage'] = lands.css('td:nth-child(2) ::text').get('').strip()
                land_info['depth'] = lands.css('td:nth-child(3) ::text').get('').strip()
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
                building_info['building_no'] = buildings.xpath(".//*[contains(text(),'Building')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['beds'] = ''
                building_info['baths'] = buildings.xpath(".//*[contains(text(),'Baths')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['stories'] = ''
                building_info['half_baths'] = ''
                building_info['built_year'] = buildings.xpath(".//*[contains(text(),'Actual Year Built')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['ac'] = buildings.xpath(".//*[contains(text(),'Air Conditioning')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['heat'] = buildings.xpath(".//*[contains(text(),'Heating Type')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['floor_cover'] = ''
                building_info['frame_type'] = ''
                building_info['interior_walls'] = ''
                building_info['roof_cover'] = buildings.xpath(".//*[contains(text(),'Roof Cover')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['exterior_walls'] = buildings.xpath(".//*[contains(text(),'Exterior Wall')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['gross_building_area'] = buildings.xpath(".//*[contains(text(),'Actual Area')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['living_area'] = buildings.xpath(".//*[contains(text(),'Conditioned Area')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []

        '''     4 - valuations      '''
        valuation_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl06_lblName')]/text()").get('').strip()
        if valuation_check:
            VALUATION = []
            valuation_count = 1
            for values in response.xpath("//*[contains(@id,'ctlBodyPane_ctl06_ctl01_grdValuation')]/thead/tr/th"):
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''

                certified_values['year'] = values.css("::text").get('').strip()

                certified_values['land'] = response.xpath(f"//*[contains(text(),'Market Land Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['building'] = response.xpath(f"//*[contains(text(),'Building Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['extra_feature'] = response.xpath(f"//*[contains(text(),'Extra Features Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['just'] = response.xpath(f"//*[contains(text(),'Just (Market) Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['assessed'] = response.xpath(f"//*[contains(text(),'Assessed Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['exemptions'] = response.xpath(f"//*[contains(text(),'Exempt Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['taxable'] = response.xpath(f"//*[contains(text(),'Taxable Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['cap'] = response.xpath(f"//*[contains(text(),'Cap Differential')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
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
        extra_features_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl09_lblName')]/text()").get('').strip()
        if extra_features_check:
            EXTRA_FEATURES_list = []
            x_features_count = 1
            for extra_features in response.css('#ctlBodyPane_ctl09_ctl01_gvwExtraFeatures>tbody>tr'):
                extra_f = dict()
                extra_f['id'] = x_features_count
                x_features_count += 1
                extra_f['real_estate_id'] = ''
                extra_f['building_number'] = extra_features.css('td:nth-child(2) ::text').get('').strip()
                extra_f['desc'] = extra_features.css('th ::text').get('').strip()
                extra_f['units'] = extra_features.css('td:nth-child(6) ::text').get('').strip()
                extra_f['unit_type'] = ''
                extra_f['year'] = ''
                EXTRA_FEATURES_list.append(extra_f)
            item['extra_features'] = EXTRA_FEATURES_list
        else:
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
                sales_info['transfer_date'] = sales.css('td:nth-child(1) ::text').get('').strip()
                book = sales.css("td:nth-child(4) span a::text").get('').strip()
                page = sales.css("td:nth-child(5) span a::text").get('').strip()
                sales_info['document_number'] = book + '/' + page
                sales_info['qualification_code'] = sales.css('td:nth-child(6) ::text').get('').strip()
                sales_info['grantor'] = sales.css('td:nth-child(8) span::text').get('').strip()
                sales_info['grantee'] = sales.css('td:nth-child(9) span::text').get('').strip()
                sales_info['document_type'] = sales.css('td:nth-child(3) ::text').get('').strip()
                sales_info['price'] = sales.css('td:nth-child(2) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []

        '''     7 - permits         -      No data available on Website             '''
        permits = response.xpath("//*[contains(text(),'permits')]/text()").get('').strip()
        if permits:
            item['permits'] = []
        else:
            item['permits'] = []

        '''     8 - flood_zones     -      No data available on Website             '''
        flood_zones = response.xpath("//*[contains(text(),'flood zones')]/text()").get('').strip()
        if flood_zones:
            item['flood_zones'] = []
        else:
            item['flood_zones'] = []

        yield item

    def save_to_csv(self, data):
        with open('Output/levy_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass

