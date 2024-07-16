#######         Monroe

import re
import csv
import scrapy
from copy import deepcopy
from scrapy.utils.response import open_in_browser
from twocaptcha import TwoCaptcha
from urllib.parse import quote

class Monroe(scrapy.Spider):
    name = 'Monroe'
    prefix = 'https://qpublic.schneidercorp.com'
    url = "https://qpublic.schneidercorp.com/Application.aspx?AppID=605&LayerID=9946&PageTypeID=2&PageID=4381"
    cookies = {
        '_ga': 'GA1.1.1729783499.1705487862',
        'MODULES828': '',
        'MODULESVISIBILE828': '43481%7C43486',
        'ASP.NET_SessionId': 'ykh3jukt4n0iwtk44efxe30k',
        '_ga_7ZQ1FTE1SG': 'GS1.1.1710707816.62.1.1710712220.0.0.0',
        'cf_clearance': 'mic7_1JyYZO5IcJmiGtWi_aLl9LbvxZCqrtoayABXr8-1710712225-1.0.1.1-bEDLIR8.JD9hJAJxMpEVH1Q0GZD62D2kVFlvgy4yi.vbFkzCZp0LkKj02II9pEUjOc0gDM4QRJhWhedj5BtGRw',
    }
    headers = {
        'authority': 'qpublic.schneidercorp.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '_ga=GA1.1.1729783499.1705487862; MODULES828=; MODULESVISIBILE828=43481%7C43486; ASP.NET_SessionId=ykh3jukt4n0iwtk44efxe30k; _ga_7ZQ1FTE1SG=GS1.1.1710707816.62.1.1710712220.0.0.0; cf_clearance=mic7_1JyYZO5IcJmiGtWi_aLl9LbvxZCqrtoayABXr8-1710712225-1.0.1.1-bEDLIR8.JD9hJAJxMpEVH1Q0GZD62D2kVFlvgy4yi.vbFkzCZp0LkKj02II9pEUjOc0gDM4QRJhWhedj5BtGRw',
        'origin': 'https://qpublic.schneidercorp.com',
        'referer': 'https://qpublic.schneidercorp.com/Application.aspx?AppID=605&LayerID=9946&PageTypeID=2&PageID=4381',
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
    data = {    # __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTARGUMENT,   ctlBodyPane$ctl01$ctl01$txtAddress
        '__EVENTTARGET': 'ctlBodyPane$ctl01$ctl01$btnSearch',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '97fDYjOlGPkWxNWuKyyyRdYOjhwQmMWyTs+4L2vuj9zGxgVyhj+sPUV/39v59i3NSZj7+NQ8LEzyICmLKJo4eBAGdsv+brle6hQI/9Hrv8c51mIqpP35Sz8pq9H6gC4I5YKDEaVJ+FbnvpTMnQEe3WvYkVvgHcJJHvY53YHKiFpiAuYQp4bX9XbUI4PG/uJjpRIIGcYe2ZOQfrZUVUuCwn4LQXXRT/ol9uxjY8hPZHcV2xKJ',
        '__VIEWSTATEGENERATOR': '569DB96F',
        'ctlBodyPane$ctl00$ctl01$txtName': '',
        'ctlBodyPane$ctl00$ctl01$txtNameExact': '',
        'ctlBodyPane$ctl01$ctl01$txtAddress': '22nd Street',
        'ctlBodyPane$ctl01$ctl01$txtAddressExact': '',
        'ctlBodyPane$ctl02$ctl01$txtParcelID': '',
        'ctlBodyPane$ctl03$ctl01$txtAlternateID': '',
        'ctlBodyPane$ctl03$ctl01$txtAlternateIDExactMatch': '',
        'ctlBodyPane$ctl04$ctl01$srch1$txtInput': '',
        'ctlBodyPane$ctl05$ctl01$srch1$txtInput': '',
        'ctlBodyPane$ctl06$ctl01$txtCondoName$txtInput': '',
    }

    captcha_data = {    # __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        '__EVENTTARGET': 'btnSubmit',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '8lLUpdG0TIdLiGH9reB9F8UfC274ISgkGTNv57+V8NHMsak3D0T+f78TzFgwvT2fLzCGLC7NUzCiaFkiVCzIuG8zhZQ=',
        '__VIEWSTATEGENERATOR': 'A1A9A2FD',
        '__EVENTVALIDATION': 'CgOuk//vTIyNDSvSSyD7atqZDwjesxIo3/vrFpAi3YktaA8R3uBodwH6p/asmajysLgb0Bym6mdbDEf8qOvztACFzzBzM/OJdCN4qCGu3nKuqd9v',
        'g-recaptcha-response': '03AFcWeA4AFFOubtKS52zfXfHY3DNFhFTIUB6-aNewwNgnu1W-eT8eV1aiCFloefJfmzOwBdxcdjT96kAQvqHJX3gm1f4-KVC0bhBXda7SUhYRlkuDL0NozZzF9b34mZ1NZMZlxKRlDsyEZ7y2v25ioZQxelBGzmU4wFCWL8JTvia0XUw2tmM_qtsf86rUrPgMtXIBaRQrWMcbum70DNhfxy3ArI_epgndyyxK0a5fIjKgJHIg-7n7XlqKGqrfk1wCoehqX53p0WiIoSeKchmAKIhLeUsUctIpgd7Z00hNvWlaz8aGxy1rZrStDNQKLeEMQkt3gxSs7G2SKFQToItDBOEnzyRdIvwob0WyPmU7GMGsQH1QvqjR2YrrIoYNp6M4Gi0LqXoaPEYxjmdUh-pn97awZGlXOQc-ytMy1SEihNvbtR7WL3yRuJlOzEtS8bzfLYec1HuSX4-qbb7nIRo4MgaD_NcGMiQENbTQjslEaMIYH0VBcF0fGQ9_KiIQYn3RpkiA4_WUzIoQ6Y3iGOC_l0Xbjy-KTlIRvbDOnRqCjTH9EkBmbTl2MmTgjVxO_fQCODt2p-xadFtYVqHNhzEFbrR5FWOsAbPmcoKGrZbPEE_SmAnLo508lKyQ4c-aFHsojKhrCawe9vdukB4PR9tEEmP_f6hNl0u5Rol8MSVBSP2DJ2kLm2-OsAk5rL_P-S6EGT66peoR9Goi',
    }

    custom_settings = {
        'FEEDS': {
            'Output/monroe_parcel_data.json': {
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
        file_path = "input/monroe_address_list.csv"
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
            yield scrapy.Request(
                url='https://qpublic.schneidercorp.com/Application.aspx?AppID=605&LayerID=9946&PageTypeID=2&PageID=4381',
                callback=self.parse,                #headers=self.headers,
                meta={'search_address': address},                dont_filter=True,
            )

    def parse(self, response):
        payload = deepcopy(self.data)
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['__EVENTARGUMENT'] = response.css('#__EVENTARGUMENT::attr(value)').get()
        payload['ctlBodyPane$ctl01$ctl01$txtAddress'] = response.meta.get('search_address')
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, #headers=self.headers,
                                 meta={'search_address':response.meta.get('search_address')})

    def parse_pages(self, response):
        if response.xpath("//*[contains(text(),'No results match your search criteria')]"):
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/monroe_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
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
                                           'parcel_url': parcel_url, 'encoded_url': encoded_url})

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
        yield scrapy.FormRequest(url=encoded_url, formdata=payload, method='POST', callback=self.parse_detail, # headers=self.headers,
                                 meta={'search_address': response.meta.get('search_address')})

    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/parent::td/following-sibling::td/div/span/text()").get('').strip()

        property_info['property_id'] = response.xpath("//*[contains(text(),'Property ID')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['millage_group'] = response.xpath("//*[contains(text(),'Millage Group')]/parent::td/following-sibling::td/div/span/text()").get('').strip()


        property_address = response.xpath("//*[contains(text(),'Location Address')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['property_address'] = property_address
        zip_code_pattern = r'\b\d{5}(?:-\d{4})?\b'  # Regular expression pattern for zip code
        zip_codes = re.findall(zip_code_pattern, property_address)
        if zip_codes:
            property_info['property_zipcode'] = zip_codes[-1]
        else:
            property_info['property_zipcode'] = ''

        property_info['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_lstDeed_ctl00_sprDeedName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_lstDeed_ctl01_sprDeedName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner3'] = ''

        mailing_add = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_lstDeed_ctl00_lblAddress1')]/text()").get('').strip()
        mailing_zip = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_lstDeed_ctl00_lblCityStateZip')]/text()").get('').strip()
        mailing_address = mailing_add + ' , ' + mailing_zip
        property_info['mailing_address_1'] = mailing_address
        property_info['mailing_address_2'] = ''
        pattern = re.compile(r'\b([^,]+),\s*([A-Za-z]{2})\s*(\d{5})\b')

        address_pattern = r'(?P<city>[A-Za-z\s]+)\s(?P<state>[A-Z]{2})\s(?P<zip>\d{5})'
        match = re.match(address_pattern, mailing_zip)
        if match:
            property_info['mailing_city'] = match.group('city')
            property_info['mailing_state'] = match.group('state')
            property_info['mailing_zipcode'] = match.group('zip')
        else:
            property_info['mailing_city'] = ''
            property_info['mailing_state'] = ''
            property_info['mailing_zipcode'] = ''

        property_info['legal_description'] = response.xpath("//*[contains(text(),'Legal Description')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['neighborhood_code'] = response.xpath("//*[contains(text(),'Neighborhood')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['property_class'] = response.xpath("//*[contains(text(),'Property Class')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['subdivision'] = response.xpath("//*[contains(text(),'Subdivision')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Sec/Twp/Rng')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['location_address'] = property_address

        property_info['search_address'] = response.meta.get('search_address')

        property_info['census'] = ''
        property_info['property_use_code'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
        property_info['neighborhood'] = ''
        property_info['affordable_housing'] = ''
        property_info['subdivision_code'] = ''
        property_info['taxing_district'] = ''
        property_info['acreage'] = ''
        property_info['mileage'] = ''
        property_info['homestead_exemption'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info


        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        listing['location_address'] = property_address
        listing['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_lstDeed_ctl00_sprDeedName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        self.save_to_csv(listing)


        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl05_lblName')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = []
            for lands in response.css('#ctlBodyPane_ctl05_ctl01_gvwList>tbody>tr'):
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
        building_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl06_lblName')]/text()").get('').strip()
        if building_info_check:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in response.css('#ctlBodyPane_ctl06_mSection .block-row'):
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = buildings.xpath(".//*[contains(text(),'Building ID')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['beds'] = buildings.xpath(".//*[contains(text(),'Bedrooms')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['baths'] = buildings.xpath(".//*[contains(text(),'Full Bathrooms')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['stories'] = buildings.xpath(".//*[contains(text(),'Stories')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['half_baths'] = buildings.xpath(".//*[contains(text(),'Half Bathrooms')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['built_year'] = buildings.xpath(".//*[contains(text(),'Year Built')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['ac'] = ''
                building_info['heat'] = buildings.xpath(".//*[contains(text(),'Heating Type')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['floor_cover'] = buildings.xpath(".//*[contains(text(),'Flooring Type')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['frame_type'] = ''
                building_info['interior_walls'] = buildings.xpath(".//*[contains(text(),'Interior Walls')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['roof_cover'] = buildings.xpath(".//*[contains(text(),'Roof Coverage')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['exterior_walls'] = buildings.xpath(".//*[contains(text(),'Exterior Walls')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['gross_building_area'] = buildings.xpath(".//*[contains(text(),'Gross Sq Ft')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['living_area'] = buildings.xpath(".//*[contains(text(),'Finished Sq Ft')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
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

                certified_values['land'] = response.xpath(f"//*[contains(text(),'Market Land Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['building'] = ''
                certified_values['extra_feature'] = ''
                certified_values['just'] = response.xpath(f"//*[contains(text(),'Just Market Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['assessed'] = response.xpath(f"//*[contains(text(),'Total Assessed Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['exemptions'] = response.xpath(f"//*[contains(text(),'School Exempt Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['taxable'] = response.xpath(f"//*[contains(text(),'School Taxable Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
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


        '''     5 - extra_features       -      No data available on Website           '''
        item['extra_features'] = []


        '''     6 - transactions      '''
        sales = response.xpath("//*[contains(@id,'ctlBodyPane_ctl08_lblName')]/text()").get('').strip()
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css('#ctlBodyPane_ctl08_ctl01_gvwList>tbody>tr'):
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('th ::text').get('').strip()
                book = sales.css("td:nth-child(5) span ::text").get('').strip()
                page = sales.css("td:nth-child(6) span ::text").get('').strip()
                sales_info['document_number'] = book + '/' + page
                sales_info['qualification_code'] = sales.css('td:nth-child(7) ::text').get('').strip()
                sales_info['grantor'] = sales.css('td:nth-child(9) span::text').get('').strip()
                sales_info['grantee'] = sales.css('td:nth-child(10) span::text').get('').strip()
                sales_info['document_type'] = sales.css('td:nth-child(3) ::text').get('').strip()
                sales_info['price'] = sales.css('td:nth-child(2) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []


        '''     7 - permits               '''
        permits = response.xpath("//*[contains(@id,'ctlBodyPane_ctl09_lblName')]/text()").get('').strip()
        if permits:
            PERMITS_LIST = []
            for permit in response.css('#ctlBodyPane_ctl09_ctl01_gvwData>tbody>tr'):
                permit_info = dict()
                permit_info['application'] = permit.css('th:nth-child(1) ::text').get('').strip()
                permit_info['property_type'] = ''
                permit_info['property_owner'] = ''
                permit_info['application_date'] = permit.css('td:nth-child(3) ::text').get('').strip()
                permit_info['valuation'] = permit.css('td:nth-child(4) ::text').get('').strip()
                permit_info['parcel_id'] = ''
                permit_info['subcontractor'] = ''
                permit_info['contractor'] = ''
                permit_info['permit_type'] = permit.css('td:nth-child(5) ::text').get('').strip()
                permit_info['issue_date'] = permit.css('td:nth-child(2) ::text').get('').strip()
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

        yield item


    def save_to_csv(self, data):
        with open('Output/monroe_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)

