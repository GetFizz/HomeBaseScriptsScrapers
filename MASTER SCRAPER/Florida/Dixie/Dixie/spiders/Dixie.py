#######         Dixie

import re
import csv
import scrapy
from copy import deepcopy
from scrapy.utils.response import open_in_browser
from twocaptcha import TwoCaptcha
from urllib.parse import quote

class Dixie(scrapy.Spider):
    name = 'Dixie'
    prefix = 'https://qpublic.schneidercorp.com'
    url = "https://qpublic.schneidercorp.com/Application.aspx?AppID=867&LayerID=16385&PageTypeID=2&PageID=7230"

    cookies = {
        '_ga': 'GA1.1.1729783499.1705487862',
        'MODULES828': '',
        'MODULESVISIBILE828': '43481%7C43486',
        'ASP.NET_SessionId': '1k243xxszece5x0xakdftdcd',
        '_ga_7ZQ1FTE1SG': 'GS1.1.1710527609.55.0.1710527609.0.0.0',
        'cf_clearance': 'Dy3xqe240CjOn6ui81UZqFltoz0GjPHkn2vVunXJojM-1710527616-1.0.1.1-ukM8t7FSyh08UXgQl4tRydktukPIFs_5k4.R4PfYIQ4k5l882orfqh_hV9epD7iwkw1ddFWwJUORTvL__wYBNg',
    }
    headers = {
        'authority': 'qpublic.schneidercorp.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '_ga=GA1.1.1729783499.1705487862; MODULES828=; MODULESVISIBILE828=43481%7C43486; ASP.NET_SessionId=1k243xxszece5x0xakdftdcd; _ga_7ZQ1FTE1SG=GS1.1.1710527609.55.0.1710527609.0.0.0; cf_clearance=Dy3xqe240CjOn6ui81UZqFltoz0GjPHkn2vVunXJojM-1710527616-1.0.1.1-ukM8t7FSyh08UXgQl4tRydktukPIFs_5k4.R4PfYIQ4k5l882orfqh_hV9epD7iwkw1ddFWwJUORTvL__wYBNg',
        'origin': 'https://qpublic.schneidercorp.com',
        'referer': 'https://qpublic.schneidercorp.com/Application.aspx?AppID=867&LayerID=16385&PageTypeID=2&PageID=7230',
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
    data = {        #   __EVENTARGUMENT,    __VIEWSTATE,    __VIEWSTATEGENERATOR,   ctlBodyPane$ctl01$ctl01$txtAddress
        '__EVENTTARGET': 'ctlBodyPane$ctl01$ctl01$btnSearch',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '26ZmN1ZCCZuDkwAxVax1Thm+XOQW2vUx1xTmSGTuvZ32BdSU+Oh38hPBhQo5oGgEh/MRXd5UnPGEJHRXwJMPyJDeug704GVj5v3RrOYYdu2uWIYKHX9X5HEKXlpUAHuMQtuUqUoZwPrQ5JFIxkD9d9R7EpG6Vm3Juk1SmLIJiSOSGmxxrlv0ho3xa5J3WZgcuuNLVBsqhz96jQe3gTCHV//6JlEtMDDfzEnR2q1WBab0uV+E',
        '__VIEWSTATEGENERATOR': '569DB96F',
        'ctlBodyPane$ctl00$ctl01$txtName': '',
        'ctlBodyPane$ctl00$ctl01$txtNameExact': '',
        'ctlBodyPane$ctl01$ctl01$txtAddress': '121st Street Northeast',
        'ctlBodyPane$ctl01$ctl01$txtAddressExact': '',
        'ctlBodyPane$ctl02$ctl01$txtParcelID': '',
        'ctlBodyPane$ctl03$ctl01$srch1$txtInput': '',
    }

    TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key
    captcha_data = {    # __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        '__EVENTTARGET': 'btnSubmit',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': 'emQdT482Wa5mCXbrkfFVj9Aa217/n1xgfq25xOOCExg4LdcZ7IObkVe2NnkxMFq8KaNTK4XpBp8tNMs4F7N0fBSJbHM=',
        '__VIEWSTATEGENERATOR': 'A1A9A2FD',
        '__EVENTVALIDATION': 'QUg1vq3hbxcB1wQgf1RvzB6oqsH5ocFWgpmZQJVJF1rGtD9zcAsRsdT1lZ2SNmECBs0VzoKlUHq3E02/S7J4fxPYPP5V0AFE1ehUI9BrYEvNtlaj',
        'g-recaptcha-response': '03AFcWeA5NL0DfCyf4M0kNs3Jhfl5bdmsmNd4ZpcbEICaC4yYtYQaa2sScgzvYCgDdKPLumdYWvqqsjMppnMTvhR_bTIyufnSuDpqmL2fndudEg2FPzw7j0Rtra6RwR6kzeCP8mVWI2ardqvoe8YdVwfMtEEW3nwx3V8awh6_73GgHYeINQ3kY2yi0Yr2yy9s3H_5g1uubjjdZrbUlYUtwIx6KOCxJgtx-B4B_UDO1wa7YrnTkT-kJy9GtgX5_CSpwL5Rnk8xOUv1-8Vcb49EPjh9yWA_KiKOXXx4ccuOx99e3jwSDhGaPBhHz3aIjmK1Ux26-K_K_cSIz0QDhJ_9XcZ5HDY--I2ACS8mfTniOQnV_YB58uUAunH2JrMsuZHG2z5-9HNPJz0Pz5u4x9261Y7fktkTm6-eHsKPM3j-1I39madrzp1UCwIi2QUFgpdc5-BuHgcCZDAjmjoGeBwhoEQwLOPjXl0unw7JEyzcan1gwDZeZmiAUBvI4JNPTQQE_HSctZxbbGE977f1zvmlq_LRDnMcGyYnqLNqZ633FODCElAKnzB_Kmd4xNpnA908PrKXCM_IK5tAf2xlVEA8oCLB0Nm_5VOX_14wFNpM6dxAce8a5QpCStaLEQDyq6so-BfdA5ErQYrOR1C4B7lV62tuPIf4QnqeazcAzaQVE1o3tclB6TiRXu_QM9pdEiho9MYQ-b_0ckAH7',
    }

    custom_settings = {
        'FEEDS': {
            'Output/dixie_parcel_data.json': {
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
        file_path = "input/dixie_address_list.csv"
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
                url='https://qpublic.schneidercorp.com/Application.aspx?AppID=867&LayerID=16385&PageTypeID=2&PageID=7230',
                callback=self.parse,                headers=self.headers,
                meta={'search_address': address, 'cookiejar': index},                dont_filter=True,
            )

    def parse(self, response):
        #   __EVENTARGUMENT,    __VIEWSTATE,    __VIEWSTATEGENERATOR,   ctlBodyPane$ctl01$ctl01$txtAddress
        payload = deepcopy(self.data)
        # payload['__EVENTTARGET'] = response.css('#__EVENTTARGET::attr(value)').get()
        # payload['__EVENTARGUMENT'] = response.css('#__EVENTARGUMENT::attr(value)').get()
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['ctlBodyPane$ctl01$ctl01$txtAddress'] = response.meta.get('search_address')

        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, headers=self.headers,
                                 meta={'search_address':response.meta.get('search_address'), 'cookiejar': response.meta['cookiejar']})

    def parse_pages(self, response):
        if response.xpath("//*[contains(text(),'No results match your search criteria')]"):
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/dixie_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            count = 0
            for row_div in response.css('#ctlBodyPane_ctl00_ctl01_gvwParcelResults>tbody>tr'):#[:2]:
                count += 1
                parcel_url = self.prefix + row_div.css('.normal-font-label ::attr(href)').get('').strip()
                encoded_url = 'https://qpublic.schneidercorp.com/ValidateUser.aspx?url=' + quote(parcel_url, safe='')
                print(count, encoded_url)
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
        # __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['__EVENTVALIDATION'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['g-recaptcha-response'] = desired_token
        encoded_url = response.meta.get('encoded_url')
        yield scrapy.FormRequest(url=encoded_url, formdata=payload, method='POST', callback=self.parse_detail, # headers=self.headers,
                                 meta={'search_address': response.meta.get('search_address'), 'cookiejar': response.meta['cookiejar']})


    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::td/span/text()").get('').strip()

        property_info['property_address'] = response.xpath("//*[contains(text(),'Property Address')]/following-sibling::td/span/text()").get('').strip()
        property_info['property_zipcode'] = ''

        property_info['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_lstOwner_ctl01_sprOwnerName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        # property_info['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName2_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        property_info['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_lstOwner_ctl01_lblOwnerAddress')]/div[1]/text()").get('').strip()
        property_info['owner3'] = ''

        mailing_add = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_lstOwner_ctl01_lblOwnerAddress')]/div/text()").getall()
        mailing_address = ', '.join(element.strip() for element in mailing_add)
        property_info['mailing_address_1'] = mailing_address
        property_info['mailing_address_2'] = ''
        pattern = re.compile(r'\b([^,]+),\s*([A-Za-z]{2})\s*(\d{5})\b')
        matches = pattern.findall(mailing_address)
        if matches:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = matches[0]

        property_info['legal_description'] = response.xpath("//*[contains(text(),'Legal Description')]/parent::td/following-sibling::td/span/text()").get('').strip()
        property_info['millage_group'] = response.xpath("//*[contains(text(),'Millage Group')]/parent::td/following-sibling::td/span/text()").get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Sec/Twp/Rng')]/following-sibling::td/span/text()").get('').strip()
        property_info['mileage'] = response.xpath("//*[contains(text(),'Millage Rate')]/following-sibling::td/span/text()").get('').strip()
        property_info['acreage'] = response.xpath("//*[contains(text(),'Acres')]/parent::td/following-sibling::td/span/text()").get('').strip()
        property_info['neighborhood_code'] = response.xpath("//*[contains(text(),'Neighborhood Code')]/following-sibling::td/span/text()").get('').strip()
        property_info['homestead_exemption'] = response.xpath("//*[contains(text(),'Homestead')]/following-sibling::td/span/text()").get('').strip()
        property_info['location_address'] = response.xpath("//*[contains(text(),'Property Address')]/following-sibling::td/span/text()").get('').strip()

        property_info['search_address'] = response.meta.get('search_address')

        property_info['property_id'] = ''
        property_info['neighborhood'] = ''
        property_info['subdivision'] = ''
        property_info['property_use_code'] = ''
        property_info['taxing_district'] = ''
        property_info['census'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = ''
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''
        property_info['property_class'] = ''
        property_info['affordable_housing'] = ''
        property_info['subdivision_code'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info

        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/following-sibling::td/span/text()").get('').strip()
        listing['location_address'] = property_info['location_address']
        listing['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_lstOwner_ctl01_sprOwnerName_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        self.save_to_csv(listing)

        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_lblName')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = []
            for lands in response.css('#ctlBodyPane_ctl03_ctl01_gvwList>tbody>tr'):
                land_info = dict()
                land_info['land_use'] = lands.css('th ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(2) ::text').get('').strip()
                land_info['unit_type'] = lands.css('td:nth-child(3) ::text').get('').strip()
                land_info['frontage'] = ''
                land_info['depth'] = ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []


        '''     3 - buildings      '''
        building_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl04_lblName')]/text()").get('').strip()
        if building_info_check:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in response.css('#ctlBodyPane_ctl04_mSection .block-row'):
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = ''
                building_info['beds']  = ''
                building_info['baths']  = buildings.xpath(".//*[contains(text(),'Full Bath')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['stories']  = buildings.xpath(".//*[contains(text(),'Story Height')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['half_baths'] = buildings.xpath(".//*[contains(text(),'Half Bath')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['built_year']  = buildings.xpath(".//*[contains(text(),'Year Built')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['ac']  = buildings.xpath(".//*[contains(text(),'Air Conditioned')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['heat'] = buildings.xpath(".//*[contains(text(),'Heat Type')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['floor_cover']  = buildings.xpath(".//*[contains(text(),'Floor Cover')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['frame_type']  = buildings.xpath(".//*[contains(text(),'Frame')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['interior_walls']  = buildings.xpath(".//*[contains(text(),'Interior Wall')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['roof_cover']  = buildings.xpath(".//*[contains(text(),'Roof Material')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['exterior_walls']  = buildings.xpath(".//*[contains(text(),'Exterior Wall')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['gross_building_area'] = buildings.xpath(".//*[contains(text(),'Total Area')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['living_area'] = buildings.xpath(".//*[contains(text(),'Finished Area')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []


        '''     4 - valuations      '''
        valuation_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_lblName')]/text()").get('').strip()
        if valuation_check:
            VALUATION = []
            valuation_count = 1
            for values in response.xpath("//*[contains(@id,'ctlBodyPane_ctl02_ctl01_grdValuation')]/thead/tr/th"):
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''

                certified_values['year'] = values.css("::text").get('').strip()

                certified_values['land'] = response.xpath(f"//*[contains(text(),'Land Value Market Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['building'] = response.xpath(f"//*[contains(text(),'Building Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['extra_feature'] = ''
                certified_values['just'] = response.xpath(f"//*[contains(text(),'Total Just or Market Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['assessed'] = response.xpath(f"//*[contains(text(),'Classified Use or Assessed Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['exemptions'] = response.xpath(f"//*[contains(text(),'School Exemptions')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['taxable'] = response.xpath(f"//*[contains(text(),'Taxable Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['cap'] = response.xpath(f"//*[contains(text(),'Capped Differential')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
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


        '''     5 - extra_features      -   No data available on site     '''
        item['extra_features'] = []

        '''     6 - transactions      '''
        sales = response.xpath("//*[contains(@id,'ctlBodyPane_ctl07_lblName')]/text()").get('').strip()
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css('#ctlBodyPane_ctl07_ctl01_gvwSales>tbody>tr'):
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('th ::text').get('').strip()
                sales_info['document_number'] = sales.css('#ctlBodyPane_ctl07_ctl01_gvwSales_ctl02_sprLegalReference ::text').get('').strip()
                sales_info['qualification_code'] = ''
                sales_info['grantor'] = sales.css('#ctlBodyPane_ctl07_ctl01_gvwSales_ctl02_sprGrantor ::text').get('').strip()
                sales_info['grantee'] = ''
                sales_info['document_type'] = sales.css('td:nth-child(5) ::text').get('').strip()
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

        # print(item)
        yield item

    def save_to_csv(self, data):
        with open('Output/dixie_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass
