import re
import csv
import scrapy
from copy import deepcopy
from scrapy.utils.response import open_in_browser
from twocaptcha import TwoCaptcha

from urllib.parse import quote

class Liberty(scrapy.Spider):
    name = 'Liberty'
    prefix = 'https://qpublic.schneidercorp.com'
    url = "https://qpublic.schneidercorp.com/Application.aspx?App=LibertyCountyFL&Layer=Parcels&PageType=Search"
    cookies = {
        '_ga': 'GA1.1.1729783499.1705487862',
        'MODULES828': '',
        'MODULESVISIBILE828': '43481%7C43486',
        'ASP.NET_SessionId': 'es1sgeo3qxjw1ieap3x4mle3',
        '_ga_7ZQ1FTE1SG': 'GS1.1.1710078935.34.1.1710078936.0.0.0',
        'cf_clearance': 'hGokL59amk0LHbf.zbIeEV2FuY.3cEqnKqxbwJVcDSY-1710078940-1.0.1.1-13kT85Hvj.4AG5XHhu9MH0R2vo_kfOSiKWCt4flnPLCNONsepKEtD26MR0t8iQrfeYcq898pnqMyHOq62_ROXg',
    }
    headers = {
        'authority': 'qpublic.schneidercorp.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        ### 'cookie': '_ga=GA1.1.1729783499.1705487862; MODULES828=; MODULESVISIBILE828=43481%7C43486; ASP.NET_SessionId=es1sgeo3qxjw1ieap3x4mle3; _ga_7ZQ1FTE1SG=GS1.1.1710078935.34.1.1710078936.0.0.0; cf_clearance=hGokL59amk0LHbf.zbIeEV2FuY.3cEqnKqxbwJVcDSY-1710078940-1.0.1.1-13kT85Hvj.4AG5XHhu9MH0R2vo_kfOSiKWCt4flnPLCNONsepKEtD26MR0t8iQrfeYcq898pnqMyHOq62_ROXg',
        'origin': 'https://qpublic.schneidercorp.com',
        'referer': 'https://qpublic.schneidercorp.com/Application.aspx?App=LibertyCountyFL&Layer=Parcels&PageType=Search',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"122.0.6261.112"',
        'sec-ch-ua-full-version-list': '"Chromium";v="122.0.6261.112", "Not(A:Brand";v="24.0.0.0", "Google Chrome";v="122.0.6261.112"',
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

    data = {
        '__EVENTTARGET': 'ctlBodyPane$ctl01$ctl01$btnSearch',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '44rN7GrpEUbqUTQcF2yCui18y07O1y3Xjb79FPt4Rcn88NkZ4vOlQ3VV3qOv5PX1yjKTNGv+hdvQuHLkRLR1MzCodHlHCb1aZ6ZlJ0W4srqMpmQKqq3KF+jMMi99ryk/AYB5dz+TUSkYCUUonbfwp1nWFfOvmVRRDVPAt/wRf/pItQxWO2H1xA+VQ8c3JdlneNc6P2Y0Y8GrEsVXd085PfHoHRiSg4iR8QDYt0MQXhCujyBx',
        '__VIEWSTATEGENERATOR': '569DB96F',
        'ctlBodyPane$ctl00$ctl01$txtName': '',
        'ctlBodyPane$ctl00$ctl01$txtNameExact': '',
        'ctlBodyPane$ctl01$ctl01$txtAddress': '12th street',
        'ctlBodyPane$ctl01$ctl01$txtAddressExact': '',
        'ctlBodyPane$ctl02$ctl01$txtParcelID': '',
        'ctlBodyPane$ctl03$ctl01$txtName': '',
    }

    site_key_headers = {
        'authority': 'qpublic.schneidercorp.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'cookie': '_ga=GA1.1.1729783499.1705487862; MODULES828=; MODULESVISIBILE828=43481%7C43486; ASP.NET_SessionId=skvesgjj4hehigdm3rfab5gn; _ga_7ZQ1FTE1SG=GS1.1.1710276295.46.1.1710276386.0.0.0; cf_clearance=AukOyOhNWbMaHt48qLtiB6bg6xUPFtKM2RgxBcU_mv4-1710281069-1.0.1.1-1x2PuI3HgbW3MHsKt3pYVt2Opo_Q4BvrblcVIC60Wqqxq1lY.U.C2B8bh8SBXxMSD8qTzkSL3oTPXsIyImNWHQ',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"122.0.6261.112"',
        'sec-ch-ua-full-version-list': '"Chromium";v="122.0.6261.112", "Not(A:Brand";v="24.0.0.0", "Google Chrome";v="122.0.6261.112"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }
    captcha_data = {    # __EVENTTARGET, __VIEWSTATE,   __VIEWSTATEGENERATOR,   __EVENTVALIDATION,   g-recaptcha-response
        '__EVENTTARGET': 'btnSubmit',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': 'Lfb7c61Zf8MpSfpUZni+Sa/YFYzeJw6EcSIkrASRV6IP53raPbvAGV/wpR4U/yQMYiTyVWUvN4V2Y1q8b+i4sc2h4KE=',
        '__VIEWSTATEGENERATOR': 'A1A9A2FD',
        '__EVENTVALIDATION': 'FwI9QkVkmc+B/zk7ebhJ9rLjrXnR73f7+KYYkeYnV3Eq3Uaebni1RMIGIAqw0LHT1U59AGb2D4swvUh0HLgZBEAIPND4bi2gO7h3CqqUt7qN2cnm',
        'g-recaptcha-response': '03AFcWeA5Uut1JZFWZ4eYsmFt0RhcO4bCizJk0cqToZZZUTh9UENEj9htUWGiTCA6s-gdilnBCFYwRbNJbtdF-LYezR0upPJr_8n-aX-mVqd-YTbCd4dNaSgNAOqrL1tvdOTDT-uq8-Y434oGnbTLqiXOlxZnQvIG6oBKiS79BubZjq3eEwIc-6W91fLrUMo5iRe7aCarMcdN_5fMN81PSiNSHvmh3h4aQdhntXCMxkgRFMofjlYxCWat1YmwRaqNg_ncR8hJ_cgrvUt-Ouz3jjZtUFtt6NdH36LoGYonVPkB5HL5uiYEfZBOI-T41mLLN8XJqse0tkqINyBR0OqPJ_zv-SRWDWrg_9ltcTKyZPCQTrXKcg_qUGbYu8EzP4XJgQE4W4FJdE4sU14MFmOfK10IqI18yDgBKegcHBTCaXvVrdQkDFMxIwy2p_jndF0oDqutoT9owlm2GlgovT_YZxK0-ldLeey35PoO_tA4SrAt1A5Esv66pO3xAgo5gz1Lpf3Ox6e-yAgUhF5Vr4fMgYJjVYYvudhHh9yxXfdacl23cG8sc3fH_GxfMDJKRg9wAxsHu-L6qlS7MlTRH-uM0Ai9GQXjBl-lFSiaUlz5HyCVj4_LAlvyLY2cmIiXBX8DZXZA6VKq-fVtPulVcMZLDJ7zDUdbOz-uOj-hXE_hVlkOoNXdlPkzF2ZNwb5XeY6zG-RGhA7CANZsb',
    }

    custom_settings = {
        'FEEDS': {
            'Output/liberty_parcel_data.json': {
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

    TwoCaptcha_Key = "1a05ce9c0ed9049c90cc27f43d9b60e5",  # 2-Captcha API Key
    '''
    solver = TwoCaptcha(TwoCaptcha_Key)
    result = solver.recaptcha(sitekey='6Le-wvkSVVABCPBMRTvw0Q4Muexq1bi0DJwx_mJ-',
                              url='https://mysite.com/page/with/recaptcha',
                              param1=..., ...)
    '''


    def start_requests(self):
        file_path = "input/liberty_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        for address in addresses:#[:1]:
            yield scrapy.Request(
                url='https://qpublic.schneidercorp.com/Application.aspx?App=LibertyCountyFL&Layer=Parcels&PageType=Search',
                callback=self.parse,                #headers=self.headers,
                meta={'search_address': address},                dont_filter=True,
            )

    def parse(self, response):
        payload = deepcopy(self.data)
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['ctlBodyPane$ctl01$ctl01$txtAddress'] = response.meta.get('search_address')
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, #headers=self.headers,
                                 meta={'search_address':response.meta.get('search_address')})

    def parse_pages(self, response):
        if response.xpath("//*[contains(text(),'No results match your search criteria')]"):
            search_address = response.meta.get('search_address')
            with open('Output/liberty_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            for row_div in response.css('#ctlBodyPane_ctl00_ctl01_gvwParcelResults>tbody>tr'):#[:5]
                parcel_url = self.prefix + row_div.css('.normal-font-label ::attr(href)').get('').strip()

                encoded_url = 'https://qpublic.schneidercorp.com/ValidateUser.aspx?url=' + quote(parcel_url, safe='')
                yield scrapy.Request(url=encoded_url, callback=self.captcha_parse, #headers=self.site_key_headers,
                    meta={'search_address':response.meta.get('search_address'),
                          'parcel_url':parcel_url, 'encoded_url':encoded_url})

    def captcha_parse(self, response):
        site_key = response.css('.g-recaptcha ::attr(data-sitekey)').get('').strip()
        parcel_url = response.meta.get('parcel_url')
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
        yield scrapy.FormRequest(url=encoded_url, formdata=payload, method='POST', callback=self.parse_detail, #headers=self.headers,
                                 meta={'search_address':response.meta.get('search_address')})

    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['location_address'] = response.xpath("//*[contains(text(),'Location Address')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['legal_description'] = response.xpath("//*[contains(text(),'Brief Tax Description')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['property_use_code'] = response.xpath("//*[contains(text(),'Property Use Code')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//*[contains(text(),'Sec/Twp/Rng')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['homestead_exemption'] = response.xpath("//*[contains(text(),'Homestead')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed_lnkSearch')]/text()").get('').strip()
        property_info['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName2_lnkUpmSearchLinkSuppressed_lblSearch')]/text()").get('').strip()
        property_info['owner3'] = ''
        mailing_add = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_lblOwnerAddress')]/text()").getall()
        mailing_address = ', '.join(element.strip() for element in mailing_add)
        property_info['mailing_address_1'] = mailing_address
        property_info['mailing_address_2'] = ''

        pattern = re.compile(r'\b([^,]+),\s*([A-Za-z]{2})\s*(\d{5})\b')
        matches = pattern.findall(mailing_address)
        if matches:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = matches[0]

        property_info['taxing_district'] = response.xpath("//*[contains(text(),'Tax District')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['mileage'] = response.xpath("//*[contains(text(),'Millage Rate')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['acreage'] = response.xpath("//*[contains(text(),'Acreage')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        property_info['search_address'] = response.meta.get('search_address')
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
        property_info['property_address'] = ''
        property_info['property_zipcode'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''
        item['main_info'] = property_info


        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.xpath("//*[contains(text(),'Parcel ID')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        listing['location_address'] = response.xpath("//*[contains(text(),'Location Address')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        listing['owner1'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        listing['owner2'] = response.xpath("//*[contains(@id,'ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName2_lnkUpmSearchLinkSuppressed')]/text()").get('').strip()
        self.save_to_csv(listing)

        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(text(),'Land Information')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = []
            for lands in response.css('#ctlBodyPane_ctl02_ctl01_grdLand_grdFlat>tbody>tr'):
                land_info = dict()
                land_info['land_use'] = lands.css('th ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(2) ::text').get('').strip()
                land_info['unit_type'] = lands.css('td:nth-child(3) ::text').get('').strip()
                land_info['frontage'] = lands.css('td:nth-child(4) ::text').get('').strip()
                land_info['depth'] = ''
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
                building_info['beds']  = buildings.xpath(".//*[contains(text(),'Bedrooms')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['baths']  = buildings.xpath(".//*[contains(text(),'Bathrooms')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['stories']  = buildings.xpath(".//*[contains(text(),'Stories')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['half_baths'] = ''
                building_info['built_year']  = buildings.xpath(".//*[contains(text(),'Actual Year Built')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['ac']  = buildings.xpath(".//*[contains(text(),'Air Conditioning')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['heat'] = buildings.xpath(".//*[contains(text(),'Heat')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['floor_cover']  = buildings.xpath(".//*[contains(text(),'Floor Cover')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['frame_type']  = buildings.xpath(".//*[contains(text(),'Frame Type')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['interior_walls']  = buildings.xpath(".//*[contains(text(),'Interior Walls')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['roof_cover']  = buildings.xpath(".//*[contains(text(),'Roof Cover')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['exterior_walls']  = buildings.xpath(".//*[contains(text(),'Exterior Walls')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['gross_building_area'] = buildings.xpath(".//*[contains(text(),'Total Area')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
                building_info['living_area'] = ''
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

                certified_values['building'] = response.xpath(f"//*[contains(text(),'Building Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['land'] = response.xpath(f"//*[contains(text(),'Land Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['extra_feature'] = response.xpath(f"//*[contains(text(),'Extra Features Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
                certified_values['just'] = response.xpath(f"//*[contains(text(),'Just (Market) Value')]/following-sibling::td[{valuation_count}]/text()").get('').strip()
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

        '''     5 - extra_features      '''
        extra_features_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl04_lblName')]/text()").get('').strip()
        if extra_features_check:
            EXTRA_FEATURES_list = []
            x_features_count = 1
            for extra_features in response.css('#ctlBodyPane_ctl04_ctl01_grdSales_grdFlat>tbody>tr'):
                extra_f = dict()
                extra_f['id'] = x_features_count
                x_features_count += 1
                extra_f['real_estate_id'] = extra_features.css('td:nth-child(1) ::text').get('').strip()
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
        with open('Output/liberty_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1','owner2']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        # pass
