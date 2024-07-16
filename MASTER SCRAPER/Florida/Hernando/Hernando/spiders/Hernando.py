#######         Hernando
import json
import re
import csv
import scrapy
from copy import deepcopy

from scrapy.utils.response import open_in_browser


class Hernando(scrapy.Spider):
    name = 'Hernando'
    url = "https://www.hernandopa-fl.us/Propertysearch/"
    cookies = {
        '__AntiXsrfToken': '4d9e42fc14f141a493a239b01971b2af',
        'arp_scroll_position': '600',
    }

    agree_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '__AntiXsrfToken=07388d62edb94061b77cc1e81e849a41',
        'origin': 'https://www.hernandopa-fl.us',
        'pragma': 'no-cache',
        'referer': 'https://www.hernandopa-fl.us/Propertysearch/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    }
    agree_data = {
        '__LASTFOCUS': '',        '__EVENTTARGET': '',        '__EVENTARGUMENT': '',        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '9DE35745',        '__VIEWSTATEENCRYPTED': '',        '__EVENTVALIDATION': '',
        'ctl00$MainCurrTabIndex': '0',        'ctl00$DetailCurrTabIndex': '',        'ctl00$MapCurrTabIndex': '',
        'ctl00$SelectedRow': '',        'ctl00$DSID': '', 'ctl00$MapIsDisplayed': '','ctl00$CurrentYear': '',
        'ctl00$MapToggleButtonHidden': '',        'ctl00$SalesRpt': 'N',                'ctl00$VisibleLayers': '',
        'ctl00$hidAccordionIndex': '0',        'ctl00$hidParcelKey': '0',        'ctl00$MainContent$btnAccept': 'I Agree',
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': '__AntiXsrfToken=6ba7047b908c4fcb8b00beb939371dcc; arp_scroll_position=700',
        'origin': 'https://www.hernandopa-fl.us',
        'referer': 'https://www.hernandopa-fl.us/Propertysearch/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }
    data = {
        '__LASTFOCUS': '',
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '9DE35745',
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': '',
        'ctl00$MainCurrTabIndex': '0',
        'ctl00$DetailCurrTabIndex': '',
        'ctl00$MapCurrTabIndex': '',
        'ctl00$SelectedRow': '',
        'ctl00$DSID': '',
        'ctl00$MapIsDisplayed': '',
        'ctl00$MapToggleButtonHidden': '',
        'ctl00$CurrentYear': '',
        'ctl00$SalesRpt': 'N',
        'ctl00$VisibleLayers': '',
        'ctl00$hidAccordionIndex': '0',
        'ctl00$hidParcelKey': '0',
        'ctl00$MainContent$txtOwnerName': '',
        'ctl00$MainContent$txtStreetNo': '',
        'ctl00$MainContent$txtStreetName': 'Admiral',
        'ctl00$MainContent$ddStreetType': 'ST',
        'ctl00$MainContent$txtParcelNumber': '',
        'ctl00$MainContent$txtParcelKey': '',
        'ctl00$MainContent$ddSection': '',
        'ctl00$MainContent$ddTownship': '',
        'ctl00$MainContent$ddRange': '',
        'ctl00$MainContent$txtBlock': '',
        'ctl00$MainContent$txtLot': '',
        'ctl00$MainContent$ddEnterpriseZone': '',
        'ctl00$MainContent$ddAcreageTractSize': '',
        'ctl00$MainContent$txtDescription': '',
        'ctl00$MainContent$ddDOR': '',
        'ctl00$MainContent$ddSubdivision': '',
        'ctl00$MainContent$ddNeighborhood': '',
        'ctl00$MainContent$txtSFHeated1': '',
        'ctl00$MainContent$txtSFHeated2': '',
        'ctl00$MainContent$txtYearBuilt1': '',
        'ctl00$MainContent$txtYearBuilt2': '',
        'ctl00$MainContent$ddSinkhole': '',
        'ctl00$MainContent$ddSinkholeRepaired': '',
        'ctl00$MainContent$txtSalesDate1': '',
        'ctl00$MainContent$txtSalesDate2': '',
        'ctl00$MainContent$txtSalesPrice1': '',
        'ctl00$MainContent$txtSalesPrice2': '',
        'ctl00$MainContent$ddVacantImproved': '',
        'ctl00$MainContent$txtORBook': '',
        'ctl00$MainContent$txtORPage': '',
        'ctl00$MainContent$txtTPPParcelID': '',
        'ctl00$MainContent$txtTPPKey': '',
        'ctl00$MainContent$txtTPPBusinessName': '',
        'ctl00$MainContent$rbSalesYear': '2024',
        'ctl00$MainContent$txtOtherYear': '',
        'ctl00$MainContent$rbSalesMonth': 'Entire Year',
        'ctl00$MainContent$rbSalesSortBy': "Owner's Name",
        'ctl00$MainContent$ddSalesSection': '',
        'ctl00$MainContent$ddSalesTownship': '',
        'ctl00$MainContent$ddSalesRange': '',
        'ctl00$MainContent$ddSalesDORCode': '',
        'ctl00$MainContent$ddSalesSubdivision': '',
        'ctl00$MainContent$ddSalesNeighborhood': '',
        'ctl00$MainContent$ddSalesVacantImproved': '',
        'ctl00$MainContent$btnSearch': 'Search',
        'ctl00$MainContent$ddRecsPerPage': '10',
        'ctl00$MainContent$ddReportType': 'Mailing Labels',
        'ctl00$MainContent$ddReportOutput': 'PDF',
    }

    pagination_data = {
        '__EVENTTARGET': 'ctl00$MainContent$gvParcelResults',
        '__EVENTARGUMENT': 'Page$2',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '9DE35745',
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': '',
        'ctl00$MainCurrTabIndex': '1',
        'ctl00$DetailCurrTabIndex': '',
        'ctl00$MapCurrTabIndex': '',
        'ctl00$SelectedRow': '',
        'ctl00$DSID': '3umdzlw826b7nibt',
        'ctl00$MapIsDisplayed': '',
        'ctl00$MapToggleButtonHidden': '',
        'ctl00$CurrentYear': '',
        'ctl00$SalesRpt': 'N',
        'ctl00$VisibleLayers': '',
        'ctl00$hidAccordionIndex': '0',
        'ctl00$hidParcelKey': '',
        'ctl00$MainContent$txtOwnerName': '',
        'ctl00$MainContent$txtStreetNo': '',
        'ctl00$MainContent$txtStreetName': 'Admiral',
        'ctl00$MainContent$ddStreetType': 'ST',
        'ctl00$MainContent$txtParcelNumber': '',
        'ctl00$MainContent$txtParcelKey': '',
        'ctl00$MainContent$ddSection': '',
        'ctl00$MainContent$ddTownship': '',
        'ctl00$MainContent$ddRange': '',
        'ctl00$MainContent$txtBlock': '',
        'ctl00$MainContent$txtLot': '',
        'ctl00$MainContent$ddEnterpriseZone': '',
        'ctl00$MainContent$ddAcreageTractSize': '',
        'ctl00$MainContent$txtDescription': '',
        'ctl00$MainContent$ddDOR': '',
        'ctl00$MainContent$ddSubdivision': '',
        'ctl00$MainContent$ddNeighborhood': '',
        'ctl00$MainContent$txtSFHeated1': '',
        'ctl00$MainContent$txtSFHeated2': '',
        'ctl00$MainContent$txtYearBuilt1': '',
        'ctl00$MainContent$txtYearBuilt2': '',
        'ctl00$MainContent$ddSinkhole': '',
        'ctl00$MainContent$ddSinkholeRepaired': '',
        'ctl00$MainContent$txtSalesDate1': '',
        'ctl00$MainContent$txtSalesDate2': '',
        'ctl00$MainContent$txtSalesPrice1': '',
        'ctl00$MainContent$txtSalesPrice2': '',
        'ctl00$MainContent$ddVacantImproved': '',
        'ctl00$MainContent$txtORBook': '',
        'ctl00$MainContent$txtORPage': '',
        'ctl00$MainContent$txtTPPParcelID': '',
        'ctl00$MainContent$txtTPPKey': '',
        'ctl00$MainContent$txtTPPBusinessName': '',
        'ctl00$MainContent$rbSalesYear': '2024',
        'ctl00$MainContent$txtOtherYear': '',
        'ctl00$MainContent$rbSalesMonth': 'Entire Year',
        'ctl00$MainContent$rbSalesSortBy': "Owner's Name",
        'ctl00$MainContent$ddSalesSection': '',
        'ctl00$MainContent$ddSalesTownship': '',
        'ctl00$MainContent$ddSalesRange': '',
        'ctl00$MainContent$ddSalesDORCode': '',
        'ctl00$MainContent$ddSalesSubdivision': '',
        'ctl00$MainContent$ddSalesNeighborhood': '',
        'ctl00$MainContent$ddSalesVacantImproved': '',
        'ctl00$MainContent$ddRecsPerPage': '10',
        'ctl00$MainContent$ddReportType': 'Mailing Labels',
        'ctl00$MainContent$ddReportOutput': 'PDF',
    }


    detail_data = {
        '__EVENTTARGET': 'ctl00$MainContent$gvParcelResults$ctl02$lkbParcelKey',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '',
        '__VIEWSTATEGENERATOR': '9DE35745',
        '__VIEWSTATEENCRYPTED': '',
        '__EVENTVALIDATION': '',
        'ctl00$MainCurrTabIndex': '1',
        'ctl00$DetailCurrTabIndex': '',
        'ctl00$MapCurrTabIndex': '',
        'ctl00$SelectedRow': '',
        'ctl00$DSID': '3x8p8h5893xyh29b',
        'ctl00$MapIsDisplayed': '',
        'ctl00$MapToggleButtonHidden': '',
        'ctl00$CurrentYear': '',
        'ctl00$SalesRpt': 'N',
        'ctl00$VisibleLayers': '',
        # 'ctl00$hidAccordionIndex': '0',
        'ctl00$hidParcelKey': '',
        'ctl00$MainContent$txtOwnerName': '',
        'ctl00$MainContent$txtStreetNo': '',
        'ctl00$MainContent$txtStreetName': 'Adrian',
        'ctl00$MainContent$ddStreetType': 'DR',
        'ctl00$MainContent$txtParcelNumber': '',
        'ctl00$MainContent$txtParcelKey': '',
        'ctl00$MainContent$ddSection': '',
        'ctl00$MainContent$ddTownship': '',
        'ctl00$MainContent$ddRange': '',
        'ctl00$MainContent$txtBlock': '',
        'ctl00$MainContent$txtLot': '',
        'ctl00$MainContent$ddEnterpriseZone': '',
        'ctl00$MainContent$ddAcreageTractSize': '',
        'ctl00$MainContent$txtDescription': '',
        'ctl00$MainContent$ddDOR': '',
        'ctl00$MainContent$ddSubdivision': '',
        'ctl00$MainContent$ddNeighborhood': '',
        'ctl00$MainContent$txtSFHeated1': '',
        'ctl00$MainContent$txtSFHeated2': '',
        'ctl00$MainContent$txtYearBuilt1': '',
        'ctl00$MainContent$txtYearBuilt2': '',
        'ctl00$MainContent$ddSinkhole': '',
        'ctl00$MainContent$ddSinkholeRepaired': '',
        'ctl00$MainContent$txtSalesDate1': '',
        'ctl00$MainContent$txtSalesDate2': '',
        'ctl00$MainContent$txtSalesPrice1': '',
        'ctl00$MainContent$txtSalesPrice2': '',
        'ctl00$MainContent$ddVacantImproved': '',
        'ctl00$MainContent$txtORBook': '',
        'ctl00$MainContent$txtORPage': '',
        'ctl00$MainContent$txtTPPParcelID': '',
        'ctl00$MainContent$txtTPPKey': '',
        'ctl00$MainContent$txtTPPBusinessName': '',
        'ctl00$MainContent$rbSalesYear': '2024',
        'ctl00$MainContent$txtOtherYear': '',
        'ctl00$MainContent$rbSalesMonth': 'Entire Year',
        'ctl00$MainContent$rbSalesSortBy': "Owner's Name",
        'ctl00$MainContent$ddSalesSection': '',
        'ctl00$MainContent$ddSalesTownship': '',
        'ctl00$MainContent$ddSalesRange': '',
        'ctl00$MainContent$ddSalesDORCode': '',
        'ctl00$MainContent$ddSalesSubdivision': '',
        'ctl00$MainContent$ddSalesNeighborhood': '',
        'ctl00$MainContent$ddSalesVacantImproved': '',
        'ctl00$MainContent$ddRecsPerPage': '10',
        'ctl00$MainContent$ddReportType': 'Mailing Labels',
        'ctl00$MainContent$ddReportOutput': 'PDF',
    }

    custom_settings = {
        'FEEDS': {
            'Output/hernando_parcel_data.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        }  # Zyte API
        # # ,
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
        # "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
    }

    address_abbreviations = {
        "AVENUE": "AVE", "BLUFF": "BLF", "BOULEVARD": "BLVD", "BEND": "BND", "CIRCLE": "CIR", "CRESCENT": "CRES",
        "COURT": "CT", "COVE": "CV", "DRIVE": "DR", "HOLLOW": "HOLW", "HIGHWAY": "HWY", "JUNCTION": "JCT",
        "LANE": "LN", "LOOP": "LOOP", "MOUNTAIN": "MTN", "PARK": "PARK", "PASS": "PASS", "PATH": "PATH",
        "PARKWAY": "PKWY", "PLACE": "PL", "POINT": "PT", "ROAD": "RD", "RUN": "RUN", "STREET": "ST",
        "TERRACE": "TER", "TRACK": "TRAK", "TRACE": "TRCE", "TRAIL": "TRL", "WAY": "WAY", "CROSSING": "XING"}

    def start_requests(self):
        yield scrapy.Request(
            url='https://www.hernandopa-fl.us/Propertysearch/', callback=self.parse,  # headers=self.headers
        )

    def parse(self, response):
        payload = deepcopy(self.agree_data)
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__EVENTVALIDATION'] = response.css('#__EVENTVALIDATION::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        yield scrapy.FormRequest(url=self.url, method='POST', formdata=payload, headers=self.agree_headers,
                                 callback=self.parse_search)

    def parse_search(self, response):
        # open_in_browser(response)
        file_path = "input/hernando_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8', errors='ignore') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        for address in addresses:#[:1]:
            parts = address.split(maxsplit=1)
            address1 = parts[0]
            address2 = parts[1].split() if len(parts) > 1 else []
            for i in range(len(address2)):
                if address2[i].upper() in self.address_abbreviations:
                    address2[i] = self.address_abbreviations[address2[i].upper()]
            address2_abb = ' '.join(address2)
            address2 = address2_abb.upper()
            payload = deepcopy(self.data)
            payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get('').strip()
            payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get('').strip()
            payload['__EVENTVALIDATION'] = response.css('#__EVENTVALIDATION::attr(value)').get('').strip()
            payload['ctl00$MainContent$txtStreetName'] = address1
            payload['ctl00$MainContent$ddStreetType'] = address2
            page_no = 1
            yield scrapy.FormRequest(url=self.url, method='POST', formdata=payload, headers=self.headers, callback=self.parse_pages,
                meta={'search_address': address, 'page_no':page_no, 'address1':address1,'address2':address2})

    def parse_pages(self, response):
        # open_in_browser(response)
        address = response.meta.get('address')
        address1 = response.meta.get('address1')
        address2 = response.meta.get('address2')

        if response.css('td[nowrap="nowrap"] a'):
            for parcels in response.css('td[nowrap="nowrap"] a'):#[:5]:
                parcel_id = parcels.css('::text').get('').strip()

                parcel_id_href = parcels.css('::attr(href)').get('').strip()
                match = re.search(r"'\b([^']+)\b'", parcel_id_href)
                event_target = match.group(1) if match else ''
                payload = deepcopy(self.detail_data)
                payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
                payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
                payload['__EVENTVALIDATION'] = response.css('#__EVENTVALIDATION::attr(value)').get()
                payload['__EVENTTARGET'] = event_target
                yield scrapy.FormRequest(url=self.url, method='POST', formdata=payload, headers=self.headers, callback=self.detail_pages,
                    meta={'search_address': address})

            '''    PAGINATION     '''
            total_pages = len(response.css('#MainContent_gvParcelResults .ui-corner-all tr td').getall())
            if response.meta.get('page_no') < total_pages:
                payload = deepcopy(self.pagination_data)
                payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
                payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
                payload['__EVENTVALIDATION'] = response.css('#__EVENTVALIDATION::attr(value)').get()
                payload['ctl00$DSID'] = response.css('#DSID::attr(value)').get()
                payload['ctl00$MainCurrTabIndex'] = response.css('#MainCurrTabIndex::attr(value)').get('').strip()

                payload['ctl00$MainContent$txtStreetName'] =  address1
                payload['ctl00$MainContent$ddStreetType'] = address2
                # payload['__EVENTTARGET'] = 'ctl00$MainContent$gvParcelResults',
                page_no = response.meta.get('page_no') + 1
                payload['__EVENTARGUMENT'] = f'Page${page_no}',

                yield scrapy.FormRequest(url=self.url, method='POST', formdata=payload, headers=self.headers, callback=self.parse_pages,
                    meta={'search_address': response.meta.get('address'), 'page_no': page_no, 'address1': address1,'address2': address2})

        else:
            search_address = response.meta.get('search_address')
            print('No results match for ', search_address)
            with open('Output/hernando_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])


    def detail_pages(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        property_info['parcel_id'] = response.xpath("//td[contains(text(),'Parcel #:')]/span[1]/text()").get('').strip()
        property_address = response.xpath("//td[contains(text(),'Site Address:')]/following-sibling::td[1]/span/text()").get('').strip()
        property_info['location_address'] = property_address.strip()
        property_info['subdivision'] = response.xpath("//td[contains(text(),'Subdivision:')]/following-sibling::td[1]/a/span/text()").get('').strip()
        property_info['sec_twp_rng'] = response.xpath("//td[contains(text(),'Sec/Tnshp/Rng:')]/following-sibling::td[1]/text()").get('').strip()

        property_info['property_address'] = property_address.strip()
        zip_code_pattern = r'\b\d{5}\b'
        zip_code_match = re.search(zip_code_pattern, property_address)
        if zip_code_match:
            property_info['property_zipcode'] = zip_code_match.group()
        else:
            property_info['property_zipcode'] = ''

        owners = response.xpath("//td[contains(text(),'Owner Name:')]/following-sibling::td[1]/span/text()").getall()
        property_info['owner1'] = owners[0].strip()
        property_info['owner2'] = owners[1].strip() if len(owners) > 1 else ''
        property_info['owner3'] = owners[2].strip() if len(owners) > 2 else ''

        property_info['mailing_address_1'] = response.xpath("//td[contains(text(),'Mailing')]/following-sibling::td[1]/span/text()").get('').strip()
        property_info['mailing_address_2'] = ''
        csz = response.xpath("//td[contains(text(),'Address:')]/following-sibling::td[1]/span/text()").get('').strip()
        pattern = r"^(.+?)\s+([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)$"
        match = re.match(pattern, csz)
        if match:
            property_info['mailing_city'] = match.group(1)
            property_info['mailing_state'] = match.group(2)
            property_info['mailing_zipcode'] = match.group(3)
        else:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = '','',''

        property_info['legal_description'] = response.xpath("//td[contains(text(),'Description:')]/following-sibling::td[1]/text()").get('').strip()

        # property_info['search_address'] = response.meta.get('search_address')
        property_info['neighborhood'] = response.xpath("//td[contains(text(),'Neighborhood:')]/following-sibling::td[1]/span/text()").get('').strip()

        ''' Empty fields'''
        property_info['census'] = ''
        property_info['property_use_code'] = ''
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
        property_info['taxing_district'] = ''
        property_info['acreage'] = ''
        property_info['mileage'] = ''
        property_info['homestead_exemption'] = ''
        property_info['homestead_exemption_grant_year'] = ''
        property_info['pool'] = ''

        item['main_info'] = property_info

        '''     saving listing in CSV file      '''
        listing = dict()
        listing['parcel_id'] = response.xpath("//td[contains(text(),'Parcel #:')]/span[1]/text()").get('').strip()
        listing['location_address'] = property_address.strip()
        listing['owner1'] = owners[0].strip()
        self.save_to_csv(listing)


        '''     2 - land      '''
        land_info_check = response.css("#MainContent_frmParcelDetail_gvLands tr")[1:]
        if land_info_check:
            LAND_INFO = []
            for lands in response.css("#MainContent_frmParcelDetail_gvLands tr")[1:]:
                land_info = dict()
                land_info['land_use'] = lands.css('td:nth-child(1) ::text').get('').strip()
                vu_string = lands.css('td:nth-child(2) ::text').get('').strip()
                if vu_string:
                    pattern = r"([\d,.]+)\s+(.+)"
                    match = re.match(pattern, vu_string)
                    if match:
                        land_info['num_of_units'] = match.group(1)
                        land_info['unit_type'] = match.group(2)
                else:
                    land_info['num_of_units'], land_info['unit_type'] = '',''
                land_info['frontage'] = ''
                land_info['depth'] = ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []


        '''     3 - buildings      '''
        building_info_check = response.css('#MainContent_frmParcelDetail_gvBldgs')
        if building_info_check:
            BUILDING_INFO_list = []
            building_count = 1
            for buildings in response.css('#MainContent_frmParcelDetail_gvBldgs tr')[1:-1]:
                building_info = dict()
                building_info['id'] = building_count
                building_count += 1
                building_info['real_estate_id'] = ''
                building_info['building_no'] = buildings.css('td:nth-child(1) input::attr(value)').get('').strip()
                bed_bath = (buildings.css('td:nth-child(5) ::text').get('').strip()).split('/')
                building_info['beds'] = int(bed_bath[0])
                building_info['baths'] = int(bed_bath[1])
                building_info['stories'] = ''
                building_info['half_baths'] = ''
                building_info['built_year'] = buildings.css('td:nth-child(3) ::text').get('').strip()
                building_info['ac'] = ''
                building_info['heat'] = ''
                building_info['floor_cover'] = ''
                building_info['frame_type'] = ''
                building_info['interior_walls'] = ''
                building_info['roof_cover'] = ''
                building_info['exterior_walls'] = ''
                base_aux_area = (buildings.css('td:nth-child(4) ::text').get('').strip()).split('/')
                building_info['gross_building_area'] = int(base_aux_area[0]) + int(base_aux_area[1])
                building_info['living_area'] = int(base_aux_area[0])
                BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []


        '''     4 - valuations      '''
        valuation_check = response.css("#MainContent_frmParcelDetail div div:nth-child(1) .float-left+ .float-left .ui-corner-bottom table")
        if valuation_check:
            VALUATION = []
            valuation_count = 1
            for values in response.css("#MainContent_frmParcelDetail div div:nth-child(1) .float-left+ .float-left .ui-corner-bottom table"):
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''
                certified_values['year'] = ''
                certified_values['land'] = values.xpath(".//span[contains(text(),'Land:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
                certified_values['building'] = values.xpath(".//span[contains(text(),'Building:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
                certified_values['extra_feature'] = values.xpath(".//span[contains(text(),'Features:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
                certified_values['just'] = values.xpath(".//span[contains(text(),'Market:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
                certified_values['assessed'] = values.xpath(".//span[contains(text(),'Assessed:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
                certified_values['exemptions'] = values.xpath(".//span[contains(text(),'Exempt:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
                certified_values['taxable'] = values.xpath(".//span[contains(text(),'Taxable:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
                certified_values['cap'] = values.xpath(".//span[contains(text(),'Capped:')]/parent::td[1]/following-sibling::td[1]/span/text()").get('').strip()
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
        item['extra_features'] = []
        extra_features_check = response.xpath("//*[contains(@id,'MainContent_frmParcelDetail_gvFeatures')]")
        if extra_features_check:
            EXTRA_FEATURES_list = []
            x_features_count = 1
            for extra_features in response.css('#MainContent_frmParcelDetail_gvFeatures tr')[1:]:
                extra_f = dict()
                extra_f['id'] = x_features_count
                x_features_count += 1
                extra_f['real_estate_id'] = ''
                extra_f['building_number'] = extra_features.css('td:nth-child(1) ::text').get('').strip()
                extra_f['desc'] = extra_features.css('td:nth-child(2) ::text').get('').strip()
                extra_f['units'] = extra_features.css('td:nth-child(4) span ::text').get('').strip()
                extra_f['unit_type'] = ''
                extra_f['year'] = extra_features.css('td:nth-child(3) ::text').get('').strip()
                EXTRA_FEATURES_list.append(extra_f)
            item['extra_features'] = EXTRA_FEATURES_list
        else:
            item['extra_features'] = []


        '''     6 - transactions      '''
        sales = response.css("#MainContent_frmParcelDetail_gvSales")
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css("#MainContent_frmParcelDetail_gvSales tr")[1:]:
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('td:nth-child(1) ::text').get('').strip()
                book_page = sales.css('td:nth-child(2) ::text').getall()
                sales_info['document_number'] = ''.join(element.strip() for element in book_page)
                sales_info['qualification_code'] = sales.css('td:nth-child(5) ::text').get('').strip()
                sales_info['grantor'] = ''
                sales_info['grantee'] = sales.css('td:nth-child(7) ::text').get('').strip()
                sales_info['document_type'] = sales.css('td:nth-child(3) ::text').get('').strip()
                sales_info['price'] = sales.css('td:nth-child(6) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []

        '''     7 - permits         -      No data available on Website             '''
        item['permits'] = []

        '''     8 - flood_zones     -      No data available on Website             '''
        item['flood_zones'] = []

        yield item
        # print(item)

    def save_to_csv(self, data):
        with open('Output/hernando_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id','location_address','owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)

