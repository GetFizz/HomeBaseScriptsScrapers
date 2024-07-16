#####       Lee

import csv
import json
import re
from copy import deepcopy
import scrapy
from scrapy.utils.response import open_in_browser
from scrapy.selector import Selector
class Lee(scrapy.Spider):
    name = 'Lee'
    prefix = 'https://www.leepa.org'
    postfix = '&historyDetails=True&SalesDetails=True&ElevationDetails=True&PropertyDetailsCurrent=True&PermitDetails=True#PermitDetails'
    url = "https://www.leepa.org/Search/PropertySearch.aspx"
    headers = {
        'authority': 'www.leepa.org',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_ga=GA1.1.253078338.1710265796; ASP.NET_SessionId=wn0g1bvf23w35rbjym42b0ki; ak_bmsc=C0A8651A1BDF7DEF6CF78AF887FBDB6C~000000000000000000000000000000~YAAQyXw2FzJ5LxqOAQAA7xytOBfRi8Q9rZ5oCzqv8bjblp1xGCjJFKYh5tnpvj5XKg3rWucoog1oge9ddMC70uKDk6/5nH59YcJ3O8uBkb5p0YJ4O5A7HwaWnDxNLdTUTMFg0FWUP7kZY+X89VCi/uJJJ6HPs8Haik6CBZKOkYkpkWRW9pm+jZbDIV3e6mYoZ7VD9gMkXP6Bpgm218v6ZbzGGvb/BBBGTpCXCgP4alae0UUy5Zr1PYRxB7SnNvWA6ojfPRdrXFahC9Q80cwJBzVI3a1qOXCHq1aRuoDStec4TE/3oC4NqjGqSGRV9vvl/MO0OCI4CAHJpef9AxTcpyjHxzyaCmLMTiRLUjHJ+7VGsPXTW4u+crry5TFuecOfaB6lvevaGisLkOwHMvXVh7RvaEKHXDrUtDSTIm5u52Kz+PoLi6TdQZsS8yyT8mjcRoc=; RT="z=1&dm=www.leepa.org&si=b04512da-d857-44b7-af79-143e1e54f7f8&ss=ltoqhs3b&sl=1&tt=35y&rl=1&ld=1ahoeq"; _ga_SFZRJ54F6F=GS1.1.1710347849.4.1.1710347937.0.0.0',
        'origin': 'https://www.leepa.org',
        'referer': 'https://www.leepa.org/Search/PropertySearch.aspx',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'x-microsoftajax': 'Delta=true',
        'x-requested-with': 'XMLHttpRequest'
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }
    data = {    #   __EVENTARGUMENT,    __LASTFOCUS,    __VIEWSTATE,    __VIEWSTATEGENERATOR
        "ctl00$ctl07": "ctl00$BodyContentPlaceHolder$PropertySearchUpdatePanel|ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SubmitPropertySearch",
        "__EVENTTARGET": "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SubmitPropertySearch",
        "__EVENTARGUMENT": "",
        "__LASTFOCUS": "",
        "__VIEWSTATEGENERATOR": "42B37C8E",
        # "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$AddressTextBox": "Bedman",
        # "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$AddressTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SearchResultsRepeater$ctl11$pagenumberList: ": '',

        "ctl00_WebExplorerBar2_clientState": "[[[[null,null,null,1,1,350]],[],null],[{},[]],null]",
        "ctl00_WebExplorerBar2_tree_clientState": "[[[[null,null,null,null,0,null,0,1,null,null,null,null,1,null,null,null,1,4,500,null,null,null,null,null,0]],[[[[[\"igeb_Item\",\"igeb_ItemDisabled\",\"igeb_ItemActive\",\"igeb_ItemHover\",null,null,null,null,null,\"igeb_ItemSelected\",null,null,null,null,null,\"igeb_Holder\",\"igeb_GroupContents\",null,null,null,\"igeb_ItemParent\"]],[],[]],[{},[]],null],[[[[null,null,null,null]],[],[]],[{},[]],null]],null],[{},[{}]],[{\"scrollTop\":0,\"scrollLeft\":0},[[],{}]]]",
        "ctl00_MainMenu_clientState": "[[null,[[[null,[],null],[{},[]],null]],null],[{},[{},{}]],null]",
        "ctl00_BodyContentPlaceHolder_WebTab1_clientState": "[[[[null,null,null,null,null,null,null,null]],[],[{\"0\":[[null,null,null,null,null,null,\"Property Information\",null,null,null,null,null]],\"1\":[[null,null,null,null,null,null,\"Deed/Recording Information\",null,null,null,null,null]],\"2\":[[null,null,null,null,null,null,\"Tangible Personal Property Information\",null,null,null,null,null]]}]],[{},[{}]],null]",
        "ctl00_BodyContentPlaceHolder_WebTab1.i": '0',
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl0_STRAPTextBox_clientState": "|0|01\u0013\u0013-\u0013\u0013-\u0013\u0013-\t\u0013-\t\t\t\t\t.\t\t\t\t||[[[[]],[],[]],[{},[]],\"01\u0013\u0013-\u0013\u0013-\u0013\u0013-\t\u0013-\t\t\t\t\t.\t\t\t\t\"]",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl0_STRAPTextBox": "",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl0_FolioTextBox_clientState": "|0|01\u0013\u0013\u0013\u0013\u0013\u0013\u0013\u0013||[[[[]],[],[]],[{},[]],\"01\u0013\u0013\u0013\u0013\u0013\u0013\u0013\u0013\"]",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl0_FolioTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$OwnerNameTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SearchSouceGroup": "SiteRadioButton",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$ZIPCodeTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$CountryDropDownList": "UNITED STATES OF AMERICA",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$LegalTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl1$InstrumentTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl1$BookTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl1$PageTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl1$GrantorTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl1$GranteeTextBox": "",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl2_TangibleSearch_AccountTextBox_clientState": "|0|01\u000f\u000f \u0014\u0014 \u0014\u0014\u0014\u0014-\u0014\u0014||[[[[]],[],[]],[{},[]],\"01\u000f\u000f \u0014\u0014 \u0014\u0014\u0014\u0014-\u0014\u0014\"]",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl2_TangibleSearch_AccountTextBox": "",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl2_TangibleSearch_TanCtrlSTRAPTextBox_clientState": "|0|01\u0013\u0013-\u0013\u0013-\u0013\u0013-\t\u0013-\t\t\t\t\t.\t\t\t\t||[[[[]],[],[]],[{},[]],\"01\u0013\u0013-\u0013\u0013-\u0013\u0013-\t\u0013-\t\t\t\t\t.\t\t\t\t\"]",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl2_TangibleSearch_TanCtrlSTRAPTextBox": "",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl2_TangibleSearch_FolioTextBox_clientState": "|0|01\u0013\u0013\u0013\u0013\u0013\u0013\u0013\u0013||[[[[]],[],[]],[{},[]],\"01\u0013\u0013\u0013\u0013\u0013\u0013\u0013\u0013\"]",
        "ctl00_BodyContentPlaceHolder_WebTab1_tmpl2_TangibleSearch_FolioTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl2$TangibleSearch$BusinessNameTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl2$TangibleSearch$SiteNumberTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl2$TangibleSearch$StreetNameTextBox": "",
        "ctl00$BodyContentPlaceHolder$WebTab1$tmpl2$TangibleSearch$ZIPCodeTextBox": "",
        "ctl00$BodyContentPlaceHolder$hidForModel": "",
        "__ASYNCPOST": "true",
        "__VIEWSTATE": "",
    }

    custom_settings = {
        'FEEDS': {
            'Output/lee_parcel_data.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        },  #### Zyte API

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
    count = 1

    def start_requests(self):
        file_path = "input/lee_address_list.csv"
        addresses = []
        with open(file_path, 'r', newline='', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                address = row['address']
                if address:
                    addresses.append(address)

        for address in addresses:
            print('The search address is : ', address)
            page_no = 0
            yield scrapy.Request(
                url='https://www.leepa.org/Search/PropertySearch.aspx',
                callback=self.parse,  headers=self.headers,
                meta={'search_address': address, 'page_no':page_no}, dont_filter=True,
            )

    def parse(self, response):
        payload = deepcopy(self.data)
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get('')
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get('')
        payload['ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$AddressTextBox'] = response.meta.get('search_address')
        page_no = response.meta.get('page_no')
        payload['ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SearchResultsRepeater$ctl11$pagenumberList'] = str(page_no)

        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, headers=self.headers,
                meta={'search_address': response.meta.get('search_address'),'page_no':page_no, 'payload':payload})


    def parse_pages(self, response):
        html_text = response.text
        selector = Selector(text=html_text)
        search_address = response.meta.get('search_address')

        if selector.xpath("//*[contains(text(),'No results match your search criteria')]"):
            print('No result found for : ', search_address)
            with open('Output/lee_missed_address.csv', 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                if csvfile.tell() == 0:
                    writer.writerow(['address'])
                writer.writerow([search_address])

        else:
            for parcel in selector.xpath("//*[contains(text(),'Parcel Details')]"):
                parcel_url = self.prefix + parcel.css('::attr(href)').get('').strip() + self.postfix
                print(self.count, parcel_url)
                self.count += 1
                yield scrapy.Request(url=parcel_url, callback=self.parse_detail, headers=self.headers,
                                     meta={'search_address': search_address})
            next_page = selector.xpath("//*[contains(@id,'ctl00_BodyContentPlaceHolder_WebTab1_tmpl0_SearchResultsRepeater_ctl11_moveNextImageButton')]")
            '''         PAGINATION          '''
            if next_page:
                payload = response.meta.get('payload')
                page_no = response.meta.get('page_no') + 1
                payload['ctl00$BodyContentPlaceHolder$WebTab1$tmpl0$SearchResultsRepeater$ctl11$pagenumberList'] = str(page_no)

                viewstate_string = selector.xpath("//*[contains(text(),'__VIEWSTATE')]/text()").get('').strip()
                string = response.text
                parts = string.split('|')
                viewstate_generator = ''
                for i in range(len(parts)):
                    if parts[i] == "__VIEWSTATEGENERATOR":
                        viewstate_generator = parts[i + 1]
                        break
                viewstate = ''
                for i in range(len(parts)):
                    if parts[i] == "__VIEWSTATE":
                        viewstate = parts[i + 1]
                        break

                payload['__VIEWSTATE'] = viewstate
                payload['__VIEWSTATEGENERATOR'] = viewstate_generator
                yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse_pages, headers=self.headers,
                    meta={'search_address': response.meta.get('search_address'), 'page_no': page_no, 'payload':payload})


    def parse_detail(self, response):
        item = dict()

        '''     1 - main_info      '''
        property_info = dict()
        parcel_id = re.search(r'FolioID=(\d+)', response.url).group(1)
        property_info['parcel_id'] = parcel_id

        location_add = response.css('#addressHistoryDiv~ .textPanel ::text').getall()
        location_address = ', '.join(element.strip() for element in location_add)  # location_address
        property_info['location_address'] = location_address
        property_info['subdivision'] = ''

        sec = response.xpath("//th[contains(text(),'Section')]/parent::tr[1]/following-sibling::tr[1]/td[3]/text()").get('').strip()
        twp = response.xpath("//th[contains(text(),'Township')]/parent::tr[1]/following-sibling::tr[1]/td[1]/text()").get('').strip()
        rng = response.xpath("//th[contains(text(),'Range')]/parent::tr[1]/following-sibling::tr[1]/td[2]/text()").get('').strip()
        property_info['sec_twp_rng'] = f'{sec}/{twp}/{rng}'

        property_info['census'] = ''
        property_info['property_use_code'] = ''
        property_info['waterfront_code'] = ''
        property_info['municipality'] = response.xpath("//th[contains(text(),'Municipality')]/parent::tr[1]/following-sibling::tr[1]/td[1]/text()").get('').strip()
        property_info['zoning_code'] = ''
        property_info['parcel_desc'] = ''

        legal_description = response.css('.columnLeft~ div+ div .textPanel ::text').getall()
        property_info['legal_desc'] = ' '.join(element.strip() for element in legal_description)  # description

        property_info['neighborhood'] = ''
        property_info['property_id'] = ''
        property_info['millage_group'] = ''
        property_info['property_class'] = ''
        property_info['affordable_housing'] = ''

        info_list = response.css('.textPanel div ::text').getall()
        owner_info = []
        for url in info_list:
            owner_info.append(url.strip())
        property_info['owner1']  = owner_info[0].strip()
        if '+' in owner_info[0]:
            property_info['owner2']  = owner_info[1].strip()
        else:
            property_info['owner2']  = ''
        property_info['owner3']  = ''

        last_non_empty_index = -1
        for i in range(len(owner_info) - 1, -1, -1):
            if owner_info[i] != "":
                last_non_empty_index = i
                break
        property_info['mailing_address_1'] = owner_info[last_non_empty_index-1]
        property_info['mailing_address_2'] = ''
        pattern = r'([A-Z\s]+)\s([A-Z]{2})\s(\d{5})'
        string1 = owner_info[last_non_empty_index]
        match1 = re.match(pattern, string1)
        if match1:
            property_info['mailing_city'] = match1.group(1).strip()
            property_info['mailing_state'] = match1.group(2).strip()
            property_info['mailing_zipcode'] = match1.group(3).strip()
        else:
            property_info['mailing_city'], property_info['mailing_state'], property_info['mailing_zipcode'] = "", "", ""

        property_info['property_address'] = ', '.join(element.strip() for element in location_add)  # location_address
        zip_code_pattern = r'\b\d{5}(?:-\d{4})?\b'  # Regular expression pattern for zip code
        zip_codes = re.findall(zip_code_pattern, location_address)
        if zip_codes:
            property_info['property_zipcode'] = zip_codes[-1]
        else:
            property_info['property_zipcode'] = ''
        property_info['search_address'] = response.meta.get('search_address').replace('%20',' ')

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
        listing['parcel_id'] = parcel_id
        listing['location_address'] = location_address
        listing['owner1'] = owner_info[0].strip()
        self.save_to_csv(listing)


        '''     2 - land      '''
        land_info_check = response.xpath("//*[contains(text(),'Land Tracts')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = [] #'.sectionTitle+ .innerBox .sectionSubTitle+ .appraisalAttributes tr'
            for lands in response.xpath("//*[contains(text(),'Land Tracts')]/parent::tr[1]/parent::table/tr")[2:]:
                land_info = dict()
                land_info['land_use'] = lands.css('td:nth-child(2) ::text').get('').strip()
                land_info['num_of_units'] = lands.css('td:nth-child(3) ::text').get('').strip()
                land_info['unit_type'] = lands.css('td:nth-child(4) ::text').get('').strip()
                land_info['frontage'] = ''
                land_info['depth'] = ''
                LAND_INFO.append(land_info)
            item['land'] = LAND_INFO
        else:
            item['land'] = []

        '''     3 - buildings      '''
        building_info_check = response.xpath("//div[contains(text(),'Buildings')]")
        if building_info_check:
            BUILDING_INFO_list = []
            building_info = dict()
            building_info['id'] = 1
            building_info['real_estate_id'] = ''
            building_info['building_no'] = ''
            bedrooms = response.xpath("//th[contains(text(),'Bedrooms')]/parent::tr[1]/following-sibling::tr[1]/td[1]/text()").getall()
            building_info['beds'] = bedrooms[-1].strip() if bedrooms else ''
            building_info['baths'] = response.xpath("//th[contains(text(),'Bathrooms')]/parent::tr[1]/following-sibling::tr[1]/td[2]/text()").get('').strip()
            building_info['stories'] = response.xpath("//th[contains(text(),'Stories')]/parent::tr[1]/following-sibling::tr[1]/td[3]/text()").get('').strip()
            building_info['half_baths'] = ''
            building_info['built_year'] = response.xpath("//th[contains(text(),'Year Built')]/parent::tr[1]/following-sibling::tr[1]/td[3]/text()").get('').strip()
            building_info['ac'] = response.xpath("//th[contains(text(),'Heated / Under Air')]/parent::tr[1]/following-sibling::tr[1]/td[2]/text()").get('').strip()
            building_info['heat'] = response.xpath("//th[contains(text(),'Heated / Under Air')]/parent::tr[1]/following-sibling::tr[1]/td[2]/text()").get('').strip()
            building_info['floor_cover'] = ''
            building_info['frame_type'] = ''
            building_info['interior_walls'] = ''
            building_info['roof_cover'] = ''
            building_info['exterior_walls'] = ''
            building_info['gross_building_area'] = response.xpath("//th[contains(text(),'Area (Sq Ft)')]/parent::tr[1]/following-sibling::tr[1]/td[3]/text()").get('').strip()
            building_info['living_area'] = response.xpath("//th[contains(text(),'Area (Sq Ft)')]/parent::tr[1]/following-sibling::tr[1]/td[3]/text()").get('').strip()
            BUILDING_INFO_list.append(building_info)
            item['buildings'] = BUILDING_INFO_list
        else:
            item['buildings'] = []


        '''     4 - valuations      '''
        valuation_check = response.xpath("//*[contains(text(),'Property Values / Exemptions / TRIM Notices')]")
        if valuation_check:
            VALUATION = []
            valuation_count = 1
            for values in response.css("#valueGrid tr")[1:]:
                certified_values = dict()
                certified_values['id'] = valuation_count
                certified_values['real_estate_id'] = ''
                certified_values['year'] = values.css("td:nth-child(2) ::text").get('').strip()
                certified_values['land'] = values.css('td:nth-child(4) ::text').get('').strip()
                certified_values['building'] = ''
                certified_values['extra_feature'] = ''
                certified_values['just'] = values.css('td:nth-child(3) ::text').get('').strip()
                certified_values['assessed'] = values.css('td:nth-child(5) ::text').get('').strip()
                certified_values['exemptions'] = values.css('td:nth-child(7) ::text').get('').strip()
                certified_values['taxable'] = values.css('td:nth-child(9) ::text').get('').strip()
                certified_values['cap'] = values.css('td:nth-child(6) ::text').get('').strip()
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


        '''     6 - transactions      '''
        sales = response.xpath("//*[contains(text(),'Sales / Transactions')]/text()").get('').strip()
        if sales:
            SALES_LIST = []
            sales_count = 1
            for sales in response.css('#SalesDetails table tr')[1:-2]:
                sales_info = dict()
                sales_info['id'] = sales_count
                sales_count += 1
                sales_info['real_estate_id'] = ''
                sales_info['transfer_date'] = sales.css('td:nth-child(2) ::text').get('').strip()
                sales_info['document_number'] = sales.css('td:nth-child(3) a ::text').get('').strip()
                sales_info['qualification_code'] = ''
                sales_info['grantor'] = ''
                sales_info['grantee'] = ''
                sales_info['document_type'] = ''
                sales_info['price'] = sales.css('td:nth-child(1) ::text').get('').strip()
                SALES_LIST.append(sales_info)
            item['transactions'] = SALES_LIST
        else:
            item['transactions'] = []


        '''     7 - permits       '''
        permits = response.xpath("//*[contains(text(),'Building / Construction Permit Data')]/text()").get('').strip()
        if permits:
            PERMITS_LIST = []
            for permit in response.css('#PermitDetails table tr')[1:]:
                permit_info = dict()
                permit_info['application'] = permit.css('td:nth-child(1) ::text').get('').strip()
                permit_info['property_type'] = ''
                permit_info['property_owner'] = ''
                permit_info['application_date'] = ''
                permit_info['valuation'] = ''
                permit_info['parcel_id'] = ''
                permit_info['subcontractor'] = ''
                permit_info['contractor'] = ''
                permit_info['permit_type'] = permit.css('td:nth-child(2) ::text').get('').strip()
                permit_info['issue_date'] = permit.css('td:nth-child(3) ::text').get('').strip()
                PERMITS_LIST.append(permit_info)
            item['permits'] = PERMITS_LIST
        else:
            item['permits'] = []


        '''     8 - flood_zones    '''
        no_flood_zones = response.xpath("//*[contains(text(),'Flood Insurance Rate Map data is not available for parcels within a municipality.')]/text()").get('')
        if no_flood_zones:
            item['flood_zones'] = []
        else:
            FLOOD_INFO = []
            flood_count = 1
            for floods in response.css('#ElevationDetails tr')[2:]:
                flood_info = dict()
                flood_info['id'] = flood_count
                flood_count += 1
                flood_info['real_estate_id'] = ''
                flood_info['firm_panel'] = floods.css('td:nth-child(2) ::text').get('').strip()
                flood_info['floodway'] = ''
                flood_info['sfha'] = ''
                flood_info['zone'] = floods.css('td:nth-child(5) ::text').get('').strip()
                flood_info['community'] = floods.css('td:nth-child(1) ::text').get('').strip()
                flood_info['base_flood_elevation'] = ''
                flood_info['cfha'] = ''
                FLOOD_INFO.append(flood_info)
            item['flood_zones'] = FLOOD_INFO

        # print(item)
        yield item


    def save_to_csv(self, data):
        with open('Output/lee_parcel_list.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parcel_id', 'location_address', 'owner1']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if csvfile.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        pass
