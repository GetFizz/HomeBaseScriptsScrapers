#####           QPublic
import csv
from copy import deepcopy
import scrapy


class QPublic(scrapy.Spider):
    custom_settings = {
        'FEEDS': {
            'outputs/Details_JSON.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf-8',
            },
        },
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000
        },
        "REQUEST_FINGERPRINTER_CLASS": "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "ZYTE_API_KEY": "adb3f0b810654938aaca08557e87ccf1",  # Please enter your API Key here
        "ZYTE_API_TRANSPARENT_MODE": True,
        "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
    }
    name = 'QPublic'
    prefix = 'https://qpublic.schneidercorp.com'
    url = 'https://qpublic.schneidercorp.com/Application.aspx?App=LibertyCountyFL&Layer=Parcels&PageType=Search'
    cookies = {
        'ASP.NET_SessionId': 'tdieeuxfbd3r23ymapyvr305',
        '_ga': 'GA1.1.853350627.1705588653',
        'cf_clearance': 'fbTSEnKYCtZ8PjXJmK8MoA2VH5tPsl.ODyvKxkb9QWk-1705588654-1-AWcG7Gvr92+1BZEG0JA6PoMZ+VaWiehgXgMeRimfOjmDaJ0AEN51F/iKGAXLSwvYUBa044Cfa+9n/KlvCpn++sY=',
        '_ga_7ZQ1FTE1SG': 'GS1.1.1705588652.1.1.1705588815.0.0.0',
    }
    headers = {
        'authority': 'qpublic.schneidercorp.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded',
        # 'cookie': 'ASP.NET_SessionId=tdieeuxfbd3r23ymapyvr305; _ga=GA1.1.853350627.1705588653; cf_clearance=fbTSEnKYCtZ8PjXJmK8MoA2VH5tPsl.ODyvKxkb9QWk-1705588654-1-AWcG7Gvr92+1BZEG0JA6PoMZ+VaWiehgXgMeRimfOjmDaJ0AEN51F/iKGAXLSwvYUBa044Cfa+9n/KlvCpn++sY=; _ga_7ZQ1FTE1SG=GS1.1.1705588652.1.1.1705588815.0.0.0',
        'origin': 'https://qpublic.schneidercorp.com',
        'pragma': 'no-cache',
        'referer': 'https://qpublic.schneidercorp.com/Application.aspx?App=LibertyCountyFL&Layer=Parcels&PageType=Search',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        # 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    params = {
        'App': 'LibertyCountyFL',
        'Layer': 'Parcels',
        'PageType': 'Search',
    }
    data = {
        '__EVENTTARGET': 'ctlBodyPane$ctl01$ctl01$btnSearch',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': '45skeiW7lq8I3higEBIJ1cfG2iGet0RidnKkzlk2+njsO9NVlxTcm5JbdiVfN4FTRmhLmFCnlJpdMmwEe9qZ8kF/+lxn0Lj0AMZLREkY4bdFfcsPPjxdsaUnkULGxdabXqSamnVYPqb17Dc8TSdoRomAMfDz1Z5/Rib6e27vB566rv3EeUdI1bTSRLovELZX47pi2Znh9d2EIVj4qxGyLVmEUVhM0VppzFiv/TvR0yagzE++',
        '__VIEWSTATEGENERATOR': '569DB96F',
        'ctlBodyPane$ctl00$ctl01$txtName': '',
        'ctlBodyPane$ctl00$ctl01$txtNameExact': '',
        'ctlBodyPane$ctl01$ctl01$txtAddress': '',
        'ctlBodyPane$ctl01$ctl01$txtAddressExact': '',
        'ctlBodyPane$ctl02$ctl01$txtParcelID': '',
        'ctlBodyPane$ctl03$ctl01$txtName': '',
    }
    ser = 1
    page_no = 0

    def start_requests(self):
        for address in list(csv.DictReader(open('inputs/input_address.csv', 'r', encoding='utf-8'))):
            yield scrapy.Request(
                url='https://qpublic.schneidercorp.com/Application.aspx?App=LibertyCountyFL&Layer=Parcels&PageType=Search',
                callback=self.parse,
                headers=self.headers,
                meta={'address': address},
                dont_filter=True,
            )

    def parse(self, response):
        payload = deepcopy(self.data)
        payload['__VIEWSTATE'] = response.css('#__VIEWSTATE::attr(value)').get()
        payload['__VIEWSTATEGENERATOR'] = response.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        payload['ctlBodyPane$ctl01$ctl01$txtAddress'] = response.meta['address']['address']
        yield scrapy.FormRequest(url=self.url, formdata=payload, headers=self.headers, method='POST',
                                 callback=self.parse_pages)

    def parse_pages(self, response):
        for row_div in response.css('#ctlBodyPane_ctl00_ctl01_gvwParcelResults>tbody>tr'):
            item1 = dict()
            item1['Parcel ID'] = row_div.css('.normal-font-label ::text').get('').strip()
            item1['Owner'] = ','.join([owner.strip() for owner in row_div.css('td:nth-child(3) *::text').getall() if owner.strip()])
            item1['Property Address'] = row_div.css('td:nth-child(4) ::text').get('').strip()
            item1['Homestead'] = row_div.css('td:nth-child(5) ::text').get('').strip()
            item1['Last Sale'] = row_div.css('td:nth-child(6) ::text').get('').strip()
            item1['Legal Description'] = row_div.css('td:nth-child(7) ::text').get('').strip()
            item1['Map'] = response.urljoin(row_div.css('td:nth-child(8)>a ::attr(href)').get(''))
            self.save_to_csv(item1)
            yield scrapy.Request(response.urljoin(row_div.css('.normal-font-label ::attr(href)').get()), callback=self.parse_detail, headers=self.headers,
                                 meta={'itemp': item1})

    def parse_detail(self, response):
        item = dict()
        itemp = response.meta.get('itemp')
        download_pdf_parcel_id = itemp['Parcel ID']
        itemp['Brief Tax Description'] = response.xpath(
            "//*[contains(text(),'Brief Tax Description')]/parent::td/following-sibling::td/div/span/text()").get(
            '').strip()
        itemp['Property Use Code'] = response.xpath(
            "//*[contains(text(),'Property Use Code')]/parent::td/following-sibling::td/div/span/text()").get(
            '').strip()
        itemp['Sec/Twp/Rng'] = response.xpath(
            "//*[contains(text(),'Sec/Twp/Rng')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        itemp['Tax District'] = response.xpath(
            "//*[contains(text(),'Tax District')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        itemp['Millage Rate'] = response.xpath(
            "//*[contains(text(),'Millage Rate')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        itemp['Acreage'] = response.xpath(
            "//*[contains(text(),'Acreage')]/parent::td/following-sibling::td/div/span/text()").get('').strip()
        item['Parcel Information'] = itemp

        ### Owner Information
        owner_info = dict()
        if response.css(
                '#ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed_lblSearch ::text').get(
                ''):
            owner_info['Name'] = response.css(
                '#ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed_lblSearch ::text').get(
                '').strip()
        elif response.css(
                '#ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed_lnkSearch ::text').get(
                ''):
            owner_info['Name'] = response.css(
                '#ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_sprOwnerName1_lnkUpmSearchLinkSuppressed_lnkSearch ::text').get(
                '').strip()
        owner_info['Address'] = ''
        add = response.css('#ctlBodyPane_ctl01_ctl01_rptOwner_ctl00_lblOwnerAddress ::text').getall()
        address = []
        for a in add:
            address.append(a.strip())
        owner_info['Address'] = ', '.join(address)
        item['Owner Information'] = owner_info

        ### Land Information
        land_info_check = response.xpath("//*[contains(text(),'Land Information')]/text()").get('').strip()
        if land_info_check:
            LAND_INFO = []
            land_use = response.css('#ctlBodyPane_ctl02_ctl01_grdLand_grdFlat tbody th ::text').getall()
            no_of_units = response.css('#ctlBodyPane_ctl02_ctl01_grdLand_grdFlat th+ td ::text').getall()
            unit_type = response.css('#ctlBodyPane_ctl02_ctl01_grdLand_grdFlat td:nth-child(3) ::text').getall()
            frontage = response.css('#ctlBodyPane_ctl02_ctl01_grdLand_grdFlat td~ td+ td ::text').getall()
            l_i = 1
            for l_u, n_o_u, u_t, f in zip(land_use, no_of_units, unit_type, frontage):
                land_info = dict()
                land_info['Land#'] = l_i
                l_i += 1
                land_info['Land Use'] = l_u.strip()
                land_info['No of Units'] = n_o_u.strip()
                land_info['Unit Type'] = u_t.strip()
                land_info['Frontage'] = f.strip()
                LAND_INFO.append(land_info)
            item['Land Information'] = LAND_INFO

        ### Building Info
        building_info_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl03_lblName')]/text()").get('').strip()
        if building_info_check:
            BUILDING_INFO_list = []
            total_area = response.xpath(
                "//*[contains(text(),'Total Area')]/parent::td/following-sibling::td/div/span/text()").getall()
            exterior_walls = response.xpath(
                "//*[contains(text(),'Exterior Walls')]/parent::td/following-sibling::td/div/span/text()").getall()
            roof_cover = response.xpath(
                "//*[contains(text(),'Roof Cover')]/parent::td/following-sibling::td/div/span/text()").getall()
            interior_walls = response.xpath(
                "//*[contains(text(),'Interior Walls')]/parent::td/following-sibling::td/div/span/text()").getall()
            frame_type = response.xpath(
                "//*[contains(text(),'Frame Type')]/parent::td/following-sibling::td/div/span/text()").getall()
            floor_cover = response.xpath(
                "//*[contains(text(),'Floor Cover')]/parent::td/following-sibling::td/div/span/text()").getall()
            heat = response.xpath(
                "//*[contains(text(),'Heat')]/parent::td/following-sibling::td/div/span/text()").getall()
            air_conditioning = response.xpath(
                "//*[contains(text(),'Air Conditioning')]/parent::td/following-sibling::td/div/span/text()").getall()
            bathrooms = response.xpath(
                "//*[contains(text(),'Bathrooms')]/parent::td/following-sibling::td/div/span/text()").getall()
            bedrooms = response.xpath(
                "//*[contains(text(),'Bedrooms')]/parent::td/following-sibling::td/div/span/text()").getall()
            stories = response.xpath(
                "//*[contains(text(),'Stories')]/parent::td/following-sibling::td/div/span/text()").getall()
            actual_year_built = response.xpath(
                "//*[contains(text(),'Actual Year Built')]/parent::td/following-sibling::td/div/span/text()").getall()
            effective_year_built = response.xpath(
                "//*[contains(text(),'Effective Year Built')]/parent::td/following-sibling::td/div/span/text()").getall()
            b_i = 1
            for ta, ew, rc, iw, ft, fc, h, ac, bt, bd, s, ayb, eyb in zip(total_area, exterior_walls, roof_cover,
                                                                          interior_walls, frame_type,
                                                                          floor_cover, heat, air_conditioning,
                                                                          bathrooms, bedrooms, stories,
                                                                          actual_year_built, effective_year_built):
                building_info = dict()
                building_info['Building#'] = b_i
                b_i += 1
                building_info['Total Area'] = ta.strip()
                building_info['Exterior Walls'] = ew.strip()
                building_info['Roof Cover'] = rc.strip()
                building_info['Interior Walls'] = iw.strip()
                building_info['Frame Type'] = ft.strip()
                building_info['Floor Cover'] = fc.strip()
                building_info['Heat'] = h.strip()
                building_info['Air Conditioning'] = ac.strip()
                building_info['Bathrooms'] = bt.strip()
                building_info['Bedrooms'] = bd.strip()
                building_info['Stories'] = s.strip()
                building_info['Actual Year Built'] = ayb.strip()
                building_info['Effective Year Built'] = eyb.strip()
                BUILDING_INFO_list.append(building_info)
            item['Building Information'] = BUILDING_INFO_list

        ### Sales
        sales = response.xpath("//*[contains(@id,'ctlBodyPane_ctl05_lblName')]/text()").get('').strip()
        if sales:
            SALES_list = []
            multi_parcel = response.css('#ctlBodyPane_ctl05_ctl01_grdSales tbody th ::text').getall()
            sale_date = response.css('#ctlBodyPane_ctl05_ctl01_grdSales th+ td ::text').getall()
            sale_price = response.css('#ctlBodyPane_ctl05_ctl01_grdSales td:nth-child(3) ::text').getall()
            instrument = response.css('#ctlBodyPane_ctl05_ctl01_grdSales td:nth-child(4) ::text').getall()
            book_page = response.css('#ctlBodyPane_ctl05_ctl01_grdSales a ::text').getall()
            qualification = response.css('#ctlBodyPane_ctl05_ctl01_grdSales td:nth-child(6) ::text').getall()
            vacant_improved = response.css('#ctlBodyPane_ctl05_ctl01_grdSales td:nth-child(7) ::text').getall()
            grantor = response.css('td:nth-child(8) span::text').getall()
            grantee = response.css('td:nth-child(9) span::text').getall()
            s_i = 1
            for mp, sd, sp, it, bp, qa, vi, gr, ge in zip(multi_parcel, sale_date, sale_price, instrument, book_page,
                                                          qualification,
                                                          vacant_improved, grantor, grantee):
                sales_info = dict()
                sales_info['Sale#'] = s_i
                s_i += 1
                sales_info['Multi Parcel'] = mp.strip()
                sales_info['Sale Date'] = sd.strip()
                sales_info['Sale Price'] = sp.strip()
                sales_info['Instrument'] = it.strip()
                sales_info['Book/Page'] = bp.strip()
                sales_info['Qualification'] = qa.strip()
                sales_info['Vacant/Improved'] = vi.strip()
                sales_info['Grantor'] = gr.strip()
                sales_info['Grantee'] = ge.strip()
                SALES_list.append(sales_info)
            item['Sales'] = SALES_list

        ## Extra Features
        extra_features_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl04_lblName')]/text()").get('').strip()
        if extra_features_check:
            EXTRA_FEATURES_list = []
            code = response.css('#ctlBodyPane_ctl04_ctl01_grdSales_grdFlat tbody th ::text').getall()
            description = response.css('#ctlBodyPane_ctl04_ctl01_grdSales_grdFlat th+ td ::text').getall()
            length_width = response.css('#ctlBodyPane_ctl04_ctl01_grdSales_grdFlat td:nth-child(3) ::text').getall()
            area = response.css('#ctlBodyPane_ctl04_ctl01_grdSales_grdFlat td:nth-child(4) ::text').getall()
            eff_year_built = response.css('#ctlBodyPane_ctl04_ctl01_grdSales_grdFlat td:nth-child(5) ::text').getall()
            for co, de, lw, ar, ey in zip(code, description, length_width, area, eff_year_built):
                extra_features = dict()
                extra_features['Code'] = co.strip()
                extra_features['Description'] = de.strip()
                extra_features['Length x Width'] = lw.strip()
                extra_features['Area'] = ar.strip()
                extra_features['Effective Year Built'] = ey.strip()
                EXTRA_FEATURES_list.append(extra_features)
            item['Extra Features'] = EXTRA_FEATURES_list

        ### Valuation
        valuation_check = response.xpath("//*[contains(@id,'ctlBodyPane_ctl07_lblName')]/text()").get('').strip()
        if valuation_check:
            VALUATION = []
            building_value = response.css('#ctlBodyPane_ctl07_ctl01_grdValuation th+ td ::text').getall()
            extra_features_value = response.css('tr:nth-child(2) .value-column ::text').getall()
            land_value = response.css('tr:nth-child(3) .value-column ::text').getall()
            land_agricultural_value = response.css('tr:nth-child(4) .value-column ::text').getall()
            agricultural_market_value = response.css('tr:nth-child(5) .value-column ::text').getall()
            just_market_value = response.css('tr:nth-child(6) .value-column ::text').getall()
            assessed_value = response.css('tr:nth-child(7) .value-column ::text').getall()
            exempt_value = response.css('tr:nth-child(8) .value-column ::text').getall()
            taxable_value = response.css('tr:nth-child(9) .value-column ::text').getall()
            max_save = response.css('tr:nth-child(10) .value-column ::text').getall()
            yr = 1
            for yr, bv, efv, lv, lav, amv, jmv, av, ev, tv, ms in zip(range(2023, 2018, -1), building_value,
                                                                      extra_features_value, land_value,
                                                                      land_agricultural_value,
                                                                      agricultural_market_value, just_market_value,
                                                                      assessed_value, exempt_value, taxable_value,
                                                                      max_save):
                valuation = dict()
                valuation['Year'] = yr
                valuation['Building Value'] = bv.strip()
                valuation['Extra Features Value'] = efv.strip()
                valuation['Land Value'] = lv.strip()
                valuation['Land Agricultural Value'] = lav.strip()
                valuation['Agricultural (Market) Value'] = amv.strip()
                valuation['Just (Market) Value'] = jmv.strip()
                valuation['Assessed Value'] = av.strip()
                valuation['Exempt Value'] = ev.strip()
                valuation['Taxable Value'] = tv.strip()
                valuation['Max Save Our Home Portability'] = ms.strip()
                VALUATION.append(valuation)
            item['Valuation'] = VALUATION

        yield item

    def save_to_csv(self, data):
        with open('outputs/Listings.csv', 'a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Parcel ID', 'Owner', 'Property Address', 'Homestead', 'Last Sale', 'Legal Description',
                          'Map']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            if csvfile.tell() == 0:
                writer.writeheader()  # Write header only if the file is empty

            writer.writerow(data)
