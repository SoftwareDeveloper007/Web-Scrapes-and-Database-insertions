## Importing list of needed Python modules
import psycopg2
import signal
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import threading

from datetime import date, datetime, timedelta
# import datetime
import re
import time
import sys, os
from Common.Common import *
from Common.Parser_input import *

## MTU(s) Times  except UK
mtusList = ["00:00 - 01:00", "01:00 - 02:00", "02:00 - 03:00", "03:00 - 04:00", "04:00 - 05:00", "05:00 - 06:00",
            "06:00 - 07:00", "07:00 - 08:00", "08:00 - 09:00", "09:00 - 10:00", "10:00 - 11:00", "11:00 - 12:00",
            "12:00 - 13:00", "13:00 - 14:00", "14:00 - 15:00", "15:00 - 16:00", "16:00 - 17:00", "17:00 - 18:00",
            "18:00 - 19:00", "19:00 - 20:00", "20:00 - 21:00", "21:00 - 22:00", "22:00 - 23:00", "23:00 - 00:00",
            "10New Line25"]

## MTU times For UK
UKmtusList = ["00:00 - 00:30", "00:30 - 01:00", "01:00 - 01:30", "01:30 - 02:00", "02:00 - 02:30", "02:30 - 03:00",
              "03:00 - 03:30", "03:30 - 04:00", "04:00 - 04:30", "04:30 - 05:00", "05:00 - 05:30", "05:30 - 06:00",
              "06:00 - 06:30", "06:30 - 07:00", "07:00 - 07:30", "07:30 - 08:00", "08:00 - 08:30", "08:30 - 09:00",
              "09:00 - 09:30", "09:30 - 10:00", "10:00 - 10:30", "10:30 - 11:00", "11:00 - 11:30", "11:30 - 12:00",
              "12:00 - 12:30", "12:30 - 13:00", "13:00 - 13:30", "13:30 - 14:00", "14:00 - 14:30", "14:30 - 15:00",
              "15:00 - 15:30", "15:30 - 16:00", "16:00 - 16:30", "16:30 - 17:00", "17:00 - 17:30", "17:30 - 18:00",
              "18:00 - 18:30", "18:30 - 19:00", "19:00 - 19:30", "19:30 - 20:00", "20:00 - 20:30", "20:30 - 21:00",
              "21:00 - 21:30", "21:30 - 22:00", "22:00 - 22:30", "22:30 - 23:00", "23:00 - 23:30", "23:30 - 00:00",
              "10New Line25"]


class log_printer():
    def __init__(self):
        self.curDate = date.today()
        curDateStr = date2str(self.curDate, '.', 0)
        try:
            self.logFile = open("Log_File_" + curDateStr + ".txt", "w+")
            logTxt = "Log file created successfully!!!\n"
            print(logTxt)
        except:
            logTxt = "It failed to create log file\n"
            print(logTxt)
            exit(1)

    def print_log(self, logTxt):
        self.logFile.write(logTxt + '\n')
        self.logFile.flush()
        print(logTxt + '\n')

    def close_log(self):
        self.logFile.close()


class main_scraper():
    def __init__(self):

        self.curDate = date.today()
        curDateStr = date2str(self.curDate, '.', 0)

        print('\n')

        self.log_printer = log_printer()

        self.input_parser = parser_input()

        if self.input_parser.period_valid == False:
            logTxt = "Please give 'From' date  & 'To' dates\n" + \
                     "'input_date.txt' file required valid inputs, Please give inputs like below format\n" + \
                     "**************************\n" + \
                     "From:28.02.2017\n" + \
                     "To:02.03.2017\n" + \
                     "New Line (Note: Empty New Line here)\n" + \
                     "**************************"
            self.log_printer.print_log(logTxt)
            exit(1)
        else:
            self.start_date = self.input_parser.period['start_date']
            self.end_date = self.input_parser.period['end_date']

        if self.input_parser.area_name_value_valid == False:
            logTxt = "Please give 'Areas/Countries_Values\n" + \
                     "'Areas_Countries_Values.csv' file required valid inputs, Please give inputs like below format\n" + \
                     "******************************************\n" + \
                     "Area/Country,Value\n" + \
                     "Belarus (BY),CTY|BY!BZN|10Y1001A1001A51S\n" + \
                     "******************************************"
            self.log_printer.print_log(logTxt)
            exit(1)
        else:
            self.area_name_value = self.input_parser.area_name_value

        self.total_data = []
        self.total_exception = []

        self.database = database_manager(self.log_printer, self.total_data)
        self.total_ind = 0

        if self.database.database_valid == True and self.database.table_valid == True:
            self.start_scraping()
            self.database.insert_data()
        else:
            exit(1)

    def start_scraping(self):

        logTxt = "\n------------------- Scraping Started!!! ------------------------"
        self.log_printer.print_log(logTxt)

        self.start_url = "https://transparency.entsoe.eu/generation/r2/actualGenerationPerGenerationUnit/show"

        '''
        logTxt = "\nScript started on " + date2str(self.curDate, '.', 0) + \
              "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + \
              "\nInput Areas/Countries_Values: \n" + '\n'.join([row['area_name'] + ', ' + row['area_value'] for i, row in enumerate(self.area_name_value)]) + \
              "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + \
              "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + \
              "\nInput Dates\nFrom: " + date2str(self.start_date, '.', 0) + \
              "\nTo: " + date2str(self.end_date, '.', 0) + \
              "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + \
              "\nScraping URL: " + self.start_url

        self.log_printer.print_log(logTxt)
        '''

        date_generated = [self.start_date + timedelta(days=x)
                          for x in range(0, (self.end_date - self.start_date).days + 1)]

        self.total_url_date = []

        for date in date_generated:
            print(date)
            scraping_date = date2str(date, '.', 1)
            convertedDate = date2str(date, '-', 0)
            dt = datetime.strptime(scraping_date, '%d.%m.%Y')

            areas_url = "https://transparency.entsoe.eu/generation/r2/actualGenerationPerGenerationUnit/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=" + \
                        scraping_date + \
                        "+00:00|CET|DAYTIMERANGE&dateTime.endDateTime=" + \
                        scraping_date + \
                        "+00:00|CET|DAYTIMERANGE&area.values="
            # print(areas_url+'\n')

            for cunt, areaValue in enumerate(self.area_name_value):
                cur_area = areas_url + areaValue[
                    'area_value'] + "&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)&dv-datatable_length=50"
                self.total_url_date.append({
                    'url': cur_area,
                    'scraping_date': scraping_date,
                    'convertedDate': convertedDate
                })
                # print(cur_area)

        # print(len(self.total_urls))
        #self.total_url_date.reverse()
        self.total_multithreading()

        # logTxt = '\n------------------- Exception Started!!! ------------------------'
        # self.log_printer.print_log(logTxt)
        # self.handle_exception_multithreading()
        # self.logFile.close()

    def scraping_one_url(self):

        url_date = self.total_url_date.pop()
        url = url_date['url']

        scraping_date = url_date['scraping_date']
        convertedDate = url_date['convertedDate']
        dt = datetime.strptime(scraping_date, '%d.%m.%Y')
        # driver = webdriver.Chrome('/home/ubuntu/Entsoe2/WebDriver/chromedriver')
        # driver = webdriver.Chrome(os.getcwd() + '/WebDriver/chromedriver')
        #driver = webdriver.Chrome()


        headers = {'Accept': '*/*',
                   'Accept-Encoding': 'gzip, deflate, sdch',
                   'Accept-Language': 'en-US,en;q=0.8',
                   'Cache-Control': 'max-age=0',
                   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'}

        des_cap = dict(DesiredCapabilities.PHANTOMJS)

        for key in headers:
            des_cap['phantomjs.page.customHeaders.{}'.format(key)] = headers[key]

        # driver = webdriver.Firefox("/home/ubuntu/Entsoe2")
        # driver = webdriver.Firefox()
        # driver = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true','--ssl-protocol=any'], desired_capabilities=des_cap)
        # driver = webdriver.PhantomJS(os.getcwd() + '/WebDriver/phantomjs', service_args=['--ignore-ssl-errors=true','--ssl-protocol=any'], desired_capabilities=des_cap)

        options = webdriver.ChromeOptions()
        options.add_argument('--ignore-certificate-errors')
        options.add_argument("--test-type")
        options.add_argument('--start-maximized')
        options.add_argument('--disable-java')
        options.add_argument('--incognito')
        options.add_argument('--use-mock-keychain')
        options.add_argument('--headless')

        driver = webdriver.Chrome(os.getcwd() + '/WebDriver/chromedriver', chrome_options=options)

        # driver = webdriver.PhantomJS(os.getcwd() + '/WebDriver/phantomjs')
        # driver = webdriver.Remote(desired_capabilities=DesiredCapabilities.HTMLUNIT)
        # driver = webdriver.Remote(desired_capabilities=DesiredCapabilities.HTMLUNITWITHJS)
        # driver.maximize_window()

        start_url = 'https://transparency.entsoe.eu/generation/r2/actualGenerationPerGenerationUnit/show'

        #driver.get(start_url)
        driver.get(url)
        logTxt = "\nStep1: Load Page and Click 'Close Button'"
        self.log_printer.print_log(logTxt)
        try:
            clost_btn = WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.ID, "close-button")))
            clost_btn.click()
            logTxt = "\t\tClicked 'Close Button' successfully"
            self.log_printer.print_log(logTxt)
            print('2OK----------------------------------------------------')


        except Exception as e:
            logTxt = "\t\tWarning Handler1: 'Close Button' can't be found in the first page!!!\n"
            self.log_printer.print_log(logTxt)
            # driver.quit()

            self.total_url_date = [{
                'url': url,
                'scraping_date': scraping_date,
                'convertedDate': convertedDate}] + self.total_url_date
            # return

            print('3OK')

        # iters_per_page_btn =  WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH, "//div[@id='dv-datatable_length']/a[@value='10']")))
        # iters_per_page_btn.click()

        #driver.get(url)
        #driver.implicitly_wait(10)
        #self.log_printer.print_log(driver.current_url)
        #self.log_printer.print_log(url)

        try:
            area_country = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "#dv-market-areas-content > div.dv-filter-hierarchic-wrapper.border > div.dv-filter-checkbox.dv-filter-checkbox-selected")))
            area_country = area_country.text.strip()
        except Exception as e:
            logTxt = "\t\tWarning Handler2: 'Area Country' can't be found\n"
            self.log_printer.print_log(logTxt)
            driver.quit()
            self.total_url_date = [{
                'url': url,
                'scraping_date': scraping_date,
                'convertedDate': convertedDate}] + self.total_url_date
            return

        try:
            bidding_zone = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.XPATH, "//*[@id=\"dv-datatable\"]/thead/tr[1]/th/span")))
            bidding_zone = bidding_zone.text
            bidding_zone = bidding_zone.strip()
        except Exception as e:
            logTxt = "\t\tWarning Handler3: 'Bidding Zone' can't be found\n"
            self.log_printer.print_log(logTxt)
            driver.quit()
            self.total_url_date = [{
                'url': url,
                'scraping_date': scraping_date,
                'convertedDate': convertedDate}] + self.total_url_date
            return

        logTxt = "\t\t**************************************************************************************************\n" + \
                 "\t\tInput Date: " + scraping_date + "\tBidding Area_Country: " + area_country + "\tBidding Zone: " + bidding_zone
        self.log_printer.print_log(logTxt)

        totalRecords = 0
        pageCnt = 0
        change_page = True

        # Navigating on pager
        while (change_page):

            pageCnt += 1
            logTxt = "\tPage Cnt: " + str(pageCnt)
            self.log_printer.print_log(logTxt)

            data_rows = []
            try:
                data_rows = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr")))
                # WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.ID, "dv-datatable")))
                # data_rows = driver.find_elements_by_xpath("//*[@id=\"dv-datatable\"]/tbody/tr")
            except Exception as e:
                logTxt = "\t\tWarning Handler4: 'Page{}' can't be found\n".format(pageCnt)
                self.log_printer.print_log(logTxt)
                driver.quit()
                self.total_exception.append({
                    'exception': 'Page Error',
                    'url': url,
                    'page': pageCnt,
                    'scraping_date': scraping_date,
                    'convertedDate': convertedDate
                })
                return

            totalDts = len(data_rows)

            if totalDts == 0:
                logTxt = "\tThis page has no data"
                self.log_printer.print_log(logTxt)
                driver.quit()
                return

            tabRowInt = 0
            # logFile.write("\ntotal rows: " + str(totalDts))
            # consoleprint("total rows: " + str(totalDts))
            for tabRowInt in range(totalDts):
                # logFile.write("\ncurrent row: " + str(tabRowInt))
                # consoleprint("current row: " + str(tabRowInt))

                try:
                    type = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                        (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[1]")))
                    type = type.text.strip()
                except Exception as e:
                    logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                        tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                             "\t\tWarning Handler5: 'Type' in Row{}, Page{}' can't be found\n".format(tabRowInt + 1,
                                                                                                      pageCnt)
                    self.log_printer.print_log(logTxt)

                    self.total_exception.append({
                        'exception': 'Row Error',
                        'url': url,
                        'page': pageCnt,
                        'row': tabRowInt,
                        'scraping_date': scraping_date,
                        'convertedDate': convertedDate
                    })
                    continue

                try:
                    generation_unit = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                        (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[2]")))
                    generation_unit = generation_unit.text.strip()
                except Exception as e:
                    logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                        tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                             "\t\tWarning Handler6: 'Generation Unit' in Row{}, Page{}' can't be found\n".format(
                                 tabRowInt + 1, pageCnt, area_country, bidding_zone)
                    self.log_printer.print_log(logTxt)
                    self.total_exception.append({
                        'exception': 'Row Error',
                        'url': url,
                        'page': pageCnt,
                        'row': tabRowInt,
                        'scraping_date': scraping_date,
                        'convertedDate': convertedDate
                    })
                    continue

                generation = 0
                consumption = 0

                try:
                    # clicking on expand button
                    detail_mtu_Bt = WebDriverWait(driver, 50).until(EC.element_to_be_clickable(
                        (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[5]/a")))
                    detail_mtu_Bt.click()
                except Exception as e:
                    logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                        tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                             "\t\tWarning Handler7: 'More Button' in Row{}, Page{}' can't be found\n".format(
                                 tabRowInt + 1, pageCnt, area_country, bidding_zone)
                    self.log_printer.print_log(logTxt)

                    try:
                        close_btn = WebDriverWait(driver, 50).until(EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, "div.ui-dialog-buttonset > Button")
                        ))
                        close_btn.click()

                    except Exception as e:
                        pass

                    self.total_exception.append({
                        'exception': 'Row Error',
                        'url': url,
                        'page': pageCnt,
                        'row': tabRowInt,
                        'scraping_date': scraping_date,
                        'convertedDate': convertedDate
                    })
                    continue

                try:
                    WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.LINK_TEXT, "50")))

                    detail_mtu_rows = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#dv-datatable > tbody > tr.dt-detail-row")))
                    more_rows = detail_mtu_rows.find_element_by_link_text("50")
                    more_rows.click()

                    detail_mtu_rows = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "#dv-datatable > tbody > tr.dt-detail-row")))

                    detail_mtu_rows_vals = detail_mtu_rows.text
                    # print(detail_mtu_rows_vals)
                    detail_mtu_rows_vals_nl = re.sub("\n", 'New Line', detail_mtu_rows_vals)
                    detail_mtu_rows_vals_nl += 'New Line'

                    scrapedMTUCnts = 0
                    MTUCnts = 0
                    genGtZero = 0
                    genZero = 0
                    if area_country == 'United Kingdom (UK)':
                        MTUCnts = len(UKmtusList) - 1
                    else:
                        MTUCnts = len(mtusList) - 1
                    for i in range(MTUCnts):
                        if area_country == 'United Kingdom (UK)':
                            # consoleprint("Inside UK")
                            dt_mtu = UKmtusList[i]
                            # consoleprint(UKmtusList[i]+' (.*)New Line' + UKmtusList[i+1])
                            mtus = re.search(UKmtusList[i] + ' (.*)New Line' + str(UKmtusList[i + 1]),
                                             detail_mtu_rows_vals_nl)
                            if mtus != None:
                                generation_cons = mtus.group(1)
                                tmGen_cons = generation_cons.split(" ")
                                generation = 0
                                consumption = 0
                                if len(tmGen_cons) == 2:
                                    (tgeneration, tconsumption) = generation_cons.split(" ", 1)
                                    generation = tgeneration
                                    consumption = tconsumption
                                else:
                                    generation = tmGen_cons[0]
                                    consumption = 0
                                # consoleprint(str(generation))
                                generation = adjust_Gen_Con(generation)
                                consumption = adjust_Gen_Con(consumption)
                                if generation == 0:
                                    genZero += 1
                                else:
                                    genGtZero += 1
                                generation = str(generation)
                                consumption = str(consumption)

                                self.total_data.append({
                                    'ar_area_country': area_country,
                                    'ar_bidding_zone': bidding_zone,
                                    'ar_type': type,
                                    'ar_generation_unit': generation_unit,
                                    'ar_detail_mtu': dt_mtu,
                                    'ar_generation': generation,
                                    'ar_consumption': consumption,
                                    'ar_date': dt
                                })
                                totalRecords += 1
                                scrapedMTUCnts += 1
                        else:
                            dt_mtu = mtusList[i]
                            # consoleprint(mtusList[i]+' (.*)New Line' + mtusList[i+1])
                            mtus = re.search(mtusList[i] + ' (.*)New Line' + str(mtusList[i + 1]),
                                             detail_mtu_rows_vals_nl)
                            if mtus != None:
                                generation_cons = mtus.group(1)
                                tmGen_cons = generation_cons.split(" ")
                                generation = 0
                                consumption = 0
                                if len(tmGen_cons) == 2:
                                    (tgeneration, tconsumption) = generation_cons.split(" ", 1)
                                    generation = tgeneration
                                    consumption = tconsumption
                                else:
                                    generation = tmGen_cons[0]
                                    consumption = 0
                                # consoleprint(str(generation))
                                generation = adjust_Gen_Con(generation)
                                consumption = adjust_Gen_Con(consumption)
                                if generation == 0:
                                    genZero += 1
                                else:
                                    genGtZero += 1
                                generation = str(generation)
                                consumption = str(consumption)

                                self.total_data.append({
                                    'ar_area_country': area_country,
                                    'ar_bidding_zone': bidding_zone,
                                    'ar_type': type,
                                    'ar_generation_unit': generation_unit,
                                    'ar_detail_mtu': dt_mtu,
                                    'ar_generation': generation,
                                    'ar_consumption': consumption,
                                    'ar_date': dt
                                })
                                totalRecords += 1
                                scrapedMTUCnts += 1

                    self.total_ind += 1

                    logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                        tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                             "\t\t{0} > {1} > {2} > {3}\n".format(area_country, bidding_zone, type, generation_unit) + \
                             "\t\t###############################################################################\n" + \
                             "\t\tTotal MTU(s) : {}".format(MTUCnts) + "\n" + \
                             "\t\tTotal Scraped MTU(s) : {}".format(scrapedMTUCnts) + "\n" + \
                             "\t\tGeneration >0 : {}".format(genGtZero) + "\n" + \
                             "\t\tGeneration =0 : {}".format(genZero) + "\n" + \
                             "\t\tTotal Rows scrapped: {}".format(self.total_ind) + "\n"

                    self.log_printer.print_log(logTxt)

                    detail_mtu_Bt.click()

                except Exception as e:
                    logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                        tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                             "\t\tWarning Handler8: 'Detailed Table' in Row{}, Page{}' can't be found\n".format(
                                 tabRowInt + 1, pageCnt, area_country, bidding_zone)
                    self.log_printer.print_log(logTxt)

                    self.total_exception.append({
                        'exception': 'Row Error',
                        'url': url,
                        'page': pageCnt,
                        'row': tabRowInt,
                        'scraping_date': scraping_date,
                        'convertedDate': convertedDate
                    })
                    continue

            if totalRecords == 0:
                change_page = False
            else:
                try:
                    next_page = WebDriverWait(driver, 50).until(
                        EC.presence_of_all_elements_located((By.ID, "dv-datatable_next-custom")))
                    if len(next_page) == 0:
                        change_page = False
                    else:
                        next_page[0].click()
                except Exception as e:
                    change_page = False

        logTxt = "Total Fetched Records: {}\n".format(totalRecords) + \
                 "**************************************************************************************************\n"

        self.log_printer.print_log(logTxt)

        driver.quit()

    def total_multithreading(self):
        self.threads = []
        self.max_threads = 1

        while self.threads or self.total_url_date:
            for thread in self.threads:
                if not thread.is_alive():
                    self.threads.remove(thread)

            while len(self.threads) < self.max_threads and self.total_url_date:
                thread = threading.Thread(target=self.scraping_one_url)
                thread.setDaemon(True)
                thread.start()
                self.threads.append(thread)

    def handle_exception_multithreading(self):
        self.threads = []
        self.max_threads = 10

        while self.threads or self.total_exception:
            for thread in self.threads:
                if not thread.is_alive():
                    self.threads.remove(thread)

            while len(self.threads) < self.max_threads and self.total_exception:
                thread = threading.Thread(target=self.handle_exception)
                thread.setDaemon(True)
                thread.start()
                self.threads.append(thread)

    def handle_exception(self):
        exception = self.total_exception.pop()
        if exception['exception'] == 'Page Error':
            url = exception['url']
            page = exception['page']
            scraping_date = exception['scraping_date']
            convertedDate = exception['convertedDate']
            self.handle_page_exception(url, page, scraping_date, convertedDate)
        elif exception['exception'] == 'Row Error':
            url = exception['url']
            page = exception['page']
            scraping_date = exception['scraping_date']
            convertedDate = exception['convertedDate']
            row_cnt = exception['row']
            scraping_date = exception['scraping_date']
            convertedDate = exception['convertedDate']
            self.handle_row_exception(url, page, row_cnt, scraping_date, convertedDate)

    def handle_page_exception(self, url, page, scraping_date, convertedDate):

        dt = datetime.strptime(scraping_date, '%d.%m.%Y')
        # driver = webdriver.Chrome(os.getcwd() + '/WebDriver/chromedriver.exe')
        driver = webdriver.Firefox()
        # driver = webdriver.PhantomJS(os.getcwd() + '/WebDriver/phantomjs.exe')
        # driver = webdriver.Remote(desired_capabilities=DesiredCapabilities.HTMLUNIT)
        # driver = webdriver.Remote(desired_capabilities=DesiredCapabilities.HTMLUNITWITHJS)
        # driver = webdriver.PhantomJS()
        # driver = webdriver.Chrome()
        # driver.maximize_window()
        driver.get(url)
        logTxt = "\nStep1: Load Page and Click 'Close Button'"
        self.log_printer.print_log(logTxt)
        try:
            clost_btn = WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.ID, "close-button")))
            logTxt = "\t\tClicked 'Close Button' successfully"
            self.log_printer.print_log(logTxt)
            clost_btn.click()
        except Exception as e:
            logTxt = "\t\tWarning Handler1: 'Close Button' can't be found in the first page!!!\n" + \
                     "\t\t(Page Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        # iters_per_page_btn =  WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH, "//div[@id='dv-datatable_length']/a[@value='10']")))
        # iters_per_page_btn.click()
        driver.implicitly_wait(10)
        try:
            area_country = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "#dv-market-areas-content > div.dv-filter-hierarchic-wrapper.border > div.dv-filter-checkbox.dv-filter-checkbox-selected")))
            area_country = area_country.text
            area_country = area_country.strip()
        except Exception as e:
            logTxt = "\t\tWarning Handler2: 'Area Country' can't be found\n" + \
                     "\t\t(Page Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        try:
            bidding_zone = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.XPATH, "//*[@id=\"dv-datatable\"]/thead/tr[1]/th/span")))
            bidding_zone = bidding_zone.text
            bidding_zone = bidding_zone.strip()
        except Exception as e:
            logTxt = "\t\tWarning Handler3: 'Bidding Zone' can't be found\n" + \
                     "\t\t(Page Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        logTxt = "\t\t**************************************************************************************************\n" + \
                 "\t\tInput Date: " + scraping_date + "\tBidding Area_Country: " + area_country + "\tBidding Zone: " + bidding_zone
        self.log_printer.print_log(logTxt)

        totalRecords = 0
        pageCnt = page

        # Navigating on pager

        pageCnt += 1
        logTxt = "\tPage Cnt: " + str(pageCnt)
        self.log_printer.print_log(logTxt)

        if pageCnt <= 5:
            try:
                page_btn = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[@id='dv-datatable_paginate-custom']/span/a")))
                page_btn[pageCnt].click()
            except:
                logTxt = "\t\tWarning Handler4: 'Page{}' can't be found\n".format(pageCnt) + \
                         "\t\t(Page Exception)"
                self.log_printer.print_log(logTxt)
                driver.quit()
                return
        else:
            try:
                page_btn = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[@id='dv-datatable_paginate-custom']/a")))
                page_btn[-1].click()
                page_btn = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[@id='dv-datatable_paginate-custom']/span/a")))
                for btn in page_btn:
                    if btn.text == str(pageCnt):
                        btn.click()
            except:
                logTxt = "\t\tWarning Handler4: 'Page{}' can't be found\n".format(pageCnt) + \
                         "\t\t(Page Exception)"
                self.log_printer.print_log(logTxt)
                driver.quit()
                return

        data_rows = []
        try:
            data_rows = WebDriverWait(driver, 50).until(
                EC.presence_of_all_elements_located((By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr")))
            # WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.ID, "dv-datatable")))
            # data_rows = driver.find_elements_by_xpath("//*[@id=\"dv-datatable\"]/tbody/tr")
        except Exception as e:
            logTxt = "\t\tWarning Handler4: 'Page{}' can't be found\n".format(pageCnt) + \
                     "\t\t(Page Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        totalDts = len(data_rows)

        if totalDts == 0:
            logTxt = "\tThis page has no data"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        tabRowInt = 0
        # logFile.write("\ntotal rows: " + str(totalDts))
        # consoleprint("total rows: " + str(totalDts))
        for tabRowInt in range(totalDts):
            # logFile.write("\ncurrent row: " + str(tabRowInt))
            # consoleprint("current row: " + str(tabRowInt))

            try:
                type = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[1]")))
                type = type.text.strip()
            except Exception as e:
                logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                    tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\tWarning Handler5: 'Type' in Row{}, Page{}' can't be found\n".format(tabRowInt + 1,
                                                                                                  pageCnt) + \
                         "\t\t(Page Exception)"
                self.log_printer.print_log(logTxt)
                continue

            try:
                generation_unit = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                    (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[2]")))
                generation_unit = generation_unit.text.strip()
            except Exception as e:
                logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                    tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\tWarning Handler6: 'Generation Unit' in Row{}, Page{}' can't be found\n".format(
                             tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\t(Page Exception)"
                self.log_printer.print_log(logTxt)
                continue

            generation = 0
            consumption = 0

            try:
                # clicking on expand button
                detail_mtu_Bt = WebDriverWait(driver, 50).until(EC.element_to_be_clickable(
                    (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[5]/a")))
                detail_mtu_Bt.click()
            except Exception as e:
                logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                    tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\tWarning Handler7: 'More Button' in Row{}, Page{}' can't be found\n".format(
                             tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\t(Page Exception)"
                self.log_printer.print_log(logTxt)

                try:
                    close_btn = WebDriverWait(driver, 50).until(EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "div.ui-dialog-buttonset > Button")
                    ))
                    close_btn.click()

                except Exception as e:
                    pass

                continue

            try:
                WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.LINK_TEXT, "50")))

                detail_mtu_rows = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#dv-datatable > tbody > tr.dt-detail-row")))
                more_rows = detail_mtu_rows.find_element_by_link_text("50")
                more_rows.click()

                WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.LINK_TEXT, "10")))
                detail_mtu_rows = driver.find_element_by_css_selector(
                    "#dv-datatable > tbody > tr.dt-detail-row ")
                detail_mtu_rows_vals = detail_mtu_rows.text
                # print(detail_mtu_rows_vals)
                detail_mtu_rows_vals_nl = re.sub("\n", 'New Line', detail_mtu_rows_vals)
                detail_mtu_rows_vals_nl += 'New Line'

                scrapedMTUCnts = 0
                MTUCnts = 0
                genGtZero = 0
                genZero = 0
                if area_country == 'United Kingdom (UK)':
                    MTUCnts = len(UKmtusList) - 1
                else:
                    MTUCnts = len(mtusList) - 1
                for i in range(MTUCnts):
                    if area_country == 'United Kingdom (UK)':
                        # consoleprint("Inside UK")
                        dt_mtu = UKmtusList[i]
                        # consoleprint(UKmtusList[i]+' (.*)New Line' + UKmtusList[i+1])
                        mtus = re.search(UKmtusList[i] + ' (.*)New Line' + str(UKmtusList[i + 1]),
                                         detail_mtu_rows_vals_nl)
                        if mtus != None:
                            generation_cons = mtus.group(1)
                            tmGen_cons = generation_cons.split(" ")
                            generation = 0
                            consumption = 0
                            if len(tmGen_cons) == 2:
                                (tgeneration, tconsumption) = generation_cons.split(" ", 1)
                                generation = tgeneration
                                consumption = tconsumption
                            else:
                                generation = tmGen_cons[0]
                                consumption = 0
                            # consoleprint(str(generation))
                            generation = adjust_Gen_Con(generation)
                            consumption = adjust_Gen_Con(consumption)
                            if generation == 0:
                                genZero += 1
                            else:
                                genGtZero += 1
                            generation = str(generation)
                            consumption = str(consumption)

                            self.total_data.append({
                                'ar_area_country': area_country,
                                'ar_bidding_zone': bidding_zone,
                                'ar_type': type,
                                'ar_generation_unit': generation_unit,
                                'ar_detail_mtu': dt_mtu,
                                'ar_generation': generation,
                                'ar_consumption': consumption,
                                'ar_date': dt
                            })
                            totalRecords += 1
                            scrapedMTUCnts += 1
                    else:
                        dt_mtu = mtusList[i]
                        # consoleprint(mtusList[i]+' (.*)New Line' + mtusList[i+1])
                        mtus = re.search(mtusList[i] + ' (.*)New Line' + str(mtusList[i + 1]),
                                         detail_mtu_rows_vals_nl)
                        if mtus != None:
                            generation_cons = mtus.group(1)
                            tmGen_cons = generation_cons.split(" ")
                            generation = 0
                            consumption = 0
                            if len(tmGen_cons) == 2:
                                (tgeneration, tconsumption) = generation_cons.split(" ", 1)
                                generation = tgeneration
                                consumption = tconsumption
                            else:
                                generation = tmGen_cons[0]
                                consumption = 0
                            # consoleprint(str(generation))
                            generation = adjust_Gen_Con(generation)
                            consumption = adjust_Gen_Con(consumption)
                            if generation == 0:
                                genZero += 1
                            else:
                                genGtZero += 1
                            generation = str(generation)
                            consumption = str(consumption)

                            self.total_data.append({
                                'ar_area_country': area_country,
                                'ar_bidding_zone': bidding_zone,
                                'ar_type': type,
                                'ar_generation_unit': generation_unit,
                                'ar_detail_mtu': dt_mtu,
                                'ar_generation': generation,
                                'ar_consumption': consumption,
                                'ar_date': dt
                            })
                            totalRecords += 1
                            scrapedMTUCnts += 1

                self.total_ind += 1

                logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                    tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\t{0} > {1} > {2} > {3}\n".format(area_country, bidding_zone, type, generation_unit) + \
                         "\t\t###############################################################################\n" + \
                         "\t\tTotal MTU(s) : {}".format(MTUCnts) + "\n" + \
                         "\t\tTotal Scraped MTU(s) : {}".format(scrapedMTUCnts) + "\n" + \
                         "\t\tGeneration >0 : {}".format(genGtZero) + "\n" + \
                         "\t\tGeneration =0 : {}".format(genZero) + "\n" + \
                         "\t\tTotal Rows scrapped: {}".format(self.total_ind) + "\n"

                self.log_printer.print_log(logTxt)

                detail_mtu_Bt.click()

            except Exception as e:
                logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                    tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\tWarning Handler8: 'Detailed Table' in Row{}, Page{}' can't be found\n".format(
                             tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                         "\t\t(Page Exception)"
                self.log_printer.print_log(logTxt)

                self.total_exception.append({
                    'exception': 'Row Error',
                    'url': url,
                    'page': pageCnt,
                    'row': tabRowInt,
                    'scraping_date': scraping_date,
                    'convertedDate': convertedDate
                })
                continue

        logTxt = "Total Fetched Records: {}\n".format(totalRecords) + \
                 "**************************************************************************************************\n"

        self.log_printer.print_log(logTxt)

        driver.quit()

    def handle_row_exception(self, url, page, row_cnt, scraping_date, convertedDate):

        dt = datetime.strptime(scraping_date, '%d.%m.%Y')
        # driver = webdriver.Chrome(os.getcwd() + '/WebDriver/chromedriver.exe')
        # driver = webdriver.Firefox()
        driver = webdriver.PhantomJS(os.getcwd() + '/WebDriver/phantomjs.exe')
        # driver = webdriver.Remote(desired_capabilities=DesiredCapabilities.HTMLUNIT)
        # driver = webdriver.Remote(desired_capabilities=DesiredCapabilities.HTMLUNITWITHJS)
        # driver = webdriver.PhantomJS()
        # driver.maximize_window()
        driver.get(url)
        logTxt = "\nStep1: Load Page and Click 'Close Button'"
        self.log_printer.print_log(logTxt)
        try:
            clost_btn = WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.ID, "close-button")))
            clost_btn.click()
            logTxt = "\t\tClicked 'Close Button' successfully"
            self.log_printer.print_log(logTxt)

        except Exception as e:
            logTxt = "\t\tWarning Handler1: 'Close Button' can't be found in the first page!!!\n" + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        # iters_per_page_btn =  WebDriverWait(driver, 50).until(EC.element_to_be_clickable((By.XPATH, "//div[@id='dv-datatable_length']/a[@value='10']")))
        # iters_per_page_btn.click()

        try:
            area_country = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR,
                 "#dv-market-areas-content > div.dv-filter-hierarchic-wrapper.border > div.dv-filter-checkbox.dv-filter-checkbox-selected")))
            area_country = area_country.text
            area_country = area_country.strip()
        except Exception as e:
            logTxt = "\t\tWarning Handler2: 'Area Country' can't be found\n" + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        try:
            bidding_zone = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.XPATH, "//*[@id=\"dv-datatable\"]/thead/tr[1]/th/span")))
            bidding_zone = bidding_zone.text
            bidding_zone = bidding_zone.strip()
        except Exception as e:
            logTxt = "\t\tWarning Handler3: 'Bidding Zone' can't be found\n" + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        logTxt = "\t\t**************************************************************************************************\n" + \
                 "\t\tInput Date: " + scraping_date + "\tBidding Area_Country: " + area_country + "\tBidding Zone: " + bidding_zone
        self.log_printer.print_log(logTxt)

        totalRecords = 0
        pageCnt = page

        # Navigating on pager

        pageCnt += 1
        logTxt = "\tPage Cnt: " + str(pageCnt)
        self.log_printer.print_log(logTxt)

        if pageCnt <= 5:
            try:
                page_btn = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[@id='dv-datatable_paginate-custom']/span/a")))
                page_btn[pageCnt].click()
            except:
                logTxt = "\t\tWarning Handler4: 'Page{}' can't be found\n".format(pageCnt) + \
                         "\t\t(Row Exception)"
                self.log_printer.print_log(logTxt)
                driver.quit()
                return
        else:
            try:
                page_btn = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[@id='dv-datatable_paginate-custom']/a")))
                page_btn[-1].click()
                page_btn = WebDriverWait(driver, 50).until(
                    EC.presence_of_all_elements_located((By.XPATH, "//div[@id='dv-datatable_paginate-custom']/span/a")))
                for btn in page_btn:
                    if btn.text == str(pageCnt):
                        btn.click()
            except:
                logTxt = "\t\tWarning Handler4: 'Page{}' can't be found\n".format(pageCnt) + \
                         "\t\t(Row Exception)"
                self.log_printer.print_log(logTxt)
                driver.quit()
                return

        data_rows = []
        try:
            data_rows = WebDriverWait(driver, 50).until(
                EC.presence_of_all_elements_located((By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr")))
            # WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.ID, "dv-datatable")))
            # data_rows = driver.find_elements_by_xpath("//*[@id=\"dv-datatable\"]/tbody/tr")
        except Exception as e:
            logTxt = "\t\tWarning Handler4: 'Page{}' can't be found\n".format(pageCnt) + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        totalDts = len(data_rows)

        if totalDts == 0:
            logTxt = "\tThis page has no data"
            self.log_printer.print_log(logTxt)
            driver.quit()
            return

        tabRowInt = row_cnt
        # logFile.write("\ntotal rows: " + str(totalDts))
        # consoleprint("total rows: " + str(totalDts))

        # logFile.write("\ncurrent row: " + str(tabRowInt))
        # consoleprint("current row: " + str(tabRowInt))

        try:
            type = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[1]")))
            type = type.text.strip()
        except Exception as e:
            logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\tWarning Handler5: 'Type' in Row{}, Page{}' can't be found\n".format(tabRowInt + 1, pageCnt) + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)
            return

        try:
            generation_unit = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[2]")))
            generation_unit = generation_unit.text.strip()
        except Exception as e:
            logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\tWarning Handler6: 'Generation Unit' in Row{}, Page{}' can't be found\n".format(
                         tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)
            return

        generation = 0
        consumption = 0

        try:
            # clicking on expand button
            detail_mtu_Bt = WebDriverWait(driver, 50).until(EC.element_to_be_clickable(
                (By.XPATH, "//*[@id=\"dv-datatable\"]/tbody/tr[" + str(tabRowInt + 1) + "]/td[5]/a")))
            detail_mtu_Bt.click()
        except Exception as e:
            logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\tWarning Handler7: 'More Button' in Row{}, Page{}' can't be found\n".format(
                         tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)

            try:
                close_btn = WebDriverWait(driver, 50).until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "div.ui-dialog-buttonset > Button")
                ))
                close_btn.click()

            except Exception as e:
                pass

            return

        try:
            WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.LINK_TEXT, "50")))

            detail_mtu_rows = WebDriverWait(driver, 50).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#dv-datatable > tbody > tr.dt-detail-row")))
            more_rows = detail_mtu_rows.find_element_by_link_text("50")
            more_rows.click()

            WebDriverWait(driver, 50).until(EC.presence_of_element_located((By.LINK_TEXT, "10")))
            detail_mtu_rows = driver.find_element_by_css_selector(
                "#dv-datatable > tbody > tr.dt-detail-row ")
            detail_mtu_rows_vals = detail_mtu_rows.text
            # print(detail_mtu_rows_vals)
            detail_mtu_rows_vals_nl = re.sub("\n", 'New Line', detail_mtu_rows_vals)
            detail_mtu_rows_vals_nl += 'New Line'

            scrapedMTUCnts = 0
            MTUCnts = 0
            genGtZero = 0
            genZero = 0
            if area_country == 'United Kingdom (UK)':
                MTUCnts = len(UKmtusList) - 1
            else:
                MTUCnts = len(mtusList) - 1
            for i in range(MTUCnts):
                if area_country == 'United Kingdom (UK)':
                    # consoleprint("Inside UK")
                    dt_mtu = UKmtusList[i]
                    # consoleprint(UKmtusList[i]+' (.*)New Line' + UKmtusList[i+1])
                    mtus = re.search(UKmtusList[i] + ' (.*)New Line' + str(UKmtusList[i + 1]),
                                     detail_mtu_rows_vals_nl)
                    if mtus != None:
                        generation_cons = mtus.group(1)
                        tmGen_cons = generation_cons.split(" ")
                        generation = 0
                        consumption = 0
                        if len(tmGen_cons) == 2:
                            (tgeneration, tconsumption) = generation_cons.split(" ", 1)
                            generation = tgeneration
                            consumption = tconsumption
                        else:
                            generation = tmGen_cons[0]
                            consumption = 0
                        # consoleprint(str(generation))
                        generation = adjust_Gen_Con(generation)
                        consumption = adjust_Gen_Con(consumption)
                        if generation == 0:
                            genZero += 1
                        else:
                            genGtZero += 1
                        generation = str(generation)
                        consumption = str(consumption)

                        self.total_data.append({
                            'ar_area_country': area_country,
                            'ar_bidding_zone': bidding_zone,
                            'ar_type': type,
                            'ar_generation_unit': generation_unit,
                            'ar_detail_mtu': dt_mtu,
                            'ar_generation': generation,
                            'ar_consumption': consumption,
                            'ar_date': dt
                        })
                        totalRecords += 1
                        scrapedMTUCnts += 1
                else:
                    dt_mtu = mtusList[i]
                    # consoleprint(mtusList[i]+' (.*)New Line' + mtusList[i+1])
                    mtus = re.search(mtusList[i] + ' (.*)New Line' + str(mtusList[i + 1]),
                                     detail_mtu_rows_vals_nl)
                    if mtus != None:
                        generation_cons = mtus.group(1)
                        tmGen_cons = generation_cons.split(" ")
                        generation = 0
                        consumption = 0
                        if len(tmGen_cons) == 2:
                            (tgeneration, tconsumption) = generation_cons.split(" ", 1)
                            generation = tgeneration
                            consumption = tconsumption
                        else:
                            generation = tmGen_cons[0]
                            consumption = 0
                        # consoleprint(str(generation))
                        generation = adjust_Gen_Con(generation)
                        consumption = adjust_Gen_Con(consumption)
                        if generation == 0:
                            genZero += 1
                        else:
                            genGtZero += 1
                        generation = str(generation)
                        consumption = str(consumption)

                        self.total_data.append({
                            'ar_area_country': area_country,
                            'ar_bidding_zone': bidding_zone,
                            'ar_type': type,
                            'ar_generation_unit': generation_unit,
                            'ar_detail_mtu': dt_mtu,
                            'ar_generation': generation,
                            'ar_consumption': consumption,
                            'ar_date': dt
                        })
                        totalRecords += 1
                        scrapedMTUCnts += 1

            self.total_ind += 1

            logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\t{0} > {1} > {2} > {3}\n".format(area_country, bidding_zone, type, generation_unit) + \
                     "\t\t###############################################################################\n" + \
                     "\t\tTotal MTU(s) : {}".format(MTUCnts) + "\n" + \
                     "\t\tTotal Scraped MTU(s) : {}".format(scrapedMTUCnts) + "\n" + \
                     "\t\tGeneration >0 : {}".format(genGtZero) + "\n" + \
                     "\t\tGeneration =0 : {}".format(genZero) + "\n" + \
                     "\t\tTotal Rows scrapped: {}".format(self.total_ind) + "\n"

            self.log_printer.print_log(logTxt)

            detail_mtu_Bt.click()

        except Exception as e:
            logTxt = "\t\t################\tRow{},\tPage{}, \t{}, \t{}\t###################\n".format(
                tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\tWarning Handler8: 'Detailed Table' in Row{}, Page{}' can't be found\n".format(
                         tabRowInt + 1, pageCnt, area_country, bidding_zone) + \
                     "\t\t(Row Exception)"
            self.log_printer.print_log(logTxt)

            return

        logTxt = "Total Fetched Records: {}\n".format(totalRecords) + \
                 "**************************************************************************************************\n"

        self.log_printer.print_log(logTxt)

        driver.quit()


class database_manager():
    def __init__(self, log_printer, total_data):
        self.log_printer = log_printer
        self.queue = total_data
        self.database = "pointconnect"
        self.user = "pointconnect"
        self.password = "*J8WFuq%YZrcE8"
        self.host = "52.208.45.157"
        self.port = "5432"

        self.database_valid = False
        self.open_database()
        self.create_table()

    def open_database(self):
        try:
            self.conn = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.database_valid = True
            logTxt = "Opened database successfully"
            self.log_printer.print_log(logTxt)
        except Exception as e:
            self.database_valid = False
            logTxt = "Failed to open database"
            self.log_printer.print_log(logTxt)

    def create_table(self):
        try:
            cur = self.conn.cursor()
            cur.execute("select * from information_schema.tables where table_name=%s", ('entsoeunits',))
            table_flag = bool(cur.rowcount)
            if cur.rowcount == 0:
                logTxt = "'entsoeunits' table not available in 'pointconnect' database\n" + \
                         "Creating new table 'entsoeunits'"
                self.log_printer.print_log(logTxt)
                cur.execute(
                    "CREATE TABLE entsoeunits (area_country VARCHAR(255),bidding_zone VARCHAR(255),type VARCHAR(255),generation_unit VARCHAR(255),generation FLOAT(50),consumption FLOAT(50),detail_mtu VARCHAR(255),day_and_time DATE)")
                self.conn.commit()

            self.conn.close()
            self.table_valid = True
        except:
            self.table_valid = False

    def insert_data(self):
        threading.Timer(10, self.insert_data).start()
        # conn = psycopg2.connect(database="local_pointconnect", user="postgres", password="admin", host="127.0.0.1", port="5432")
        conn = psycopg2.connect(database="pointconnect", user="pointconnect", password="*J8WFuq%YZrcE8",
                                host="52.208.45.157", port="5432")

        cur = conn.cursor()
        while self.queue:
            element = self.queue.pop()
            try:
                cur.execute(
                    "INSERT INTO entsoeunits (area_country,bidding_zone,type,generation_unit,generation,consumption,detail_mtu,day_and_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                    (element['ar_area_country'], element['ar_bidding_zone'], element['ar_type'],
                     element['ar_generation_unit'], element['ar_generation'],
                     element['ar_consumption'], element['ar_detail_mtu'], element['ar_date']))
                conn.commit()
            except:
                pass


if __name__ == '__main__':
    start_time = time.time()
    app = main_scraper()
    elapsed_time = time.time() - start_time

    print('Total Run time: ', elapsed_time)
