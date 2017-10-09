import re, csv, datetime, sys, psycopg2
from datetime import datetime, timedelta, date

path = "/home/ubuntu/Entsoe2/Data/"
class parser_input():
    def __init__(self, csv_file = path + "Areas_Countries_Values.csv"):

        self.csv_file = csv_file

        self.period = ''
        self.period_valid = False

        self.area_name_value = []
        self.area_name_value_valid = False

        self.parsing()

    def parsing(self):
        try:
            conn = psycopg2.connect(database="pointconnect", user="pointconnect", password="*J8WFuq%YZrcE8",
                                    host="52.208.45.157", port="5432")

            print("Opened database successfully for getting last updated date in 'enstounit' table")

            cur = conn.cursor()
            cur.execute("SELECT * from covalis1.entsoeunits order by day_and_time desc limit 1")
            rows = cur.fetchall()
            conn.close()

            # getting start and end dates ranges
            for row in rows:
                last_updated_date = row[7]

            from_date = last_updated_date - timedelta(days=1)
            from_date = "{:%d.%m.%Y}".format(from_date).strip()

            to_date = datetime.today() - timedelta(days=4)
            to_date = "{:%d.%m.%Y}".format(to_date).strip()

            from_date = datetime.strptime(from_date, "%d.%m.%Y")
            to_date = datetime.strptime(to_date, "%d.%m.%Y")

            self.period = {
                'start_date': from_date,
                'end_date': to_date
            }
            self.period_valid = True
            #print(self.period)
            #input_data_file.close()

        except Exception as e:
            self.period_valid = False
            print("------------------------------------------------------------------------\n"
                  "\tError: Errors happened in database'\t\n"
                  "------------------------------------------------------------------------\n")

        try:
            areas_countries_file = open(self.csv_file, "r")
            csv_reader = csv.reader(areas_countries_file)
            for line in csv_reader:
                self.area_name_value.append({
                    'area_name': line[0].strip(),
                    'area_value': line[1].strip()
                })
            self.area_name_value_valid = True
            #print(self.area_name_value)
            areas_countries_file.close()

        except:
            self.area_name_value_valid = False
            print("------------------------------------------------------------------------\n"
                  "\tError: There is no 'Areas_Countries_Values.csv'\t\n"
                  "------------------------------------------------------------------------\n")



if __name__ == '__main__':
    app = parser_input()
