from urllib.request import urlopen
from bs4 import BeautifulSoup
import psycopg2
import requests
import json
import time

from zip_codes import STATES


def getCdcHtmlForZip(zipCode):
    page = urlopen(
        "https://gettested.cdc.gov/search_results?location=" + zipCode)
    soup = BeautifulSoup(page, "html.parser")
    saveHtml(soup.prettify())
    return soup


def saveHtml(htmlString):
    newFile = open("latest_cdc.html", "w")
    newFile.write(htmlString)
    newFile.close()


def getCdcJsonForZip(zipCode):
    url = "https://npin.cdc.gov/api/organization/gettested/json/42.35310972588549--43.80517412180078/-90.3550107863868---88.36637550457212"
    params = dict(
        zip=zipCode
    )
    resp = requests.get(url=url, params=params)
    return resp.json()
    # newFile = open("latest_cdc.json", "w")
    # newFile.write(json.dumps(resp.json()))
    # newFile.close()


def getListOfZipsForMinMax(min, max):
    zips = []
    for x in range(min, max):
        zips.append(x)

    return zips


def extractSiteDataFromFeatures(features):
    listOfSites = []
    for feature in features["features"]:
        properties = feature["properties"]
        nameHtml = properties["name"]
        nameSoup = BeautifulSoup(nameHtml, "html.parser")
        names = extractNames(nameSoup)

        listOfSites.append({
            "nid": properties["nid"],
            "group_name": names["groupName"],
            "site_name": names["siteName"],
            "address": properties["description"],
            "services": properties["gsl_feature_filter_list_rendered"],
            "phone_number": properties["gsl_props_phone_rendered"],
            "fees": properties["fees"],
            "href": nameSoup.select_one("a").attrs["href"],
        })
    return listOfSites


def extractNames(nameSoup):
    fullName = nameSoup.get_text()
    names = fullName.split('|')
    return {
        "groupName": names[0],
        "siteName": names[1]
    }


def getAllSiteData():
    zips = []
    for state in STATES:
        zips = getListOfZipsForMinMax(state["min"], state["max"])

    for zip in zips:
        features = getCdcJsonForZip(str(zip))
        extractSiteDataFromFeatures(features)
        time.sleep(5)


def getSiteDataByZip(zipCode):
    features = getCdcJsonForZip(str(zipCode))
    siteProperties = extractSiteDataFromFeatures(features)
    for site in siteProperties:
        writeTestSiteToDb(site)
    newFile = open("latest_cdc_parsed.json", "w")
    newFile.write(json.dumps(siteProperties))
    newFile.close()


def writeTestSiteToDb(testSite):
    sql = """INSERT INTO testing_sites(nid, group_name, site_name, address, services, phone_number, fees, href)
             VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(nid)
                DO UPDATE SET (group_name, site_name, address, services, phone_number, fees, href) 
                    = (EXCLUDED.group_name, EXCLUDED.site_name, EXCLUDED.address, EXCLUDED.services, EXCLUDED.phone_number, EXCLUDED.fees, EXCLUDED.href);"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="my_db",
            user="root",
            password="root"
        )
        cur = conn.cursor()
        print('connected')
        cur.execute(sql, (int(testSite["nid"]), str(testSite["group_name"]), str(testSite["site_name"]), str(testSite["address"]),
                    str(testSite["services"]), str(testSite["phone_number"]), str(testSite["fees"]), str(testSite["href"])))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Postgres error: ", error)
    finally:
        if conn is not None:
            conn.close()


def main():
    getSiteDataByZip(53703)


if __name__ == "__main__":
    main()
