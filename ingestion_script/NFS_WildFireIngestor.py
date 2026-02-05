import sys
import pymongo
import os
import datetime

from ingestor.NFS_WildFireLoader import NFS_WildFireLoader

from .Ingestor import (Ingestor, add_geometry)

from typing import Optional
from pymongo import IndexModel
from pymongo import ASCENDING

COLLECTION = "zephyr_nfs_wildfire"

OUT_DIR = "./wildfire_data"

DEFAULT_SERVER = "mongodb://localhost:27018"
DEFAULT_DB = "sustaindb"

username = os.getenv("ROOT_MONGO_USER")
password = os.getenv("ROOT_MONGO_PASS")

class NFS_WildFireIngestor(Ingestor):
    disc_doy = "discovery_doy"
    cont_doy = "cont_doy"
    year = "fire_year"

    def calc_day(self, day, year):
        return datetime.datetime.strftime((datetime.datetime(year, 1, 1) + datetime.timedelta(day - 1)), "%Y-%m-%d")

    def convert_mdy(self, document: dict, disc_date: str, cont_date: str) -> None:
        disc_day = int(document[self.disc_doy])
        cont_day = document[self.cont_doy]
        if (float(cont_day) != float(cont_day)):
            cont_day = 1
            document[self.cont_doy] = "1"
        else:
            cont_day = int(cont_day)
        year = int(document[self.year])

        if isinstance(disc_day, int) and isinstance(year, int) and isinstance(cont_day, int):
            start_date = self.calc_day(disc_day, year)
            end_date = self.calc_day(cont_day, year)
            if end_date < start_date:
                end_date = self.calc_day(cont_day, year + 1)
            
            document[disc_date] = start_date
            document[cont_date] = end_date


    def add_GISJoins(self, document: dict, fips_key: str, gis_state_key: str, gis_county_key: str):
        fips_value = document[fips_key]
        if (isinstance(fips_value, str)):
            gis_state = f"G{fips_value[0: 2]}0"
            document[gis_state_key] = gis_state
            document[gis_county_key] = f"{gis_state}{fips_value[2:]}0"
        else:
            del document[fips_key]


    def preprocess_document(self, document: dict) -> Optional[dict]:
        self.convert_mdy(document, "discovery_date", "cont_date")
        self.add_GISJoins(document, "fips_code", "gis_state", "gis_county")
        add_geometry(document)
        return document
    
    def get_indexes(self) -> list[IndexModel]:
        base_index_fields = [("fod_id", ASCENDING), ("discovery_date", ASCENDING), ("cont_date", ASCENDING)]
        return [IndexModel(base_index_fields)]


    def ingestData(self, file: str, collection: str):
        nrows = 1 if (collection is not None) else None
        ingestor = NFS_WildFireIngestor()
        loader = NFS_WildFireLoader(file, nrows=nrows)
        if collection is not None:
            #ingestor.create_indexes(collection)
            ingestor.ingest(loader, collection)
        else:
            ingestor.print_sample(loader)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("AAAAAH STOP!! Usage:", sys.argv[0], "FILE COLLECTION(optional)")
        exit

    file = sys.argv[1]
    if len(sys.argv) >= 3:
        collection = sys.argv[2]
    else:
        collection = None

    client = pymongo.MongoClient(DEFAULT_SERVER, username=username, password=password)

    if collection != None:
        collection = client[DEFAULT_DB][collection]


    wf_ingest = NFS_WildFireIngestor()
    wf_ingest.ingestData(file, collection)

    print("Success!!")
