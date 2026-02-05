from .WildFireLoader import WildFireLoader

NFS_WildFireFields = {
    "UnitId": "nwcg_unitid",
}

NFS_WildFireFields_DTYPE = {
    "OBJECTID" : str,
    "SOURCE_SYSTEM_TYPE": str,
    "SOURCE_SYSTEM": str,
    "NWCG_REPORTING_AGENCY" : str,
    "NWCG_REPORTING_UNIT_ID" : str,
    "NWCG_REPORTING_UNIT_NAME" : str,
    "SOURCE_REPORTING_UNIT": str,
    "SOURCE_REPORTING_UNIT_NAME" : str,
    "LOCAL_FIRE_REPORT_ID" : str,
    "LOCAL_INCIDENT_ID" : str,
    "COMPLEX_NAME" : str,
    "OWNER_DESCR" : str,

    "FOD_ID" : str,
    "FPA_ID": str,
    "FIRE_CODE": str,
    "FIRE_NAME": str,
    "ICS_209_PLUS_INCIDENT_JOIN_ID": str,
    "ICS_209_PLUS_COMPLEX_JOIN_ID": str,
    "MTBS_ID": str,
    "MTBS_FIRE_NAME": str,
    "FIRE_YEAR": int,
    "DISCOVERY_DATE": str,
    "DISCOVERY_DOY": str,
    "DISCOVERY_TIME": str,
    "NWCG_CAUSE_CLASSIFICATION": str,
    "NWCG_GENERAL_CAUSE": str,
    "NWCG_CAUSE_AGE_CATEGORY": str,
    "CONT_DATE": str,
    "CONT_DOY": str,
    "CONT_TIME": str,
    "FIRE_SIZE": str,
    "FIRE_SIZE_CLASS": str,
    "LATITUDE": float,
    "LONGITUDE": float,
    "STATE": str,
    "COUNTY": str,
    "FIPS_CODE": str,
    "FIPS_NAME": str,
    "UnitId": str,
    "GeographicArea": str,
    "Gacc": str,
}

NFS_WildFireUnnecessary = [
"OBJECTID",
"SOURCE_SYSTEM_TYPE",
"SOURCE_SYSTEM",
"ICS_209_PLUS_INCIDENT_JOIN_ID",
"ICS_209_PLUS_COMPLEX_JOIN_ID",
"SOURCE_SYSTEM_TYPE",
"SOURCE_SYSTEM",
"NWCG_REPORTING_AGENCY",
"NWCG_REPORTING_UNIT_ID",
"NWCG_REPORTING_UNIT_NAME",
"NWCG_CAUSE_AGE_CATEGORY",
"NWCG_CAUSE_CLASSIFICATION", 
"NWCG_GENERAL_CAUSE",
"CONT_TIME",
"DISCOVERY_TIME",
"SOURCE_REPORTING_UNIT",
"SOURCE_REPORTING_UNIT_NAME",
"LOCAL_FIRE_REPORT_ID",
"LOCAL_INCIDENT_ID",
"COMPLEX_NAME",
"OWNER_DESCR"
]



class NFS_WildFireLoader(WildFireLoader):
    def __init__(self, path: str, **kwargs) -> None:
        self.path = path
        self.kwargs = kwargs
        self.skip = NFS_WildFireUnnecessary
        self.fields = NFS_WildFireFields
        self.dtypes = NFS_WildFireFields_DTYPE
