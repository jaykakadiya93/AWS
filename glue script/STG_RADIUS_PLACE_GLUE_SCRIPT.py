import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, when, explode, rank, split, array, lit, struct, posexplode
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
import hashlib
from pyspark.sql.types import ArrayType, StringType
import json

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME','s3File','filename'])

seperator = "~"
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
stg_radius_place_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place"
stg_radius_place_assoc_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_assoc"
stg_radius_place_clsd_rsns_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_clsd_rsns"
stg_radius_place_ctgry_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_ctgry"
stg_radius_place_naics_cd_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_naics_cd"
stg_radius_place_sic_cd_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_sic_cd"
stg_radius_place_scndry_nm_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_scndry_nm"
stg_radius_place_eml_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_eml"
stg_radius_place_wbst_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_wbst"
stg_radius_place_xref_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_xref"
stg_radius_place_typ_phn_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_typ_phnnmbr"
stg_radius_place_external_lnk_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_external_link"
stg_radius_place_cstm_attr_output_location = "s3://149676569426-preprod/Export_Json/stg_radius_place_cstm_attr"
#test
#import_from_oracle
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sampledb", table_name = "test", transformation_ctx = "datasource0")

datasource0 = glueContext.create_dynamic_frame.from_options( connection_type = "s3", connection_options = {"paths": [args["s3File"]]}, format = "json", transformation_ctx = "datasource0")
stg_radius_place_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("`mailing_address.dnm`", "boolean", "mlng_addr_dnm", "string"),
                                                                    ("address_key", "string", "address_key", "string"),
                                                                    ("address_key_extended", "string", "addrss_key_extndd", "string"),
                                                                    ("build_id", "string", "build_id", "string"),
                                                                    ("be_id", "string", "be_id", "string"),
                                                                    ("city", "string", "city", "string"),
                                                                    ("closed", "boolean", "closed", "string"),
                                                                    ("location_headcount_int", "int", "location_headcount", "string"),
                                                                    ("location_headcount", "string", "location_headcount_range", "string"),
                                                                    ("location_revenue", "string", "location_revenue_range", "string"),
                                                                    ("location_name_dba", "string", "location_name_dba", "string"),
                                                                    ("location_revenue_int", "int", "location_revenue", "string"),
                                                                    ("`mailing_address.carrier_route`", "string", "mlng_addr_carrier_route_cd", "string"),
                                                                    ("`mailing_address.address_type`", "string", "mlng_addr_type", "string"),
                                                                    ("`mailing_address.city`", "string", "mlng_addr_city", "string"),
                                                                    ("`mailing_address.correction_footnote`", "string", "mlng_addr_crrctn_ftnte", "string"),
                                                                    ("`mailing_address.delivery_point`", "string", "mlng_addr_delivery_point_cd", "string"),
                                                                    ("`mailing_address.dpv_match_code`", "string", "mlng_addr_dpv_match_cd", "string"),
                                                                    ("`mailing_address.is_seasonal`", "boolean", "mlng_addr_is_seasonal", "string"),
                                                                    ("`mailing_address.e_lot`", "string", "mlng_addr_elot_cd", "string"),
                                                                    ("`mailing_address.ncoa_move_date`", "string", "mlng_addr_ncoa_move_date", "string"),
                                                                    ("`mailing_address.state`", "string", "mlng_addr_state", "string"),
                                                                    ("`mailing_address.street`", "string", "mlng_addr_street", "string"),
                                                                    ("`mailing_address.street2`", "string", "mlng_addr_street2", "string"),
                                                                    ("`mailing_address.zip4`", "string", "mlng_addr_zip4", "string"),
                                                                    ("`mailing_address.zip5`", "string", "mlng_addr_zip5", "string"),
                                                                    ("`mailing_address.zip_type`", "string", "mlng_addr_zip_type", "string"),
                                                                    ("name", "string", "name", "string"),
                                                                    ("`physical_address.carrier_route`", "string", "carrier_route_cd", "string"),
                                                                    ("`physical_address.street`", "string", "physical_addr_1", "string"),
                                                                    ("`physical_address.correction_footnote`", "string", "physical_addr_crrctn_ftnte", "string"),
                                                                    ("`mailing_address.is_vacant`", "boolean", "mlng_addr_is_vacant", "string"),
                                                                    ("`physical_address.dpv_match_code`", "string", "physical_addr_dpv_match_cd", "string"),
                                                                    ("`physical_address.street2`", "string", "physical_addr_2", "string"),
                                                                    ("`physical_address.state`", "string", "physical_state", "string"),
                                                                    ("`physical_address.zip4`", "string", "physical_zip4", "string"),
                                                                    ("`physical_address.zip5`", "string", "physical_zip5", "string"),
                                                                    ("`physical_address.address_type`", "string", "physical_address_type", "string"),
                                                                    ("postal_code", "string", "postal_code", "string"),
                                                                    ("postal_code4", "string", "postal_code4", "string"),
                                                                    ("state", "string", "state", "string"),
                                                                    ("address", "string", "cass_cert_address", "string"),
                                                                    ("chain_type", "string", "chain_type", "string"),
                                                                    ("hq", "boolean", "hq", "string"),
                                                                    ("`physical_address.ncoa_move_date`", "string", "ncoa_move_date", "string"),
                                                                    ("location.lat", "double", "latitude", "string"),
                                                                    ("location_name_legal", "string", "location_name_legal", "string"),
                                                                    ("mailable", "string", "mailable", "string"),
                                                                    ("metro", "string", "metro", "string"),
                                                                    ("place_quality_indicator", "double", "place_quality_indicator", "string"),
                                                                    ("soho", "boolean", "soho", "string"),
                                                                    ("twitter_account", "string", "twitter_account", "string"),
                                                                    ("chain_count", "int", "chain_count", "string"),
                                                                    ("`mailing_address.dnm_type`", "string", "mlng_addr_dnm_type", "string"),
                                                                    ("business_start_date", "string", "business_start_date", "string"),
                                                                    ("`physical_address.city`", "string", "physical_city", "string"),
                                                                    ("closed_date", "string", "closed_date", "string"),
                                                                    ("`physical_address.ncoa_move_footnote`", "string", "ncoa_move_footnote", "string"),
                                                                    ("`physical_address.ncoa_move_type`", "string", "ncoa_move_type", "string"),
                                                                    ("`mailing_address.cass_certified`", "boolean", "mlng_addr_cass_certified", "string"),
                                                                    ("`mailing_address.ncoa_move_footnote`", "string", "mlng_addr_ncoa_move_footnote", "string"),
                                                                    ("`mailing_address.ncoa_move_type`", "string", "mlng_addr_ncoa_move_type", "string"),
                                                                    ("`physical_address.is_seasonal`", "boolean", "is_ssnal", "string"),
                                                                    ("`physical_address.dnm_type`", "string", "rds_dnm_typ", "string"),
                                                                    ("location.lon", "double", "longitude", "string"),
                                                                    ("`physical_address.delivery_point`", "string", "dlvry_pnt_cd", "string"),
                                                                    ("`physical_address.dnm`", "boolean", "rds_dnm_ind", "string"),
                                                                    ("`physical_address.is_vacant`", "boolean", "is_vcnt", "string"),
                                                                    ("`physical_address.e_lot`", "string", "elot_cd", "string"),
                                                                    ("`physical_address.zip_type`", "string", "zip_type", "string"),
                                                                    ("`physical_address.dnk`", "string", "rds_dnk_ind", "string"),
                                                                    ("`physical_address.dnk_type`", "string", "rds_dnk_typ", "string"),
                                                                    ("`mailing_address.dnk`", "string", "mlng_addr_dnk", "string"),
                                                                    ("`mailing_address.dnk_type`", "string", "mlng_addr_dnk_type", "string"),
                                                                    ("physical_address.cass_certified", "string", "physical_address_cass_certified", "string"),
                                                                    ("hash_md5_nax", "string", "hash_md5_nax", "string"),
                                                                    ("hash_md5_addr_mstr", "string", "hash_md5_addr_mstr", "string"),
                                                                    ("hash_md5_bldng_mstr", "string", "hash_md5_bldng_mstr", "string"),
                                                                    ("hash_md5_addr_mlng", "string", "hash_md5_addr_mlng", "string"),
                                                                    ("hash_md5_bsnss_mstr", "string", "hash_md5_bsnss_mstr", "string"),
                                                                    ("src_file_name", "string", "src_file_name", "string")], transformation_ctx = "stg_radius_place_applymaping").toDF()


replace_null = stg_radius_place_applymaping.na.fill('N/A')

stg_radius_place_dynamic_frame = DynamicFrame.fromDF(replace_null, glueContext, "stg_radius_place_dynamic_frame")


def hash_calculation_stg_radius_place(dynamicRecord):
    dynamicRecord["mlng_addr_dnm"] = dynamicRecord["mlng_addr_dnm"].upper()
    dynamicRecord["address_key"] = dynamicRecord["address_key"].upper()
    dynamicRecord["addrss_key_extndd"] = dynamicRecord["addrss_key_extndd"].upper()
    dynamicRecord["city"] = dynamicRecord["city"].upper()
    dynamicRecord["closed"] = dynamicRecord["closed"].upper()
    dynamicRecord["location_headcount_range"] = dynamicRecord["location_headcount_range"].upper()
    dynamicRecord["location_revenue_range"] = dynamicRecord["location_revenue_range"].upper()
    dynamicRecord["location_name_dba"] = dynamicRecord["location_name_dba"].upper()
    dynamicRecord["mlng_addr_carrier_route_cd"] = dynamicRecord["mlng_addr_carrier_route_cd"].upper()
    dynamicRecord["mlng_addr_type"] = dynamicRecord["mlng_addr_type"].upper()
    dynamicRecord["mlng_addr_city"] = dynamicRecord["mlng_addr_city"].upper()
    dynamicRecord["mlng_addr_crrctn_ftnte"] = dynamicRecord["mlng_addr_crrctn_ftnte"].upper()
    dynamicRecord["mlng_addr_dpv_match_cd"] = dynamicRecord["mlng_addr_dpv_match_cd"].upper()
    dynamicRecord["mlng_addr_is_seasonal"] = dynamicRecord["mlng_addr_is_seasonal"].upper()
    dynamicRecord["mlng_addr_elot_cd"] = dynamicRecord["mlng_addr_elot_cd"].upper()
    dynamicRecord["mlng_addr_state"] = dynamicRecord["mlng_addr_state"].upper()
    dynamicRecord["mlng_addr_street"] = dynamicRecord["mlng_addr_street"].upper()
    dynamicRecord["mlng_addr_street2"] = dynamicRecord["mlng_addr_street2"].upper()
    dynamicRecord["mlng_addr_zip_type"] = dynamicRecord["mlng_addr_zip_type"].upper()
    dynamicRecord["name"] = dynamicRecord["name"].upper()
    dynamicRecord["carrier_route_cd"] = dynamicRecord["carrier_route_cd"].upper()
    dynamicRecord["physical_addr_1"] = dynamicRecord["physical_addr_1"].upper()
    dynamicRecord["physical_addr_crrctn_ftnte"] = dynamicRecord["physical_addr_crrctn_ftnte"].upper()
    dynamicRecord["mlng_addr_is_vacant"] = dynamicRecord["mlng_addr_is_vacant"].upper()
    dynamicRecord["physical_addr_dpv_match_cd"] = dynamicRecord["physical_addr_dpv_match_cd"].upper()
    dynamicRecord["physical_addr_2"] = dynamicRecord["physical_addr_2"].upper()
    dynamicRecord["physical_state"] = dynamicRecord["physical_state"].upper()
    dynamicRecord["physical_address_type"] = dynamicRecord["physical_address_type"].upper()
    dynamicRecord["state"] = dynamicRecord["state"].upper()
    dynamicRecord["cass_cert_address"] = dynamicRecord["cass_cert_address"].upper()
    dynamicRecord["chain_type"] = dynamicRecord["chain_type"].upper()
    dynamicRecord["hq"] = dynamicRecord["hq"].upper()
    dynamicRecord["location_name_legal"] = dynamicRecord["location_name_legal"].upper()
    dynamicRecord["mailable"] = dynamicRecord["mailable"].upper()
    dynamicRecord["metro"] = dynamicRecord["metro"].upper()
    dynamicRecord["soho"] = dynamicRecord["soho"].upper()
    dynamicRecord["twitter_account"] = dynamicRecord["twitter_account"].upper()
    dynamicRecord["mlng_addr_dnm_type"] = dynamicRecord["mlng_addr_dnm_type"].upper()
    dynamicRecord["physical_city"] = dynamicRecord["physical_city"].upper()
    dynamicRecord["ncoa_move_footnote"] = dynamicRecord["ncoa_move_footnote"].upper()
    dynamicRecord["ncoa_move_type"] = dynamicRecord["ncoa_move_type"].upper()
    dynamicRecord["mlng_addr_cass_certified"] = dynamicRecord["mlng_addr_cass_certified"].upper()
    dynamicRecord["mlng_addr_ncoa_move_footnote"] = dynamicRecord["mlng_addr_ncoa_move_footnote"].upper()
    dynamicRecord["mlng_addr_ncoa_move_type"] = dynamicRecord["mlng_addr_ncoa_move_type"].upper()
    dynamicRecord["is_ssnal"] = dynamicRecord["is_ssnal"].upper()
    dynamicRecord["rds_dnm_typ"] = dynamicRecord["rds_dnm_typ"].upper()
    dynamicRecord["rds_dnm_ind"] = dynamicRecord["rds_dnm_ind"].upper()
    dynamicRecord["is_vcnt"] = dynamicRecord["is_vcnt"].upper()
    dynamicRecord["elot_cd"] = dynamicRecord["elot_cd"].upper()
    dynamicRecord["zip_type"] = dynamicRecord["zip_type"].upper()
    dynamicRecord["rds_dnk_ind"] = dynamicRecord["rds_dnk_ind"].upper()
    dynamicRecord["rds_dnk_typ"] = dynamicRecord["rds_dnk_typ"].upper()
    dynamicRecord["mlng_addr_dnk"] = dynamicRecord["mlng_addr_dnk"].upper()
    dynamicRecord["mlng_addr_dnk_type"] = dynamicRecord["mlng_addr_dnk_type"].upper()
    dynamicRecord["physical_address_cass_certified"] = dynamicRecord["physical_address_cass_certified"].upper()
    dynamicRecord["src_file_name"] = args["filename"]

    create_nax_data = dynamicRecord["city"]+dynamicRecord["state"]+dynamicRecord["postal_code"]+dynamicRecord["postal_code4"]+dynamicRecord["physical_addr_1"]+dynamicRecord["physical_addr_2"]
    dynamicRecord["hash_md5_nax"] = hashlib.md5(str(create_nax_data).encode("utf8")).hexdigest()+"|"+dynamicRecord["place_id"]

    create_addr_data = dynamicRecord["physical_addr_1"]+dynamicRecord["physical_addr_2"]+dynamicRecord["physical_city"]+dynamicRecord["physical_state"]+dynamicRecord["physical_zip5"]+dynamicRecord["latitude"]+dynamicRecord["longitude"]+dynamicRecord["metro"]+dynamicRecord["cass_cert_address"]+dynamicRecord["physical_address_type"]+dynamicRecord["zip_type"]+dynamicRecord["physical_address_cass_certified"]+dynamicRecord["physical_addr_crrctn_ftnte"]+dynamicRecord["physical_addr_dpv_match_cd"]
    dynamicRecord["hash_md5_addr_mstr"] = hashlib.md5(str(create_addr_data).encode("utf8")).hexdigest()+"|"+dynamicRecord["place_id"]

    create_bldng_data = dynamicRecord["physical_addr_1"]+dynamicRecord["physical_city"]+dynamicRecord["physical_state"]+dynamicRecord["physical_zip5"]
    dynamicRecord["hash_md5_bldng_mstr"] = hashlib.md5(str(create_bldng_data).encode("utf8")).hexdigest()+"|"+dynamicRecord["place_id"]

    create_addr_mlng_data = dynamicRecord["mlng_addr_street"]+dynamicRecord["mlng_addr_street2"]+dynamicRecord["mlng_addr_state"]+dynamicRecord["mlng_addr_city"]+dynamicRecord["mlng_addr_zip5"]+dynamicRecord["physical_addr_crrctn_ftnte"]+dynamicRecord["physical_addr_dpv_match_cd"]
    dynamicRecord["hash_md5_addr_mlng"] = hashlib.md5(str(create_addr_mlng_data).encode("utf8")).hexdigest()+"|"+dynamicRecord["place_id"]

    create_bsnss_data = dynamicRecord["physical_addr_1"]+dynamicRecord["physical_addr_2"]+dynamicRecord["physical_city"]+dynamicRecord["physical_state"]+dynamicRecord["physical_zip5"]+dynamicRecord["physical_zip4"]+dynamicRecord["be_id"]+dynamicRecord["chain_count"]+dynamicRecord["chain_type"]+dynamicRecord["closed_date"]+dynamicRecord["closed"]+dynamicRecord["hq"]+dynamicRecord["location_headcount"]+dynamicRecord["location_headcount_range"]+dynamicRecord["location_revenue_range"]+dynamicRecord["location_revenue"]+dynamicRecord["name"]+dynamicRecord["soho"]+dynamicRecord["twitter_account"]+dynamicRecord["mailable"]+dynamicRecord["place_quality_indicator"]+dynamicRecord["ncoa_move_footnote"]+dynamicRecord["ncoa_move_type"]+dynamicRecord["is_ssnal"]+dynamicRecord["is_vcnt"]+dynamicRecord["location_name_dba"]+dynamicRecord["location_name_legal"]+dynamicRecord["rds_dnk_typ"]+dynamicRecord["rds_dnk_ind"]+dynamicRecord["rds_dnm_ind"]+dynamicRecord["rds_dnm_typ"]+dynamicRecord["dlvry_pnt_cd"]+dynamicRecord["elot_cd"]+dynamicRecord["carrier_route_cd"]+dynamicRecord["mlng_addr_type"]+dynamicRecord["mlng_addr_city"]+dynamicRecord["mlng_addr_delivery_point_cd"]+dynamicRecord["mlng_addr_elot_cd"]+dynamicRecord["mlng_addr_carrier_route_cd"]+dynamicRecord["mlng_addr_dnk"]+dynamicRecord["mlng_addr_dnk_type"]+dynamicRecord["mlng_addr_dnm_type"]+dynamicRecord["mlng_addr_dnm"]+dynamicRecord["mlng_addr_state"]+dynamicRecord["mlng_addr_street"]+dynamicRecord["mlng_addr_street2"]+dynamicRecord["mlng_addr_zip4"]+dynamicRecord["mlng_addr_zip5"]+dynamicRecord["mlng_addr_cass_certified"]+dynamicRecord["mlng_addr_is_vacant"]+dynamicRecord["mlng_addr_is_seasonal"]+dynamicRecord["mlng_addr_zip_type"]+dynamicRecord["mlng_addr_ncoa_move_footnote"]+dynamicRecord["mlng_addr_ncoa_move_type"]+dynamicRecord["mlng_addr_crrctn_ftnte"]+dynamicRecord["mlng_addr_dpv_match_cd"]+dynamicRecord["mlng_addr_ncoa_move_date"]+dynamicRecord["ncoa_move_date"]+dynamicRecord["closed_date"]
    dynamicRecord["hash_md5_bsnss_mstr"] = hashlib.md5(str(create_bsnss_data).encode("utf8")).hexdigest()
    return dynamicRecord

stg_radius_place_with_hash = Map.apply(frame = stg_radius_place_dynamic_frame, f = hash_calculation_stg_radius_place, transformation_ctx = "stg_radius_place_with_hash")

stg_radius_place_selectfields = SelectFields.apply(frame = stg_radius_place_with_hash, paths = ["place_id","mlng_addr_dnm","address_key","addrss_key_extndd","build_id","be_id","city","closed","location_headcount","location_headcount_range","location_revenue_range",
                                                                                                "location_name_dba","location_revenue","mlng_addr_carrier_route_cd","mlng_addr_type","mlng_addr_city","mlng_addr_crrctn_ftnte","mlng_addr_delivery_point_cd",
                                                                                                "mlng_addr_dpv_match_cd","mlng_addr_is_seasonal","mlng_addr_elot_cd","mlng_addr_ncoa_move_date","mlng_addr_state","mlng_addr_street","mlng_addr_street2",
                                                                                                "mlng_addr_zip4","mlng_addr_zip5","mlng_addr_zip_type","name","carrier_route_cd","physical_addr_1","physical_addr_crrctn_ftnte","mlng_addr_is_vacant","physical_addr_dpv_match_cd",
                                                                                                "physical_addr_2","physical_state","physical_zip4","physical_zip5","physical_address_type","postal_code","postal_code4","state","cass_cert_address","chain_type","hq","ncoa_move_date",
                                                                                                "latitude","location_name_legal","mailable","metro","place_quality_indicator","soho","twitter_account","chain_count","mlng_addr_dnm_type","business_start_date","physical_city","closed_date",
                                                                                                "ncoa_move_footnote","ncoa_move_type","mlng_addr_cass_certified","mlng_addr_ncoa_move_footnote","mlng_addr_ncoa_move_type","is_ssnal","rds_dnm_typ","longitude","dlvry_pnt_cd","rds_dnm_ind",
                                                                                                "is_vcnt","elot_cd","zip_type","rds_dnk_ind","rds_dnk_typ","mlng_addr_dnk","mlng_addr_dnk_type","physical_address_cass_certified","hash_md5_nax","hash_md5_addr_mstr","hash_md5_bldng_mstr",
                                                                                                "hash_md5_addr_mlng","hash_md5_bsnss_mstr","src_file_name"], transformation_ctx = "stg_radius_place_selectfields")


output_stg_radius_place = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_selectfields, connection_type = "s3", connection_options = {"path": stg_radius_place_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator }, transformation_ctx = "output_stg_radius_place")

#############  STG_RADIUS_PLACE_ASSOC   ###########################
stg_radius_place_assoc_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("place_ids", "array", "place_id_assoc", "array")], transformation_ctx = "stg_radius_place_assoc_applymaping").toDF()

place_assoc_data = stg_radius_place_assoc_applymaping.withColumn("place_id_assoc", explode(stg_radius_place_assoc_applymaping.place_id_assoc))

stg_radius_place_assoc_dynamic_frame = DynamicFrame.fromDF(place_assoc_data, glueContext, "stg_radius_place_assoc_dynamic_frame")

output_stg_radius_place_assoc = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_assoc_dynamic_frame, connection_type = "s3", connection_options = {"path": stg_radius_place_assoc_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_assoc")



#############  STG_RADIUS_PLACE_CLSD_RSNS   ###########################
stg_radius_place_clsd_rsns_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("closed_reasons", "array", "clsd_rsns", "array"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_clsd_rsns_applymaping").toDF()

clsd_rsns_data = stg_radius_place_clsd_rsns_applymaping.withColumn("clsd_rsns", explode(stg_radius_place_clsd_rsns_applymaping.clsd_rsns))

stg_radius_place_clsd_rsns_dynamic_frame = DynamicFrame.fromDF(clsd_rsns_data, glueContext, "stg_radius_place_clsd_rsns_dynamic_frame")


def clsd_rsns_hash(dynamicRecord):
    dynamicRecord["clsd_rsns"] = dynamicRecord["clsd_rsns"].upper()

    hash_data = dynamicRecord["clsd_rsns"]
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_clsd_rsns_with_hash = Map.apply(frame = stg_radius_place_clsd_rsns_dynamic_frame, f = clsd_rsns_hash, transformation_ctx = "stg_radius_place_clsd_rsns_with_hash")


output_stg_radius_place_clsd_rsns = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_clsd_rsns_with_hash, connection_type = "s3", connection_options = {"path": stg_radius_place_clsd_rsns_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_clsd_rsns")



#############  STG_RADIUS_PLACE_CTGRY   ###########################
stg_radius_place_ctgry_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("categories", "array", "categories", "array"),
                                                                    ("category_code", "string", "category_code", "string"),
                                                                    ("category_description", "string", "category_description", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_ctgry_applymaping").toDF()

ctgry_data = stg_radius_place_ctgry_applymaping.withColumn("categories", explode(stg_radius_place_ctgry_applymaping.categories)).select('place_id', 'categories.*')


stg_radius_place_ctgry_dynamic_frame = DynamicFrame.fromDF(ctgry_data, glueContext, "stg_radius_place_ctgry_dynamic_frame")


def ctgry_hash(dynamicRecord):
    dynamicRecord["category_code"] = dynamicRecord["category_code"].upper()
    dynamicRecord["category_description"] = dynamicRecord["category_description"].upper()

    hash_data = dynamicRecord["category_code"]+dynamicRecord["category_description"]
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_ctgry_with_hash = Map.apply(frame = stg_radius_place_ctgry_dynamic_frame, f = ctgry_hash, transformation_ctx = "stg_radius_place_ctgry_with_hash")


output_stg_radius_place_ctgry = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_ctgry_with_hash, connection_type = "s3", connection_options = {"path": stg_radius_place_ctgry_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_clsd_rsns")



#############  STG_RADIUS_PLACE_NAICS_CDS   ###########################
stg_radius_place_naics_cd_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("naics_codes", "array", "naics_codes", "array"),
                                                                    ("naics_cd", "string", "naics_cd", "string"),
                                                                    ("rank", "string", "rank", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_naics_cd_applymaping").toDF()

naics_cd_data = stg_radius_place_naics_cd_applymaping.withColumn("naics_codes", explode(stg_radius_place_naics_cd_applymaping.naics_codes)).select('place_id', 'naics_codes.*')

rank_naics = Window.partitionBy(naics_cd_data['place_id']).orderBy(naics_cd_data['category_code'].desc())

rank_naics_apply = naics_cd_data.select('*', rank().over(rank_naics).alias('rank'))


stg_radius_place_naics_cd_dynamic_frame = DynamicFrame.fromDF(rank_naics_apply, glueContext, "stg_radius_place_naics_cd_dynamic_frame").repartition(1)


def naics_cd_hash(dynamicRecord):

    hash_data = dynamicRecord["category_code"]+str(dynamicRecord["rank"])
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_naics_cd_with_hash = Map.apply(frame = stg_radius_place_naics_cd_dynamic_frame, f = naics_cd_hash, transformation_ctx = "stg_radius_place_naics_cd_with_hash")


output_stg_radius_place_naics_cd = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_naics_cd_with_hash, connection_type = "s3", connection_options = {"path": stg_radius_place_naics_cd_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_naics_cd")


#############  STG_RADIUS_PLACE_SIC_CDS   ###########################
stg_radius_place_sic_cd_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("sic_codes", "array", "sic_codes", "array"),
                                                                    ("sic_cd", "string", "sic_cd", "string"),
                                                                    ("rank", "string", "rank", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_sic_cd_applymaping").toDF()

sic_cd_data = stg_radius_place_sic_cd_applymaping.withColumn("sic_codes", explode(stg_radius_place_sic_cd_applymaping.sic_codes)).select('place_id', 'sic_codes.*')

rank_sic = Window.partitionBy(sic_cd_data['place_id']).orderBy(sic_cd_data['category_code'].desc())

rank_sic_apply = sic_cd_data.select('*', rank().over(rank_sic).alias('rank'))

stg_radius_place_sic_cd_dynamic_frame = DynamicFrame.fromDF(rank_sic_apply, glueContext, "stg_radius_place_sic_cd_dynamic_frame").repartition(1)


def sic_cd_hash(dynamicRecord):

    hash_data = dynamicRecord["category_code"]+str(dynamicRecord["rank"])
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_sic_cd_with_hash = Map.apply(frame = stg_radius_place_sic_cd_dynamic_frame, f = sic_cd_hash, transformation_ctx = "stg_radius_place_sic_cd_with_hash")


output_stg_radius_place_sic_cd = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_sic_cd_with_hash, connection_type = "s3", connection_options = {"path": stg_radius_place_sic_cd_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_sic_cd")


#############  STG_RADIUS_PLACE_SCNDRY_NM   ###########################
stg_radius_place_scndry_nm_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("names", "array", "names", "array"),
                                                                    ("rank", "string", "rank", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_scndry_nm_applymaping").toDF()


remove_dupes_from_array = udf(lambda row: list(set(row)), ArrayType(StringType()))

scndry_nm_distinct_names = stg_radius_place_scndry_nm_applymaping.withColumn("names", remove_dupes_from_array("names"))

scndry_nm_data = scndry_nm_distinct_names.withColumn("names", explode(scndry_nm_distinct_names.names))

rank_scndry_nm = Window.partitionBy(scndry_nm_data['place_id']).orderBy(scndry_nm_data['names'].desc())

rank_scndry_nm_apply = scndry_nm_data.select('*', rank().over(rank_scndry_nm).alias('rank'))

stg_radius_place_scndry_nm_dynamic_frame = DynamicFrame.fromDF(rank_scndry_nm_apply, glueContext, "stg_radius_place_scndry_nm_dynamic_frame").repartition(1)


def scndry_nm_hash(dynamicRecord):
    dynamicRecord["names"] = dynamicRecord["names"].upper()

    hash_data = dynamicRecord["names"]+str(dynamicRecord['rank'])
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_scndry_nm_with_hash = Map.apply(frame = stg_radius_place_scndry_nm_dynamic_frame, f = scndry_nm_hash, transformation_ctx = "stg_radius_place_scndry_nm_with_hash")


output_stg_radius_place_scndry_nm = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_scndry_nm_with_hash, connection_type = "s3", connection_options = {"path": stg_radius_place_scndry_nm_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_scndry_nm")


#############  STG_RADIUS_PLACE_EML   ###########################
stg_radius_place_eml_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("email", "array", "email", "array"),
                                                                    ("rank", "string", "rank", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_eml_applymaping").toDF()


place_eml_data = stg_radius_place_eml_applymaping.withColumn("email", explode(stg_radius_place_eml_applymaping.email))

rank_eml = Window.partitionBy(place_eml_data['place_id']).orderBy(place_eml_data['email'].desc())

rank_eml_apply = place_eml_data.select('*', rank().over(rank_eml).alias('rank'))

stg_radius_place_eml_dynamic_frame = DynamicFrame.fromDF(rank_eml_apply, glueContext, "stg_radius_place_eml_dynamic_frame").repartition(1)

def eml_hash(dynamicRecord):
    dynamicRecord["email"] = dynamicRecord["email"].upper()

    hash_data = dynamicRecord["email"]+str(dynamicRecord['rank'])
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_eml_with_hash = Map.apply(frame = stg_radius_place_eml_dynamic_frame, f = eml_hash, transformation_ctx = "stg_radius_place_eml_with_hash")


output_stg_radius_place_eml = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_eml_with_hash, connection_type = "s3", connection_options = {"path": stg_radius_place_eml_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_eml")


#############  STG_RADIUS_PLACE_WBST   ###########################
stg_radius_place_wbst_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("website", "array", "website", "array"),
                                                                    ("rank", "string", "rank", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_wbst_applymaping").toDF()


place_wbst_data = stg_radius_place_wbst_applymaping.withColumn("website", explode(stg_radius_place_wbst_applymaping.website))

rank_wbst = Window.partitionBy(place_wbst_data['place_id']).orderBy(place_wbst_data['website'].desc())

rank_wbst_apply = place_wbst_data.select('*', rank().over(rank_wbst).alias('rank'))

stg_radius_place_wbst_dynamic_frame = DynamicFrame.fromDF(rank_wbst_apply, glueContext, "stg_radius_place_wbst_dynamic_frame").repartition(1)

def wbst_hash(dynamicRecord):
    dynamicRecord["website"] = dynamicRecord["website"].upper()

    hash_data = dynamicRecord["website"]+str(dynamicRecord['rank'])
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_wbst_with_hash = Map.apply(frame = stg_radius_place_wbst_dynamic_frame, f = wbst_hash, transformation_ctx = "stg_radius_place_wbst_with_hash")


output_stg_radius_place_wbst = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_wbst_with_hash, connection_type = "s3", connection_options = {"path": stg_radius_place_wbst_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_wbst")


#############  STG_RADIUS_PLACE_XREF   ###########################
stg_radius_place_xref_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("record_id", "array", "record_id", "array"),
                                                                    ("source", "string", "source", "string"),
                                                                    ("record_no", "string", "record_no", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_xref_applymaping").toDF()


place_xref_data = stg_radius_place_xref_applymaping.withColumn("record_id", explode(stg_radius_place_xref_applymaping.record_id))

split_xref = place_xref_data.withColumn('record_id', F.split(place_xref_data['record_id'], '-')).withColumn('source',F.col('record_id')[0]).withColumn('record_no', F.col('record_id')[1])

stg_radius_place_xref_dynamic_frame = DynamicFrame.fromDF(split_xref, glueContext, "stg_radius_place_xref_dynamic_frame")

def xref_hash(dynamicRecord):
    dynamicRecord["source"] = dynamicRecord["source"].upper()

    hash_data = dynamicRecord["source"]+dynamicRecord['record_no']
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()

    return dynamicRecord

stg_radius_place_xref_with_hash = Map.apply(frame = stg_radius_place_xref_dynamic_frame, f = xref_hash, transformation_ctx = "stg_radius_place_xref_with_hash")

stg_radius_place_xref_selectfields = SelectFields.apply(frame = stg_radius_place_xref_with_hash, paths = ["place_id","source","record_no","hash_md5_mstr"], transformation_ctx = "stg_radius_place_xref_selectfields")

output_stg_radius_place_xref = glueContext.write_dynamic_frame.from_options(frame = stg_radius_place_xref_selectfields, connection_type = "s3", connection_options = {"path": stg_radius_place_xref_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_xref")



#############  STG_RADIUS_PLACE_TYP_PHNNMBR   ###########################
stg_radius_place_typ_phn_applymaping = ApplyMapping.apply(frame = datasource0, mappings = [("place_id", "string", "place_id", "string"),
                                                                    ("typed_phone_numbers", "array", "typed_phone_numbers", "array"),
                                                                    ("phone_number", "string", "phone_number", "string"),
                                                                    ("phone_type", "string", "phone_type", "string"),
                                                                    ("do_not_call", "boolean", "do_not_call", "string"),
                                                                    ("rank", "double", "rank", "string"),
                                                                    ("source", "string", "source", "string"),
                                                                    ("rank_number", "string", "rank_number", "string"),
                                                                    ("hash_md5_mstr", "string", "hash_md5_mstr", "string")], transformation_ctx = "stg_radius_place_typ_phn_applymaping").toDF()


place_phn_type_data = stg_radius_place_typ_phn_applymaping.withColumn("typed_phone_numbers", explode(stg_radius_place_typ_phn_applymaping.typed_phone_numbers)).selectExpr("place_id","typed_phone_numbers.*")

select_cell_fax = place_phn_type_data.filter(F.col('phone_type') != 'PHONE' )

stg_radius_place_type_cell_fax_dynamic_frame = DynamicFrame.fromDF(select_cell_fax, glueContext, "stg_radius_place_type_cell_fax_dynamic_frame")

def cell_fax_hash(dynamicRecord):
    dynamicRecord["phone_type"] = dynamicRecord["phone_type"].upper()
    dynamicRecord["do_not_call"] = str(dynamicRecord["do_not_call"]).upper()
    dynamicRecord["source"] = dynamicRecord["source"].upper()
    dynamicRecord["rank_number"] = str("0")

    hash_data = dynamicRecord["phone_number"]+dynamicRecord['phone_type']+str(dynamicRecord["do_not_call"])+str(dynamicRecord['rank'])+dynamicRecord["source"]+str("0")
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()
    return dynamicRecord
stg_radius_place_cell_fax_with_hash = Map.apply(frame = stg_radius_place_type_cell_fax_dynamic_frame, f = cell_fax_hash, transformation_ctx = "stg_radius_place_cell_fax_with_hash").toDF()

select_phone = place_phn_type_data.filter(F.col('phone_type') == 'PHONE' )

rank_phone = Window.partitionBy(select_phone['place_id']).orderBy(select_phone['source'].desc())

rank_phone_apply = select_phone.select('*', rank().over(rank_phone).alias('rank_number'))

stg_radius_place_phone_dynamic_frame = DynamicFrame.fromDF(rank_phone_apply, glueContext, "stg_radius_place_phone_dynamic_frame")

def phone_hash(dynamicRecord):
    dynamicRecord["phone_type"] = dynamicRecord["phone_type"].upper()
    dynamicRecord["do_not_call"] = str(dynamicRecord["do_not_call"]).upper()
    dynamicRecord["source"] = dynamicRecord["source"].upper()

    hash_data = dynamicRecord["phone_number"]+dynamicRecord['phone_type']+str(dynamicRecord["do_not_call"])+str(dynamicRecord['rank'])+dynamicRecord["source"]+str(dynamicRecord["rank_number"])
    dynamicRecord["hash_md5_mstr"] = hashlib.md5(str(hash_data).encode("utf8")).hexdigest()
    return dynamicRecord

stg_radius_place_phone_with_hash = Map.apply(frame = stg_radius_place_phone_dynamic_frame, f = phone_hash, transformation_ctx = "stg_radius_place_phone_with_hash").toDF()

union_data_frame = stg_radius_place_phone_with_hash.unionAll(stg_radius_place_cell_fax_with_hash)

union_dynamic_frame = DynamicFrame.fromDF(union_data_frame, glueContext, "union_dynamic_frame").repartition(1)

output_stg_radius_place_type_phone = glueContext.write_dynamic_frame.from_options(frame = union_dynamic_frame, connection_type = "s3", connection_options = {"path": stg_radius_place_typ_phn_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_type_phone")



#############  STG_RADIUS_PLACE_EXTRNL_LNKS   ###########################

extrnl_lnks_cd_data = SelectFields.apply(frame = datasource0, paths = ["place_id","external_link"], transformation_ctx = "extrnl_lnks_cd_data")


def extrnal_function(dynamicRecord):
    out_list_external_link=[]
    place_id = dynamicRecord["place_id"]
    external_links = dynamicRecord["external_link"]

    if len(external_links) != 0:
        RANK_NUMBER=1
        for key in external_links:
            for values in external_links[key]:
                externalData=place_id+key+values+str(RANK_NUMBER)
                External_Hash= hashlib.md5(str(externalData).encode("utf8")).hexdigest()
                HASH_MD5_MSTR_EXTR=External_Hash
                output_external_link =(place_id+seperator+key.upper()+seperator+values+seperator+HASH_MD5_MSTR_EXTR+seperator+str(RANK_NUMBER))
                out_list_external_link.append(output_external_link)
                RANK_NUMBER=RANK_NUMBER+1
    dynamicRecord["extrnl_lnks_vl"] = out_list_external_link
    return dynamicRecord

convert_struct_array = Map.apply(frame = extrnl_lnks_cd_data, f = extrnal_function, transformation_ctx = "convert_struct_array").toDF()

stg_extrnal_lnk_data = convert_struct_array.select(explode("extrnl_lnks_vl").alias("place_id"+seperator+"key"+seperator+"values_external"+seperator+"HASH_MD5_MSTR_EXTR"+seperator+"RANK_NUMBER"))

stg_external_dynamic = DynamicFrame.fromDF(stg_extrnal_lnk_data, glueContext, "stg_external_dynamic")

output_stg_radius_place_external_lnk = glueContext.write_dynamic_frame.from_options(frame = stg_external_dynamic, connection_type = "s3", connection_options = {"path": stg_radius_place_external_lnk_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_external_lnk")


#############  STG_RADIUS_PLACE_CSTM_ATTR   ###########################

cstm_attr_data = SelectFields.apply(frame = datasource0, paths = ["place_id","custom_place_attributes"], transformation_ctx = "cstm_attr_data")


def cstm_attr_function(dynamicRecord):
    out_list_cstm_attr=[]
    place_id = dynamicRecord["place_id"]
    custom_place = dynamicRecord["custom_place_attributes"]


    if len(custom_place) != 0:
        for properties in custom_place:
            for val in custom_place[properties]:
                if val is not None:
                    source=str(val["source"])
                    record_id=str(val["record_id"])
                    value=str(val["value"])
                    record_type=str(val["record_type"])

                    categoryData=place_id+properties+source+record_id+value+record_type
                    Category_Hash= hashlib.md5(str(categoryData).encode("utf8")).hexdigest()
                    HASH_MD5_MSTR_ATTR=Category_Hash
                    output_categories =(place_id+seperator+properties.upper()+seperator+source.upper()+seperator+record_id+seperator+value.upper()+seperator+record_type.upper()+seperator+HASH_MD5_MSTR_ATTR)
                    out_list_cstm_attr.append(output_categories)
    dynamicRecord["cstm_attr_vl"] = out_list_cstm_attr
    return dynamicRecord
convert_struct_array_cstm = Map.apply(frame = cstm_attr_data, f = cstm_attr_function, transformation_ctx = "convert_struct_array_cstm").toDF()


stg_cstm_attr_data = convert_struct_array_cstm.select(explode("cstm_attr_vl").alias("place_id"+seperator+"properties"+seperator+"source"+seperator+"record_id"+seperator+"value"+seperator+"record_type"+seperator+"HASH_MD5_MSTR_ATTR"))

stg_cstm_attr_dynamic = DynamicFrame.fromDF(stg_cstm_attr_data, glueContext, "stg_cstm_attr_dynamic")

output_stg_radius_place_cstm_attr = glueContext.write_dynamic_frame.from_options(frame = stg_cstm_attr_dynamic, connection_type = "s3", connection_options = {"path": stg_radius_place_cstm_attr_output_location}, format = "csv", format_options = {"quoteChar" : -1, "separator": seperator}, transformation_ctx = "output_stg_radius_place_cstm_attr")
