#!/opt/python/bin/python3

import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import subprocess

def dirCleanup(seed_profile_out):

	try:
		subprocess.run(['hadoop', 'fs', '-rm', '-r', seed_profile_out], check=True)
		print(f"INFO: Successfully deleted output HDFS directory: {seed_profile_out}")
	
	except subprocess.CalledProcessError as e:
		print(f"INFO: Failed to delete output HDFS directory: {seed_profile_out}. Error: {e}")


	

def fetchSeed(spark,file_path,schema):

	seed_list = spark.read.option("delimiter", "\t").schema(schema).csv(file_path)
	return seed_list

def fetchPreferences(spark,pref_table,date):

	pref_table = spark.read.table(pref_table)
	pref_table_today = pref_table.filter(pref_table.generated_date==date)
	
	return pref_table_today


def fetchProfile(spark,profile_table,date):

	cpt = spark.read.table(profile_table)
	cpt_today = cpt.filter(cpt.date==date)

	return cpt_today

def joinPreferences(seed_users,user_preferences):
	
	join_break = seed_users.join(user_preferences,seed_users["guid"]==user_preferences["key"],'inner')
	join_break_stage = join_break.selectExpr("guid","preferences['ynbn'] as breakingNewsPref")

	join_break_unsub = join_break_stage.filter((join_break_stage["breakingNewsPref"].isNull())|(join_break_stage["breakingNewsPref"] == 'Y'))

	return join_break_unsub

def joinProfile(seed_preferences,user_profile):


	join_profile = seed_preferences.join(user_profile,seed_preferences["guid"]==user_profile["customerkey"],'inner')
	join_profile_out = join_profile.select("guid","emailaddress","firstname","obfuid")

	return join_profile_out

def supressGDPR(spark,seed_profile_stage,gdpr_dsr,schema_dsr,gdpr_aoc,schema_aoc):

	gdpr_supp_dsr = spark.read.option("delimiter", "\t").schema(schema_dsr).csv(gdpr_dsr)
	gdpr_supp_aoc = spark.read.option("delimiter", "\t").schema(schema_aoc).csv(gdpr_aoc)
	
	seed_gen_stage = seed_profile_stage.join(gdpr_supp_dsr,seed_profile_stage["guid"]==gdpr_supp_dsr["guid"],'left_anti')
	seed_gen_stage2 = seed_gen_stage.join(gdpr_supp_aoc,seed_gen_stage["guid"]==gdpr_supp_aoc["key"],'left')
	seed_gen_stage3 = seed_gen_stage2.filter((gdpr_supp_aoc["cooAnalysisOfCommunications"] != '0')|(gdpr_supp_aoc["cooAnalysisOfCommunications"].isNull()))
	seed_gen = seed_gen_stage3.select(seed_gen_stage["guid"].alias("user_id"),seed_gen_stage["emailaddress"],seed_gen_stage["firstname"],seed_gen_stage["obfuid"])

	return seed_gen

def writeSeedProfile(seed_profile,seed_profile_out):

	seed_profile_final = seed_profile.toJSON()
	seed_profile_final.coalesce(1).saveAsTextFile(seed_profile_out)
	#seed_profile.coalesce(1).write.format("csv").option("header", "true").save(seed_profile_out)


def main(date,gdpr_supp_list,gdpr_inp_list):

	print("INFO: Initialising spark session")
	spark = SparkSession.builder.appName("BreakingNews Campaign").enableHiveSupport().getOrCreate()
	
	file_path = ""
	#output_path = "/projects/direct/stage/manish/breakingn/out_notnull/"
	seed_profile_out = ""
	pref_table=""
	profile_table=""
	gdpr_dsr=gdpr_supp_list
	gdpr_aoc=gdpr_inp_list

	schema = StructType([
	StructField("guid", StringType(), True),
	StructField("remaining", StringType(), True),
	])

	schema_dsr = StructType([StructField("guid",StringType(), True),StructField("yuid",StringType(), True),StructField("gdprDel",StringType(), True),StructField("gdprObj",StringType(), True),StructField("gdprRstrct",StringType(), True)])

	schema_aoc = StructType([StructField("key",StringType(), True), StructField("yuid",StringType(), True), StructField("gdprDel",StringType(), True), StructField("gdprObj",StringType(), True), StructField("gdprRstrct",StringType(), True), StructField("cooNonEUConsent",StringType(), True), StructField("cooCoreEUConsent",StringType(), True), StructField("cooOathAsThirdParty",StringType(), True), StructField("cooAnalysisOfCommunications",StringType(), True), StructField("cooPreciseGeoLocation",StringType(), True), StructField("cooCrossDeviceMapping",StringType(), True), StructField("cooAccountMatching",StringType(), True), StructField("cooSearchHistory",StringType(), True), StructField("cooFirstPartyAds",StringType(), True), StructField("cooContentPersonalization",StringType(), True), StructField("cooIab",StringType(), True)])

	dirCleanup(seed_profile_out)
	seed_users = fetchSeed(spark,file_path,schema)
	user_preferences = fetchPreferences(spark,pref_table,date)
	user_profile = fetchProfile(spark,profile_table,date)
	seed_preferences = joinPreferences(seed_users,user_preferences)
	seed_profile_stage = joinProfile(seed_preferences,user_profile)
	seed_profile = supressGDPR(spark,seed_profile_stage,gdpr_dsr,schema_dsr,gdpr_aoc,schema_aoc)
	
	print("INFO: Writing final out")
	writeSeedProfile(seed_profile,seed_profile_out)
	


if __name__ == "__main__":

	if len(sys.argv) != 4:
		
		print("INFO: Please Provide Date,GDPR,AOC Details!")
		sys.exit(1)

	date = sys.argv[1]
	gdpr_supp=sys.argv[2]
	gdpr_inp=sys.argv[3]
	print("INFO: Calling Main Function.")
	main(date,gdpr_supp,gdpr_inp)
	


