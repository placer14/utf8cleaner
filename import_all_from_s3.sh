#!/bin/bash

set -euo pipefail

# variables
DATABASE_URL="postgres://tsdbadmin@hb105vkpyz.rn0l1vx697.tsdb.cloud.timescale.com:39741/tsdb?sslmode=require"
BUCKET=lily-data
S3_PATH=s3://${BUCKET}
SCHEMA=filltest
PSQL_OPTIONS="--output=/dev/null -b -Xtd ${DATABASE_URL}"

# local_table_over_epochs accepts a table and epoch set and loads that CSV
# data into postgres connection string defined in env var DATABASE_URL
# $1 - epoch set folder name
# $2 - table name

load_table_over_epochs () {
	COLUMNS=$(aws s3 cp "${S3_PATH}/$2.header" -)
	IMPORT_FILENAME=${S3_PATH}/data/$1/$2.csv

  # Fix error logging edge cases
  psql ${PSQL_OPTIONS} -c "BEGIN TRANSACTION;" -c "CREATE TEMP TABLE temp_$2 ON COMMIT DROP AS SELECT * FROM ${SCHEMA}.$2 WITH NO DATA;" -c "\\copy temp_$2(${COLUMNS}) FROM PROGRAM 'aws s3 cp --request-payer requester ${IMPORT_FILENAME} -' DELIMITER ',' CSV;" -c "INSERT INTO ${SCHEMA}.${TABLE_NAME} SELECT * FROM temp_${TABLE_NAME} ON CONFLICT DO NOTHING;" -c "COMMIT;" \
    | sed "s/^\([: a-zA-Z]*\):[[:space:]]*\(.*\)$/{\"time\":\"$(date -u +"%FT%T.000Z")\",\"level\":\"\1\",\"message\":\"\2\"}\n/"
}

# calc_percent_complete accepts integers and finds floor($1 div $2) + floor($3 div $4)
# in order to approximate the amount of work that has been completed and yet to be done
# $1 - epoch set numerator ...should be current count
# $2 - epoch set demoninator ...should be total count
# $3 - table set numerator ...should be current count
# $4 - table set demoninator ...should be total count
calc_percentage () {
	(printf "%.2f" `echo "(((${1} / ${2}) + (${3} / ${4} * (1 / ${2})))*100)" | bc -l`)
}

# get tables and dates into enumerable form
DB_EPOCH_SETS=$(
	# list all available heights
	aws s3 ls ${S3_PATH}/data/ |\
	# pull out just the numbered name of the folders (of pattern /[0-9]+__[0-9]+/)
	awk 'BEGIN { FS=" " } match($2, /([0-9]+__[0-9]+)\//, m) { print m[1] }' |\
	# sort in ascending order
	sort -n -t "_" -k 1
)
# TODO: Handle no epoch sets

SAVEIFS=$IFS
IFS=$'\n'
# poof, now it's an array separated on newlines
DB_EPOCH_SETS=($DB_EPOCH_SETS)
IFS=$SAVEIFS

TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"message\":\"load schema\",\"real\":%E,\"user\":%U,\"sys\":%S}" 
# TODO: Fix error logging here
# TODO: Make setup optional based on DB state
# time psql ${PSQL_OPTIONS} -f ./setup_schema.sql \
#   | sed "s/^\(.*\):[[:space:]]*\(.*\)$/{\"time\":\"$(date -u +"%FT%T.000Z")\",\"level\":\"\1\",\"message\":\"\2\"}\n/"

EPOCH_SET_TOTAL=${#DB_EPOCH_SETS[@]}
for (( i=0; i<${EPOCH_SET_TOTAL}; i++ )); do
	if [ ${i} -lt 376 ]; then continue; fi
# debug just one iteration
#EPOCH_SET_TOTAL=1
#for (( i=0; i<1; i++ )); do
	EPOCH_SET=${DB_EPOCH_SETS[$i]}
	DB_TABLES=$(
		# list all available tables
		aws s3 ls ${S3_PATH}/data/${EPOCH_SET}/ |\
		# grab the table names (ending with .csv)
		awk 'BEGIN { FS=" " } match($4, /^([a-z_]+).csv$/, m) { print m[1] }'
	)
  # TODO: Handle no tables in this epoch set

	SAVEIFS=$IFS
	IFS=$'\n'
	# poof, now it's an array separated on newlines
	DB_TABLES=($DB_TABLES)
	IFS=$SAVEIFS

  TABLE_SET_TOTAL=${#DB_TABLES[@]}
	for (( j=0; j<${TABLE_SET_TOTAL}; j++)); do
	# debug just one iteration
	#for (( j=0; j<1; j++)); do
		TABLE_NAME=${DB_TABLES[$j]}
    # adding +1 to i and j going into calc_percentage because the line
    # will show AFTER the work is completed
    TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"message\":\"load span\",\"eIndex\":${i},\"tIndex\":${j},\"progress\":$(calc_percentage ${i+1}.0 ${EPOCH_SET_TOTAL}.0 ${j+1}.0 ${TABLE_SET_TOTAL}.0),\"epoch\":\"${EPOCH_SET}\",\"table\":\"${TABLE_NAME}\",\"real\":%E,\"user\":%U,\"sys\":%S}" 
		time load_table_over_epochs ${EPOCH_SET} ${TABLE_NAME}
	done
done
# TODO: each success should produce a semaphore to prevent it from executing again
