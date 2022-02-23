#!/bin/bash

set -euo pipefail

# variables
export DATABASE_URL=postgres://ww09kd86v1.rn0l1vx697.tsdb.cloud.timescale.com:34392/tsdb?sslmode=require
export BUCKET=fil-archive
export NETWORK=mainnet
export S3_PATH=s3://${BUCKET}/${NETWORK}/csv/1/
export SCHEMA=filltest

calc_percentage () {
  currentYear=$1
  totalYears=$2
  yearPortion="$1/$2"

  currentDump=$3
  totalDumps=$4
  dumpPortion="($3+1)/$4/$2"

  tableProgress=$(printf "%.2f" `echo "((($yearPortion*100) + ($dumpPortion*100)))" | bc -l`)
  echo $tableProgress
}
export -f calc_percentage

ingest_table () {
  thisTable=${1}
  thisTablePath=${S3_PATH}${thisTable}
  thisTableColumns=$(
    aws s3 cp "${thisTablePath}${thisTable/\/}.header" - |\
    # double-quote all fields for SQL safety
    awk 'BEGIN{FS=OFS=","} { for (i=1;i<=NF;i++) {$i="\""$i"\""}}1'
  )

  thisTableYears=$(
    aws s3 ls ${thisTablePath} |\
    awk 'BEGIN { FS=" " } match($2, /([0-9]{4}\/)/, m) { print m[1] }'
  )
  IFS=$'\n' thisTableYears=($thisTableYears)

  #tablesYearsCount=${#thisTableYears[@]}
  tablesYearsCount=1
  for (( yearIndex=0; yearIndex<${tablesYearsCount}; yearIndex++ )); do
    thisTableYear=${thisTableYears[$yearIndex]}
    thisTableYearsPath=${thisTablePath}${thisTableYear}
    thisTableYearsDailyDumps=$(
      aws s3 ls ${thisTableYearsPath} |\
      awk 'BEGIN { FS=" " } { print $4 }'
    )
    IFS=$'\n' thisTableYearsDailyDumps=($thisTableYearsDailyDumps)

    #tableYearsDailyDumpCount=${#thisTableYearsDailyDumps[@]}
    tableYearsDailyDumpCount=1
    for (( dumpIndex=0; dumpIndex<${tableYearsDailyDumpCount}; dumpIndex++ )); do
      strippedTableName=${thisTable/\/}
      csvImportTarget=${thisTableYearsPath}${thisTableYearsDailyDumps[$dumpIndex]}

      TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"progress\": $(calc_percentage $yearIndex $tablesYearsCount $dumpIndex $tableYearsDailyDumpCount) ,\"real\":%E,\"user\":%U,\"sys\":%S,\"message\":\"load csv\",\"table\":\"${tableName}\",\"year\":\"${thisTableYear/\/}\",\"file\":\"${thisTableYearsDailyDumps[$dumpIndex]}\"}"
      time psql --output=/dev/null -b -Xt ${DATABASE_URL} \
        -c "BEGIN TRANSACTION;" \
        -c "CREATE TEMP TABLE temp_${strippedTableName} ON COMMIT DROP AS SELECT * FROM ${SCHEMA}.${strippedTableName} WITH NO DATA;" \
        -c "\\copy temp_${strippedTableName}(${thisTableColumns}) FROM PROGRAM 'aws s3 cp --request-payer requester ${csvImportTarget} - | gzip -dc' DELIMITER ',' CSV;" \
        -c "INSERT INTO ${SCHEMA}.${strippedTableName} SELECT * FROM temp_${strippedTableName} ON CONFLICT DO NOTHING;" \
        -c "COMMIT;" |\
        # Fix error logging edge cases
        sed "s/^\([: a-zA-Z]*\):[[:space:]]*\(.*\)$/{\"time\":\"$(date -u +"%FT%T.000Z")\",\"level\":\"\1\",\"message\":\"\2\"}\n/"
    done
  done
}
export -f ingest_table

# Main Function

# TODO: Fix error logging here
# TODO: Make setup optional based on DB state
TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"message\":\"load schema\",\"real\":%E,\"user\":%U,\"sys\":%S}" \
time psql --output=/dev/null -b -Xt ${DATABASE_URL} -f ./setup_schema.sql |\
  sed "s/^\(.*\):[[:space:]]*\(.*\)$/{\"time\":\"$(date -u +"%FT%T.000Z")\",\"level\":\"\1\",\"message\":\"\2\"}\n/"


dbTables=$(
  # list all available tables
  aws s3 ls ${S3_PATH} |\
  # pull out just the numbered name of the folders (of pattern /[0-9]+__[0-9]+/)
  awk 'BEGIN { FS=" " } match($2, /([a-zA-Z][a-zA-Z0-9_/]+)/, m) { print m[1] }'
)

parallel -j5 ingest_table ::: $dbTables

## put values into enumerable form
## separated on newlines
#IFS=$'\n' dbTables=($dbTables)


#dbTablesStart=0
##dbTablesStopBefore=5
#dbTablesStopBefore=${#dbTables[@]}
#for (( tableIndex=${dbTablesStart}; tableIndex<${dbTablesStopBefore}; tableIndex++ )); do
  #ingestTable $tableIndex $thisTablePath $thisTablesColumns &
#done
#wait

#TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"message\":\"load schema\",\"real\":%E,\"user\":%U,\"sys\":%S}" 
## TODO: Fix error logging here
## TODO: Make setup optional based on DB state
## time psql ${PSQL_OPTIONS[@]} -f ./setup_schema.sql \
##   | sed "s/^\(.*\):[[:space:]]*\(.*\)$/{\"time\":\"$(date -u +"%FT%T.000Z")\",\"level\":\"\1\",\"message\":\"\2\"}\n/"


#EPOCH_SET_TOTAL=${#DB_EPOCH_SETS[@]}
#for (( i=0; i<${EPOCH_SET_TOTAL}; i++ )); do
	#if [ ${i} -lt 376 ]; then continue; fi
## debug just one iteration
##EPOCH_SET_TOTAL=1
##for (( i=0; i<1; i++ )); do
	#EPOCH_SET=${DB_EPOCH_SETS[$i]}
	#DB_TABLES=$(
		## list all available tables
		#aws s3 ls ${S3_PATH}data/${EPOCH_SET}/ |\
		## grab the table names (ending with .csv)
		#awk 'BEGIN { FS=" " } match($4, /^([a-z_]+).csv$/, m) { print m[1] }'
	#)
  ## TODO: Handle no tables in this epoch set

	#SAVEIFS=$IFS
	#IFS=$'\n'
	## poof, now it's an array separated on newlines
	#DB_TABLES=($DB_TABLES)
	#IFS=$SAVEIFS

  #TABLE_SET_TOTAL=${#DB_TABLES[@]}
	#for (( j=0; j<${TABLE_SET_TOTAL}; j++)); do
	## debug just one iteration
	##for (( j=0; j<1; j++)); do
		#TABLE_NAME=${DB_TABLES[$j]}
    ## adding +1 to i and j going into calc_percentage because the line
    ## will show AFTER the work is completed
    #TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"message\":\"load span\",\"eIndex\":${i},\"tIndex\":${j},\"progress\":$(calc_percentage ${i+1}.0 ${EPOCH_SET_TOTAL}.0 ${j+1}.0 ${TABLE_SET_TOTAL}.0),\"epoch\":\"${EPOCH_SET}\",\"table\":\"${TABLE_NAME}\",\"real\":%E,\"user\":%U,\"sys\":%S}" 
		#time load_table_over_epochs ${EPOCH_SET} ${TABLE_NAME}
	#done
#done
## TODO: each success should produce a semaphore to prevent it from executing again
