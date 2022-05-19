#!/bin/bash

set -euo pipefail

#exitcodes
E_HELP=1
E_BADARGS=2

# variables
export DATABASE_URL=postgresql://postgres:postgres@localhost/defaultdb
export BUCKET=fil-archive
export NETWORK=mainnet
export S3_PATH=s3://${BUCKET}/${NETWORK}/csv/1/
export SCHEMA=lily

# reasonable argument defaults
# TODO: Extract all DEBUG checks into an inlined exec function
export DEBUG=false
export PROCESS_MIGRATION=false
export PROCESS_PARALLEL=
# TODO: Support date ranges on a model-by-model basis
export sanitizedStart=0 # smallest possible date
export sanitizedEnd=99991231 # largest possible date

usage() {
	cat <<-EOF
		
		Usage: $0 [-hdi] [-c DATABASEURL] [-s STARTDATE] [-e ENDDATE] [-p PARALLEL_INSERTS]
		
		  -b  a begin date from which to begin importing data (inclusive) (default: $sanitizedStart)
		      Begin condition is checked internally by parsing arg with
		      'date -d ARG +%Y%m%d' and comparing ARCHIVE_DATE >= ARG. A
		      successful check will attempt to import that archive.
		  -c  specify the postgres DB to connect to (default: $DATABASE_URL)
		  -d  enable debugging to show script state and executed messages (default: $DEBUG)
		  -e  an end date to which importing data will process up until (inclusive) (default: $sanitizedEnd)
		      End condition is checked internally by parsing arg with
		      'date -d ARG +%Y%m%d' and comparing ARCHIVE_DATE <= ARG. A
		      successful check will attempt to import that archive.
		      successful check will attempt to import that archive.
		  -h  show this usage information
		  -i  initialize the database by applying lily's initial schema migration (default: $PROCESS_MIGRATION)
		  -p  the number of parallel threads to ingest tables (default: number of cores).
		      Parallel ingestion processes one table per thread and then begins the next table. This
		      is useful for throttling the data being pushed into the database
		  -s  the schema that should be populated with data (default: lily)
		
		
	EOF
  exit $E_HELP
}
export -f usage


# arguments setup/validation
while getopts ":b:c:e:hip:s:d" opt; do
  case $opt in
    b)
      sanitizedStart=$(date -d "$OPTARG" +%Y%m%d)
      if [ $? -ne 0 ]; then
        echo "ERROR: Invalid begin date provided to -$opt: $OPTARG"
        exit $E_BADARGS
      fi
      export sanitizedStart=$sanitizedStart
      ;;
    c)
      echo ""
      echo "INFO: Importing into '$OPTARG'..."
      export DATABASE_URL=$OPTARG
      ;;
    d)
      echo ""
      echo "INFO: Performing a dry run. No changes are being applied."
      echo ""
      export DEBUG=true
      ;;
    e)
      sanitizedEnd=$(date -d "$OPTARG" +%Y%m%d)
      if [ $? -ne 0 ]; then
        echo "ERROR: Invalid end date provided to -$opt: $OPTARG"
        exit $E_BADARGS
      fi
      export sanitizedEnd=$sanitizedEnd
      ;;
    h)
      usage
      exit $E_HELP
      ;;
    i)
      echo "INFO: Creating default schema"
      export PROCESS_MIGRATION=true
      ;;
    p)
      if [ $OPTARG -lt 0 ] || [ $OPTARG -gt 10 ]; then
        echo "ERROR: Parallel execution option -$opt should be between 0-10"
        exit $E_BADARGS
      fi
      export PROCESS_PARALLEL=-j$OPTARG
      ;;
    s)
      export SCHEMA=$opt
      echo "INFO: Using schema '$SCHEMA'"
      ;;
    :)
      echo "options -$OPTARG requires an argument"
      exit $E_BADARGS
      ;;
  esac
done

if [ $sanitizedStart -gt $sanitizedEnd ]; then
  echo "ERROR: Start must be before end"
  exit $E_BADARGS
fi

# test connectivity
echo "INFO: Testing connection to database at '$DATABASE_URL'..."
psql -bqX ${DATABASE_URL} -c "SELECT;"

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

enable_debug_opt () {
  if [[ $DEBUG == true ]]; then
    PS4='$LINENO: '
    set -x
  fi
}
export -f enable_debug_opt

return_on_fail () {
  if [[ $1 != 0 ]]; then
    return
  fi
}
export -f return_on_fail

ingest_table () {
  enable_debug_opt
  thisTable=${1}

  thisTablePath=${S3_PATH}${thisTable}
  thisTableColumns=$(
    aws s3 cp "${thisTablePath}${thisTable/\/}.header" - |\
    # double-quote all fields for SQL safety
    awk 'BEGIN{FS=OFS=","} { for (i=1;i<=NF;i++) {$i="\""$i"\""}}1'
    return_on_fail $?
  )

  thisTableYears=$(
    aws s3 ls ${thisTablePath} |\
    awk 'BEGIN { FS=" " } match($2, /([0-9]{4}\/)/, m) { print m[1] }'
    return_on_fail $?
  )
  IFS=$'\n' thisTableYears=($thisTableYears)

  tablesYearsCount=${#thisTableYears[@]}
  #tablesYearsCount=1
  for (( yearIndex=0; yearIndex<${tablesYearsCount}; yearIndex++ )); do
    thisTableYear=${thisTableYears[$yearIndex]}
    thisTableYearsPath=${thisTablePath}${thisTableYear}
    thisTableYearsDailyDumps=$(
      aws s3 ls ${thisTableYearsPath} |\
      awk 'BEGIN { FS=" " } $3>20 { print $4; }'
    )
    IFS=$'\n' thisTableYearsDailyDumps=($thisTableYearsDailyDumps)

    tableYearsDailyDumpCount=${#thisTableYearsDailyDumps[@]}
    #tableYearsDailyDumpCount=1
    for (( dumpIndex=0; dumpIndex<${tableYearsDailyDumpCount}; dumpIndex++ )); do
      strippedTableName=${thisTable/\/}
      csvImportTarget=${thisTableYearsPath}${thisTableYearsDailyDumps[$dumpIndex]}

      dumpDate="$(echo "${thisTableYearsDailyDumps[$dumpIndex]}" | sed -E 's/.*-([0-9]{4})-([0-9]{2})-([0-9]{2}).csv.gz/\1\2\3/')"

      if [[ $dumpDate -ge $(date -d $sanitizedStart +"%Y%m%d") || $dumpDate -le $(date -d $sanitizedEnd +"%Y%M%D") ]]; then
        continue
      fi

      if [ $DEBUG == "true" ]; then
        echo "DEBUG: Insert CSV payload '${csvImportTarget}'"
      fi

      TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"progress\": $(calc_percentage $yearIndex $tablesYearsCount $dumpIndex $tableYearsDailyDumpCount),\"real\": %E,\"user\": %U,\"sys\": %S,\"message\":\"load csv\",\"table\":\"${thisTable}\",\"year\":\"${thisTableYear/\/}\",\"file\":\"${csvImportTarget}\"}"
      time psql -bqX ${DATABASE_URL} \
        -c "BEGIN TRANSACTION;" \
        -c "CREATE TEMP TABLE temp_${strippedTableName} ON COMMIT DROP AS SELECT * FROM ${SCHEMA}.${strippedTableName} WITH NO DATA;" \
        -c "\\copy temp_${strippedTableName}(${thisTableColumns}) FROM PROGRAM 'aws s3 cp --request-payer requester ${csvImportTarget} - | gzip -dc' DELIMITER ',' CSV;" \
        -c "INSERT INTO ${SCHEMA}.${strippedTableName} SELECT * FROM temp_${strippedTableName} ON CONFLICT DO NOTHING;" \
        -c "COMMIT;" |\
        # Fix error logging edge cases
        sed "s/^\([: a-zA-Z]*\):[[:space:]]*\(.*\)$/{\"time\":\"$(date -u +"%FT%T.000Z")\",\"level\":\"\1\",\"message\":\"\2\"}\n/"

      if [[ $? -ne 0 ]]; then
        echo `{"error":"failed to import","file":"${csvImportTarget}"}`
      fi

    done
  done
}
export -f ingest_table

# Main Function

enable_debug_opt
if [ $PROCESS_MIGRATION == "true" ]; then
  # TODO: Fix psql error logging here
  # TODO: Make setup optional based on DB state instead of flag checks
  TIMEFORMAT="{\"time\":\"$(date -u +"%FT%T.000Z")\",\"message\":\"load schema\",\"real\":%E,\"user\":%U,\"sys\":%S}"
  time psql -bqX ${DATABASE_URL} -c "CREATE SCHEMA IF NOT EXISTS ${SCHEMA}"
  time psql -bqX ${DATABASE_URL} -f ./setup_schema.sql |\
    sed "s/^\(.*\):[[:space:]]*\(.*\)$/{\"time\":\"$(date -u +"%FT%T.000Z")\",\"level\":\"\1\",\"message\":\"\2\"}\n/"
fi

dbTables=$(
  # list all available tables
  aws s3 ls ${S3_PATH} |\
  awk 'BEGIN { FS=" " } match($2, /([a-zA-Z][a-zA-Z0-9_/]+)/, m) { print m[1] }'
)

if [ $DEBUG == "true" ]; then
  # hide echo
  echo "DEBUG: start: $sanitizedStart end: $sanitizedEnd p: $PROCESS_PARALLEL"
fi

parallel --line-buffer --delay 5 ${PROCESS_PARALLEL} ingest_table ::: $dbTables

#ingest_table actors/
