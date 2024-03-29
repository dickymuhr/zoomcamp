set -e
TAXI_TYPE=$1 #"yellow"
YEAR=$2 #2020

# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in $(seq 1 12); do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"

    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    mkdir -p ${LOCAL_PREFIX}
    echo wget ${URL} -O ${LOCAL_PATH}
    wget ${URL} -O ${LOCAL_PATH}

done

# sh download_data.sh yellow 2021