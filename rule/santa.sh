#!/bin/bash
set -e

cd /etc/bash_etl/crossEntry/rule/
source ../santa.conn
export url="http://$user:$password@$host:8123"
export rows=2000000
if [ -f ./santa.mli ]; then
        export mindt=$(<./santa.mli)
else
        export mindt=$(cat ./initdt.sql | curl $url -sS -d @-)
fi
export maxdt_query=$(cat ./maxdt.sql | sed "s/__pb__/$mindt/g" | sed "s/__rows__/$rows/")
export maxdt=$(echo $maxdt_query | curl $url -sS -d @-)
export log_query=$(cat ./log.sql | sed "s/__pb__/$mindt/" | sed "s/__pe__/$maxdt/")
export query=$(cat ./insert.sql | sed "s/__pb__/$mindt/" | sed "s/__pe__/$maxdt/")
export msg=$(echo $query | curl --write-out '%{http_code}' --output /dev/null --silent $url -d @-)
if [[ "$msg" == "200" ]]; then
        echo $maxdt>./santa.mli
        echo $log_query | curl $url -sS -d @-
else
        echo "Error!!" 1>&2
        exit 64
fi