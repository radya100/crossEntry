#!/bin/bash

cd /etc/bash_etl/crossEntry/bonus/ || return
source $1
# shellcheck disable=SC2154
export url="http://$user:$password@$host:8123"
export url2="$url?query=insert%20into%20service.qwe%20format%20Values"
export rows=2000000
if [ -f $state_file ]; then
        export pb=$(<$state_file)
else
        export pb=$(cat ./initdt.sql | curl $url -sS -d @-)
fi
export pe_query=$(cat ./get_pe_where_rows.sql | sed "s/__pb__/$pb/g" | sed "s/__rows__/$rows/")
export pe=$(echo $pe_query | curl $url -sS -d @-)

#prepare set for fill
# shellcheck disable=SC2046
echo $(cat ./data_prepare_keys.sql | sed "s/__pb__/$pb/g" | sed "s/__pe__/$pe/") | curl $url -sS -d @-

#calculate showcase
export msg=$(echo $(<./data_select.sql) | curl $url -sS -d @- | curl --write-out '%{http_code}' --output /dev/null --silent $url2 -d @-)

if [[ "$msg" == "200" ]]; then
        echo $pe>$state_file
        export log_query=$(cat ./log.sql | sed "s/__pb__/$pb/" | sed "s/__pe__/$pe/")
        echo $log_query | curl $url -sS -d @-
else
        echo "Error!!" 1>&2
        exit 64
fi
