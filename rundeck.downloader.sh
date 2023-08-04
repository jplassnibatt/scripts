#!/bin/bash

options="
rundeckpro-enterprise-3.0.9-20181127.war
rundeckpro-enterprise-3.0.11-20181221.war
rundeckpro-enterprise-3.0.12-20190114.war
rundeckpro-enterprise-3.0.13-20190123.war
rundeckpro-enterprise-3.0.14-20190221.war
rundeckpro-enterprise-3.0.15-20190222.war
rundeckpro-enterprise-3.0.16-20190223.war
rundeckpro-enterprise-3.0.17-20190311.war
rundeckpro-enterprise-3.0.18-20190322.war
rundeckpro-enterprise-3.0.19-20190327.war
rundeckpro-enterprise-3.0.20-20190408.war
rundeckpro-enterprise-3.0.21-20190424.war
rundeckpro-enterprise-3.0.22-20190512.war
rundeckpro-enterprise-3.0.23-20190619.war
rundeckpro-enterprise-3.0.24-20190719.war
rundeckpro-enterprise-3.0.25-20190814.war
rundeckpro-enterprise-3.0.26-20190829.war
rundeckpro-enterprise-3.0.27-20191204.war
rundeckpro-enterprise-3.1.0-20190731.war
rundeckpro-enterprise-3.1.1-20190923.war
rundeckpro-enterprise-3.1.2-20190927.war
rundeckpro-enterprise-3.1.3-20191204.war
rundeckpro-enterprise-3.1.4-20191226.war
rundeckpro-enterprise-3.1.5-20200129.war
rundeckpro-enterprise-3.1.6-20200210.war
rundeckpro-enterprise-3.2.0-20191218.war
rundeckpro-enterprise-3.2.1-20200113.war
rundeckpro-enterprise-3.2.2-20200204.war
rundeckpro-enterprise-3.2.3-20200221.war
rundeckpro-enterprise-3.2.4-20200318.war
rundeckpro-enterprise-3.2.5-20200403.war
rundeckpro-enterprise-3.2.6-20200427.war
rundeckpro-enterprise-3.2.7-20200515.war
rundeckpro-enterprise-3.2.8-20200608.war
rundeckpro-enterprise-3.2.9-20200708.war
rundeckpro-enterprise-3.3.0-20200701.war
rundeckpro-enterprise-3.3.1-20200727.war
rundeckpro-enterprise-3.3.2-20200817.war
rundeckpro-enterprise-3.3.3-20200910.war
rundeckpro-enterprise-3.3.4-20201007.war
rundeckpro-enterprise-3.3.5-20201019.war
rundeckpro-enterprise-3.3.6-20201111.war
rundeckpro-enterprise-3.3.7-20201208.war
rundeckpro-enterprise-3.3.8-20210111.war
rundeckpro-enterprise-3.3.9-20210201.war
rundeckpro-enterprise-3.3.10-20210301.war
rundeckpro-enterprise-3.3.11-20210507.war
rundeckpro-enterprise-3.3.12-20210521.war
rundeckpro-enterprise-3.3.13-20210614.war
rundeckpro-enterprise-3.3.14-20210827.war
rundeckpro-enterprise-3.3.15-20211210.war
rundeckpro-enterprise-3.3.16-20211214.war
rundeckpro-enterprise-3.3.17-20211221.war
rundeckpro-enterprise-3.3.18-20220118.war
rundeckpro-enterprise-3.4.0-20210614.war
rundeckpro-enterprise-3.4.1-20210715.war
rundeckpro-enterprise-3.4.2-20210803.war
rundeckpro-enterprise-3.4.3-20210823.war
rundeckpro-enterprise-3.4.4-20210920.war
rundeckpro-enterprise-3.4.5-20211018.war
rundeckpro-enterprise-3.4.6-20211110.war
rundeckpro-enterprise-3.4.7-20211210.war
rundeckpro-enterprise-3.4.8-20211214.war
rundeckpro-enterprise-3.4.9-20211221.war
rundeckpro-enterprise-3.4.10-20220118.war
rundeckpro-enterprise-4.0.0-20220322.war
rundeckpro-enterprise-4.0.1-20220404.war
rundeckpro-enterprise-4.1.0-20220420.war
rundeckpro-enterprise-4.2.0-20220509.war
rundeckpro-enterprise-4.2.1-20220511.war
rundeckpro-enterprise-4.2.2-20220615.war
rundeckpro-enterprise-4.2.3-20220714.war
rundeckpro-enterprise-4.3.0-20220602.war
rundeckpro-enterprise-4.3.1-20220615.war
rundeckpro-enterprise-4.3.2-20220714.war
rundeckpro-enterprise-4.4.0-20220714.war
rundeckpro-enterprise-4.5.0-20220811.war
rundeckpro-enterprise-4.6.0-20220906.war
rundeckpro-enterprise-4.6.1-20220914.war
rundeckpro-enterprise-4.7.0-20221007.war
rundeckpro-enterprise-4.8.0-20221110.war
rundeckpro-enterprise-4.9.0-20230111.war
rundeckpro-enterprise-4.10.0-20230213.war
rundeckpro-enterprise-4.10.1-20230221.war
rundeckpro-enterprise-4.10.2-20230307.war
rundeckpro-enterprise-4.11.0-20230313.war
rundeckpro-enterprise-4.12.0-20230417.war
rundeckpro-enterprise-4.12.1-20230510.war
rundeckpro-enterprise-4.13.0-20230515.war
rundeckpro-enterprise-4.14.0-20230615.war
rundeckpro-enterprise-4.14.1-20230622.war
rundeckpro-enterprise-4.14.2-20230713.war
rundeckpro-enterprise-4.15.0-20230725.war
Exit"

while true; do
    clear
    echo "Menu:"
    i=1
    for option in $options; do
        echo "$i. $option"
        i=$((i+1))
    done

    read -p "Enter your choice (1-$((i-1))): " choice

    if [ "$choice" -ge 1 ] && [ "$choice" -lt $i ] 2>/dev/null; then
        if [ "$choice" -eq $((i-1)) ]; then
            echo "Exiting the script."
            exit 0
        else
            selected_option=$(echo $options | cut -d " " -f $choice)
            wget https://packagecloud.io/pagerduty/rundeckpro/packages/java/com.rundeck.enterprise/$selected_option/artifacts/$selected_option/download -O $selected_option
            break
        fi
    else
        echo "Invalid choice. Please select a valid option (1-$((i-1)))."
        sleep 2
    fi
done
