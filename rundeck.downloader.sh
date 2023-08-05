#!/bin/bash

options="
rundeck-3.0.9-20181127.war
rundeck-3.0.11-20181221.war
rundeck-3.0.12-20190114.war
rundeck-3.0.13-20190123.war
rundeck-3.0.14-20190221.war
rundeck-3.0.15-20190222.war
rundeck-3.0.16-20190223.war
rundeck-3.0.17-20190311.war
rundeck-3.0.18-20190322.war
rundeck-3.0.19-20190327.war
rundeck-3.0.20-20190408.war
rundeck-3.0.21-20190424.war
rundeck-3.0.22-20190512.war
rundeck-3.0.23-20190619.war
rundeck-3.0.24-20190719.war
rundeck-3.0.25-20190814.war
rundeck-3.0.26-20190829.war
rundeck-3.0.27-20191204.war
rundeck-3.1.0-20190731.war
rundeck-3.1.1-20190923.war
rundeck-3.1.2-20190927.war
rundeck-3.1.3-20191204.war
rundeck-3.1.4-20191226.war
rundeck-3.1.5-20200129.war
rundeck-3.1.6-20200210.war
rundeck-3.2.0-20191218.war
rundeck-3.2.1-20200113.war
rundeck-3.2.2-20200204.war
rundeck-3.2.3-20200221.war
rundeck-3.2.4-20200318.war
rundeck-3.2.5-20200403.war
rundeck-3.2.6-20200427.war
rundeck-3.2.7-20200515.war
rundeck-3.2.8-20200608.war
rundeck-3.2.9-20200708.war
rundeck-3.3.0-20200701.war
rundeck-3.3.1-20200727.war
rundeck-3.3.2-20200817.war
rundeck-3.3.3-20200910.war
rundeck-3.3.4-20201007.war
rundeck-3.3.5-20201019.war
rundeck-3.3.6-20201111.war
rundeck-3.3.7-20201208.war
rundeck-3.3.8-20210111.war
rundeck-3.3.9-20210201.war
rundeck-3.3.10-20210301.war
rundeck-3.3.11-20210507.war
rundeck-3.3.12-20210521.war
rundeck-3.3.13-20210614.war
rundeck-3.3.14-20210827.war
rundeck-3.3.15-20211210.war
rundeck-3.3.16-20211214.war
rundeck-3.3.17-20211221.war
rundeck-3.3.18-20220118.war
rundeck-3.4.0-20210614.war
rundeck-3.4.1-20210715.war
rundeck-3.4.2-20210803.war
rundeck-3.4.3-20210823.war
rundeck-3.4.4-20210920.war
rundeck-3.4.5-20211018.war
rundeck-3.4.6-20211110.war
rundeck-3.4.7-20211210.war
rundeck-3.4.8-20211214.war
rundeck-3.4.9-20211221.war
rundeck-3.4.10-20220118.war
rundeck-4.0.0-20220322.war
rundeck-4.0.1-20220404.war
rundeck-4.1.0-20220420.war
rundeck-4.2.0-20220509.war
rundeck-4.2.1-20220511.war
rundeck-4.2.2-20220615.war
rundeck-4.2.3-20220714.war
rundeck-4.3.0-20220602.war
rundeck-4.3.1-20220615.war
rundeck-4.3.2-20220714.war
rundeck-4.4.0-20220714.war
rundeck-4.5.0-20220811.war
rundeck-4.6.0-20220906.war
rundeck-4.6.1-20220914.war
rundeck-4.7.0-20221007.war
rundeck-4.8.0-20221110.war
rundeck-4.9.0-20230111.war
rundeck-4.10.0-20230213.war
rundeck-4.10.1-20230221.war
rundeck-4.10.2-20230307.war
rundeck-4.11.0-20230313.war
rundeck-4.12.0-20230417.war
rundeck-4.12.1-20230510.war
rundeck-4.13.0-20230515.war
rundeck-4.14.0-20230615.war
rundeck-4.14.1-20230622.war
rundeck-4.14.2-20230713.war
rundeck-4.15.0-20230725.war
Add_a_new_version_to_the_list
Exit"

function add_new_option() {
    echo -n "Enter a new version name: "
    read new_option
    options=$(echo "$options" | sed -i '/^Add_a_new_version_to_the_list/i '"$new_option"'' $0)
    echo "New option added successfully!"
    sleep 2
    $0
    exit 0
}
function display_menu() {
    clear
    echo "Menu:"
    i=1
    for option in $options; do
        echo "$i. $option"
        i=$((i+1))
    done
}

function is_numeric() {
    [[ "$1" =~ ^[0-9]+$ ]]
}

while true; do
    display_menu

    read -p "Enter your choice (1-$((i-1))): " choice

    if is_numeric "$choice" && [ "$choice" -ge 1 ] && [ "$choice" -lt $i ] 2>/dev/null; then
        if [ "$choice" -eq $((i-1)) ]; then
            echo "Exiting the script."
            exit 0
        elif [ "$choice" -eq $((i-2)) ]; then
            add_new_option
        else
            selected_option=$(echo $options | cut -d " " -f $choice)
            wget https://packagecloud.io/pagerduty/rundeck/packages/java/org.rundeck/$selected_option/artifacts/$selected_option/download -O $selected_option
            break
        fi
    else
        echo "Invalid choice. Please select a valid option (1-$((i-1)))."
        sleep 2
    fi
done
