#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************APP MENU***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1) ${MENU} 1a) Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2) ${MENU} 1b) Find top 5 job titles who are having highest avg growth in applications.[ALL] [no sub menu is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} 2a) Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} 2b) find top 5 locations in the US who have got certified visa for each year.[certified] [sub menu - year option is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} 3) Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified] [no sub menu is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} 4) Which top 5 employers file the most petitions each year? - Case Status - ALL [sub menu - year option is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7)${MENU} 5a) Find the most popular top 10 job positions for H1B visa applications for each year? a) for all the applications [second sub menu - year option is required]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} 5b) Find the most popular top 10 job positions for H1B visa applications for each year?
b) for only certified applications [second sub menu - year option is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} 6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time. [no sub menu is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} 7) Create a bar graph to depict the number of applications for each year [All] [no sub menu is required] ${NORMAL}"
    
    echo -e "${MENU}**${NUMBER} 11)${MENU} 8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.] [Top 20 only required] [first sub menu - full time OR part time] [second sub menu - year option is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12)${MENU} 9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ? Display the values in descending order of success rate.[no sub menu is required] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13)${MENU} 10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)? Display the values in descending order of success rate.[no sub menu is required] ${NORMAL}"
     echo -e "${MENU}**${NUMBER} 14)${MENU} 11) Export result for question no 10 to MySql database ${NORMAL}"
     echo -e "${MENU}*********************************************${NORMAL}"
     echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
     read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

function getpinCodeBank(){
	echo "in getPinCodebank"
	echo $1
	echo $2
	#hive -e "Select * from AppData where PinCode = $1 AND Bank = '$2'"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
	   echo "Enter output path"
        read path
        echo "You've selected ${path}"
	
        hadoop jar H1B.jar OT /user/hive/warehouse/h1b.db/h1b_final /niit/out1
        hadoop fs -cat /niit/out1/p*
        hadoop fs -rm -r /niit/out1
show_menu;     
        ;;

        2) clear;
         pig -x local "h1b1b.pig"
         show_menu;
        ;;
    
        
        3) clear;
       
            hive -e "create table site as select COUNT(job_title) as most,job_title,worksite,year from H1B.h1b_final where job_title='DATA ENGINEER' group by job_title,worksite,year; 

	select * from site a where most in (select max(most) from site b where b.year=a.year) order by a.year; " 
        show_menu;
        ;;

       4) clear;
       echo "Enter the year from 2011 to 2016"
        read year
        echo "You've selected ${year}"
	          
            hadoop jar H1B.jar top5 /user/hive/warehouse/h1b.db/h1b_final /niit/2bout 
            hadoop fs -cat /niit/2bout/p*
            hadoop fs -rm -r /niit/2bout
        show_menu;
        ;;
    
        5) clear;
        pig -x local "h1b2.pig"
        show_menu;     
        ;;	
        6) clear;
        
        echo "Enter year from 2011 to 2016"
        read year
        echo "You've selected ${year}"
	    hive -e "select employer_name, count(*) as total from H1B.h1b_final where year = $year group by employer_name order by total desc limit 5;" 
        show_menu;
        ;;
            
	    7) clear;
        echo "Enter year from 2011 to 2016"
        read year
        echo "You've selected ${year}"
	    hive -e "select year, job_title, count(*) as job from H1B.H1b_final where year=$year group by year, job_title order by job desc limit 10" 
        show_menu;
        ;;

                       
        8) clear;
        echo "Enter the year from 2011 to 2016"
        read year
        echo "You've selected ${year}"
           hive -e "select year, job_title, count(*) as job from H1b.h1b_final where year=$year and lower(case_status)='CERTIFIED' group by year, job_title order by job desc limit 10"
        show_menu;
        ;;
              
       9) clear;
     pig -x local "h1b.pig"
show_menu;
       ;;
                
       10) clear;

echo "Enter output path"
        read path
        echo "You've selected ${path}"
	
     hadoop jar H1B.jar Applications /user/hive/warehouse/h1b.db/h1b_final /niit/app
     hadoop fs -cat /niit/app/p*
     hadoop fs -rm -r /niit/app
show_menu;     
;;

       11) clear;
     pig -x local "h1b8.pig"
        show_menu;
        ;;                     

       12) clear;
     pig -x local "h1b9.pig"
show_menu;
       ;;
                
       13) clear;
     pig -x local "h1b10.pig"
show_menu;
         
;;
       14) clear;
sqoop export --connect jdbc:mysql://localhost/project --username root --password 'sana' --table hten --update-mode  allowinsert --update-key job_title   --export-dir /niit/h1b1/part-r-00000 --input-fields-terminated-by '\t';
        mysql -uroot -p ''  -e "use project;" 
        mysql -uroot -p ''  -e "select * from project.hten;" 
        mysql -uroot -p ''  -e "delete from project.hten;"
show_menu;
;;
          *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi
done


