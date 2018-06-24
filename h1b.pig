h1b = LOAD  '/home/hduser/Downloads/H1B'  USING  PigStorage()  AS (s_no, case_status:chararray, employer_name: chararray, soc_name: chararray, job_title:chararray, full_time_position:chararray,prevailing_wage:long,year:chararray, worksite:chararray, lon,lat);

sixgen = foreach h1b generate case_status,year;

--dump sixgen;

sixgengrp = GROUP sixgen all;

--dump sixgengrp;

sixcount = foreach sixgengrp generate COUNT(sixgen.case_status), COUNT(sixgen.case_status) - COUNT(sixgen.case_status);

--dump sixcount;

sixyr12 = filter sixgen by $1 == '2012';

--dump sixyr12;

six12grp = GROUP sixyr12 by case_status;

--dump six12grp;

six12count = foreach six12grp generate group, COUNT(sixyr12.year), COUNT(sixyr12.year) - COUNT(sixyr12.year);

--dump six12count;

six12join = join six12count by $2, sixcount by $1;

--dump six12join;

six12percent = foreach six12join generate $0, (DOUBLE)$1, (DOUBLE)$3;

--dump six12percent;

six12pergen = foreach six12percent generate $0, $1, $2, (DOUBLE)(($1*100)/$2); 

dump six12pergen;


