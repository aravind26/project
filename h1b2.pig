h1b2  = LOAD  '/home/hduser/Downloads/H1B'  USING  PigStorage()  AS (s_no, case_status:chararray, employer_name: chararray, soc_name: chararray, job_title:chararray, full_time_position:chararray,prevailing_wage:long,year:chararray, worksite:chararray, longitute: double, latitute: double );

top = foreach h1b2 generate job_title, year;

--dump top;

top11 = filter top by $1 == '2011';

--dump top11;

top11grpbyjob = GROUP top11 by $0;

--dump top11grpbyjob;

top11count = foreach top11grpbyjob generate group, (DOUBLE)COUNT(top11.year) AS tot;

--dump top11count;

top12 = filter top by $1 == '2012';

--dump top12;

top12grpbyjob = GROUP top12 by $0;

--dump top12grpbyjob;

top12count = foreach top12grpbyjob generate group, (DOUBLE)COUNT(top12.year) AS tot;

--dump top12count;

top1112join = join top11count by $0, top12count by $0;

--dump top1112join;

top1112cal = foreach top1112join generate $0, $1,$3, (DOUBLE)((($3-$1)*100)/$1); 

--dump top1112cal;

top13 = filter top by $1 == '2013';
top13grpbyjob = GROUP top13 by $0;
top13count = foreach top13grpbyjob generate group, (DOUBLE)COUNT(top13.year) AS tot;

top1213join = join top12count by $0, top13count by $0;
top1213cal = foreach top1213join generate $0, $1,$3, (DOUBLE)((($3-$1)*100)/$1);




top14 = filter top by $1 == '2014';
top14grpbyjob = GROUP top14 by $0;
top14count = foreach top14grpbyjob generate group, (DOUBLE)COUNT(top14.year) AS tot;

top1314join = join top13count by $0, top14count by $0;
top1314cal = foreach top1314join generate $0, $1,$3, (DOUBLE)((($3-$1)*100)/$1);



top15 = filter top by $1 == '2015';
top15grpbyjob = GROUP top15 by $0;
top15count = foreach top15grpbyjob generate group, (DOUBLE)COUNT(top15.year) AS tot;

top1415join = join top14count by $0, top15count by $0;
top1415cal = foreach top1415join generate $0, $1,$3, (DOUBLE)((($3-$1)*100)/$1);



top16 = filter top by $1 == '2016';
top16grpbyjob = GROUP top16 by $0;
top16count = foreach top16grpbyjob generate group, (DOUBLE)COUNT(top16.year) AS tot;

top1516join = join top15count by $0, top16count by $0;
top1516cal = foreach top1516join generate $0, $1,$3, (DOUBLE)((($3-$1)*100)/$1);

finjoin = join top1112cal by $0, top1213cal by $0, top1314cal by $0, top1415cal by $0, top1516cal by $0;

fingen = foreach finjoin generate $0, (DOUBLE)(($3+$7+$11+$15+$19)/5);
finlim = limit (order fingen BY $1 desc) 5;

dump finlim;




