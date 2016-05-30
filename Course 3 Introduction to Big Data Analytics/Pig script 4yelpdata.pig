-- *******   PIG LATIN SCRIPT for Yelp Assignmet ******************
-- unix > head -1 /usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv
-- unix> hdfs dfs -copyFromLocal /usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv /user/cloudera/input/yelp_data.csv

-- 0. get function defined for CSV loader

register /usr/lib/pig/piggybank.jar;
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

-- The data-fu jar file has a CSVLoader with more options, like reading multiline records,
--  but for this assignment we don't need it, so the next line is commented out
-- register /home/cloudera/incubator-datafu/datafu-pig/build/libs/datafu-pig-incubating-1.3.0-SNAPSHOT.jar;

-- 1 load data

Y      = LOAD '/usr/lib/hue/apps/search/examples/collections/solr_configs_yelp_demo/index_data.csv' USING CSVLoader() AS(business_id:chararray,cool,date,funny,id,stars:int,text:chararray,type,useful:int,user_id,name,full_address,latitude,longitude,neighborhoods,open,review_count,state);
Y_good = FILTER Y BY (useful is not null and stars is not null);

--2 Find max useful
Y_all = GROUP Y_good ALL;
Umax  = FOREACH Y_all GENERATE MAX(Y_good.useful);
DUMP Umax

-- this shows max useful rating of 28! ...

-- 3 Now limit useful field to be <=5 and then get wtd average

Y_rate  = FOREACH Y_good GENERATE business_id,stars,(useful>5 ? 5:useful) as useful_clipped;
Y_rate2 = FOREACH Y_rate GENERATE $0..,(double) stars*(useful_clipped/5) as wtd_stars;

-- Note: a student noticed the divisor above should be 5.0 so that it is cast as a float number 
-- but that doesn't really change the gist of the assignment and quiz (as he also noted), so I'll leave it as just 5

-- 4 Rank businesses

Y_g = GROUP Y_rate2 BY business_id;
Y_m = FOREACH Y_g
      GENERATE group as business_idgroup,COUNT(Y_rate2.stars) as num_ratings ,
          AVG(Y_rate2.stars) as avg_stars,
          AVG(Y_rate2.useful_clipped) as avg_useful,
          AVG(Y_rate2.wtd_stars) as avg_wtdstars;
         
Y_rnk = RANK Y_m BY avg_wtdstars;


---Quiz Code
Y_m2 = FILTER Y_m BY (num_ratings > 1);
Y_g2 = GROUP Y_m2 ALL;
Y_m2avg = FOREACH Y_g2 GENERATE AVG(Y_m2.avg_wtdstars);

Y_rate_join = JOIN Y_rate2 BY business_id, Y_m2 BY business_idgroup;
All_Y_rate = GROUP Y_rate_join ALL;
Y_rate_avg = FOREACH All_Y_rate GENERATE AVG(Y_rate_join.Y_rate2::wtd_stars);

Ygf = FOREACH Y_g GENERATE COUNT(Y_rate2.stars) as num_rated, Y_rate2.wtd_stars as wtd_stars2use;
Ygf_gt1 = FILTER Ygf BY num_rated>1;
Y_next = FOREACH Ygf_gt1 GENERATE FLATTEN(wtd_stars2use) AS your_filled_in_field;
Yg2 = GROUP Y_next ALL;
A2 = FOREACH Yg2 GENERATE AVG(your_filled_in_field);
