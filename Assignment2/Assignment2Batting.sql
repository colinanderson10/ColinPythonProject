



USE baseball;

DROP TABLE IF EXISTS totalbattingaverage, annualbattingaverage, rollingbainter, rollingbattingaverage,
	allbattertotalbattingaverage, allbatterannualbattingaverage, allbatterrollingbainter, allbatterrollingbattingaverage;

#For single batter 112736

#The all time batting average for 1 player

CREATE TABLE totalbattingaverage
	SELECT batter, sum(Hit) AS totalhits, sum(atBat) AS totalatbats, sum(Hit)/sum(atBat) AS totalbattingaverage 
	FROM batter_counts 
	WHERE batter=112736
	GROUP BY batter;

#The annual batting average for 1 player

CREATE Table annualbattingaverage
	Select bc.batter, year(g.local_date) as gameyear, sum(bc.Hit) AS annualhits, sum(bc.atBat) AS annualatbats, sum(bc.Hit)/sum(bc.atBat) AS annualbattingaverage
	FROM batter_counts bc
	JOIN game_temp g ON g.game_id=bc.game_id
	WHERE batter=112736
	GROUP BY batter, gameyear;

#Intermediate table to help create the rolling batting averages

CREATE Table rollingbainter
	SELECT bc.game_id, bc.batter, g.local_date as gamedate, DATE_ADD(g.local_date, INTERVAL -100 DAY) as firstdate, DATE_ADD(g.local_date, INTERVAL -1 DAY) as yesterday, bc.Hit as Hit, bc.atBat as atBat
	FROM batter_counts bc
	JOIN game_temp g ON g.game_id=bc.game_id
	WHERE batter=112736 and atBat>0;

#The rolling batting average over the previous 100 days for each game for 1 player
	
CREATE Table rollingbattingaverage
	SELECT rbi0.batter, rbi0.gamedate, rbi0.firstdate, year(rbi0.gamedate) as gameyear, rbi0.Hit as gamehit, rbi0.atBat as gameatbat, sum(rbi1.Hit) AS rollinghits, sum(rbi1.atBat) AS rollingatbats, sum(rbi1.Hit)/sum(rbi1.atBat) AS rollingbattingaverage
	FROM rollingbainter rbi0
	LEFT JOIN rollingbainter rbi1 on rbi0.batter=rbi1.batter
	WHERE rbi1.gamedate BETWEEN rbi0.firstdate AND rbi0.yesterday
	GROUP BY batter, gamedate, firstdate, gamehit, gameatbat
	ORDER BY batter, gamedate, firstdate;


#For all players

CREATE TABLE allbattertotalbattingaverage
	SELECT batter, sum(Hit) AS totalhits, sum(atBat) AS totalatbats, sum(Hit)/sum(atBat) AS totalbattingaverage 
	FROM batter_counts 
	WHERE atBat>0
	GROUP BY batter;

CREATE Table allbatterannualbattingaverage
	Select bc.batter, year(g.local_date) as gameyear, sum(bc.Hit) AS annualhits, sum(bc.atBat) AS annualatbats, sum(bc.Hit)/sum(bc.atBat) AS annualbattingaverage
	FROM batter_counts bc
	JOIN game_temp g ON g.game_id=bc.game_id
	WHERE atBat>0
	GROUP BY batter, gameyear;

#Intermediate table

CREATE Table allbatterrollingbainter
	SELECT bc.game_id, bc.batter, g.local_date as gamedate, DATE_ADD(g.local_date, INTERVAL -100 DAY) as firstdate, DATE_ADD(g.local_date, INTERVAL -1 DAY) as yesterday, bc.Hit as Hit, bc.atBat as atBat
	FROM batter_counts bc
	JOIN game_temp g ON g.game_id=bc.game_id
	WHERE atBat>0;

#This table has the rolling batting average for all players
	
CREATE Table allbatterrollingbattingaverage
	SELECT rbi0.batter, rbi0.gamedate, rbi0.firstdate, year(rbi0.gamedate) as gameyear, rbi0.Hit as gamehit, rbi0.atBat as gameatbat, sum(rbi1.Hit) AS rollinghits, sum(rbi1.atBat) AS rollingatbats, sum(rbi1.Hit)/sum(rbi1.atBat) AS rollingbattingaverage
	FROM allbatterrollingbainter rbi0
	LEFT JOIN allbatterrollingbainter rbi1 on rbi0.batter=rbi1.batter
	WHERE rbi1.gamedate BETWEEN rbi0.firstdate AND rbi0.yesterday
	GROUP BY batter, gamedate, firstdate, gamehit, gameatbat
	ORDER BY batter, gamedate, firstdate;


