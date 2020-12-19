
USE baseball;

CREATE TABLE IF NOT EXISTS allbatterrollingbainter
	SELECT bc.game_id, bc.batter, g.local_date as gamedate, DATE_ADD(g.local_date, INTERVAL -100 DAY) as firstdate, DATE_ADD(g.local_date, INTERVAL -1 DAY) as yesterday, bc.Hit as Hit, bc.atBat as atBat
	FROM batter_counts bc
	JOIN game g ON g.game_id=bc.game_id
	WHERE atBat>0;

CREATE TABLE IF NOT EXISTS allbatterrollingbattingaverage
	SELECT rbi0.batter, rbi0.gamedate, rbi0.firstdate, year(rbi0.gamedate) as gameyear, rbi0.Hit as gamehit, rbi0.atBat as gameatbat, sum(rbi1.Hit) AS rollinghits, sum(rbi1.atBat) AS rollingatbats, sum(rbi1.Hit)/sum(rbi1.atBat) AS rollingbattingaverage
	FROM allbatterrollingbainter rbi0
	LEFT JOIN allbatterrollingbainter rbi1 on rbi0.batter=rbi1.batter
	WHERE rbi1.gamedate BETWEEN rbi0.firstdate AND rbi0.yesterday
	GROUP BY batter, gamedate, firstdate, gamehit, gameatbat
	ORDER BY batter, gamedate, firstdate;

CREATE TABLE IF NOT EXISTS gameplayers
    SELECT l.game_id, l.player_id, l.batting_order, bc.batter, g.local_date as gamedate, DATE_ADD(g.local_date, INTERVAL -100 DAY) as firstdate, DATE_ADD(g.local_date, INTERVAL -1 DAY) as yesterday, bc.Hit as Hit, bc.atBat as atBat
    FROM lineup l
    JOIN game g on g.game_id=l.game_id
    JOIN batter_counts bc on bc.game_id=l.game_id
    WHERE l.game_id = 12560 AND l.batting_order>0 AND bc.atbat>0;

CREATE TABLE IF NOT EXISTS gamerollingbattingaverage
    SELECT gp.batter, gp.gamedate, gp.firstdate, gp.Hit as gamehit, gp.atBat as gameatbat, sum(rbi1.Hit) AS rollinghits, sum(rbi1.atBat) AS rollingatbats, sum(rbi1.Hit)/sum(rbi1.atBat) AS rollingbattingaverage
	FROM gameplayers gp
	LEFT JOIN allbatterrollingbainter rbi1 on gp.batter=rbi1.batter
	WHERE rbi1.gamedate BETWEEN gp.firstdate AND gp.yesterday
	GROUP BY batter, gamedate, firstdate, gamehit, gameatbat
	ORDER BY batter, gamedate, firstdate;
