
USE baseball;

CREATE TABLE IF NOT EXISTS batter_counts_pitcher_date
	SELECT bc.*,
	pc.pitcher,
	g.local_date,
	DATE_ADD(g.local_date, INTERVAL -100 DAY) as firstdate,
	DATE_ADD(g.local_date, INTERVAL -1 DAY) as yesterday
	FROM batter_counts bc
	LEFT JOIN pitcher_counts pc
		ON bc.game_id=pc.game_id
		AND bc.team_id!=pc.team_id
	LEFT JOIN game g
	    ON bc.game_id=g.game_id
	WHERE (g.`type`="R" OR g.`type`="D" OR g.`type`="L" OR g.`type`="W")
	And pc.startingPitcher=1;

#create overall batting stats for each player in all game prior to specified game

CREATE TABLE IF NOT EXISTS batting_history
    SELECT bc.batter,
    bc.game_id,
    bc.team_id,
    bc.homeTeam,
    sum(bc2.Hit) AS Hits,
	sum(bc2.atBat) AS AtBats,
	sum(bc2.plateApperance) AS PlateAppearance,
	sum(bc2.Walk+bc2.Intent_Walk) AS Walks,
	sum(bc2.Hit_By_Pitch) AS HitByPitch,
	sum(bc2.Sac_Fly) AS SacflyAgainst,
	sum(bc2.Single) AS SingleAgainst,
	sum(bc2.Double) AS DoubleAgainst,
	sum(bc2.Triple) AS TripleAgainst,
	sum(bc2.Home_Run) AS HomerunAgainst,
	sum(bc2.Strikeout+bc2.`Strikeout_-_DP`+bc2.`Strikeout_-_TP`) AS StrikeOutAgainst,
	sum(bc2.Double_Play) AS DoublePlayAgainst,
	sum(bc2.Triple_Play) AS TriplePlayAgainst
	FROM batter_counts_pitcher_date bc
	LEFT JOIN batter_counts_pitcher_date bc2 on bc.batter=bc2.batter
	WHERE bc.local_date>bc2.local_date
	GROUP BY batter, game_id, homeTeam, team_id;



#create overall batting stats for each player against the opposing starting pitcher in all game prior to specified game

CREATE TABLE IF NOT EXISTS batting_against_pitcher
    SELECT bc.batter as batter2,
    bc.game_id as game_id2,
    sum(bc2.Hit) AS HitsAgainstPitcher,
    sum(bc2.atBat) AS AtBatsAgainstPitcher,
    sum(bc2.plateApperance) AS PlateAppearanceAgainstPitcher,
    sum(bc2.Walk+bc2.Intent_Walk) AS WalksAgainstPitcher,
    sum(bc2.Hit_By_Pitch) AS HitByPitchPitcher,
    sum(bc2.Sac_Fly) AS SacflyAgainstPitcher,
    sum(bc2.Single) AS SingleAgainstPitcher,
    sum(bc2.Double) AS DoubleAgainstPitcher,
    sum(bc2.Triple) AS TripleAgainstPitcher,
    sum(bc2.Home_Run) AS HomerunAgainstPitcher,
    sum(bc2.Strikeout+bc2.`Strikeout_-_DP`+bc2.`Strikeout_-_TP`) AS StrikeOutAgainstPitcher,
    sum(bc2.Double_Play) AS DoublePlayAgainstPitcher,
    sum(bc2.Triple_Play) AS TriplePlayAgainstPitcher
	FROM batter_counts_pitcher_date bc
	LEFT JOIN batter_counts_pitcher_date bc2 on bc.batter=bc2.batter
	WHERE bc.local_date>bc2.local_date
	AND bc.pitcher=bc2.pitcher
	GROUP BY batter2, game_id2;

#Create recent batting stats for 100 days prior to the game

CREATE Table IF NOT EXISTS recent_batting
	SELECT bc.batter as batter3,
    bc.game_id as game_id3,
    sum(bc2.Hit) AS RecentHits,
    sum(bc2.atBat) AS RecentAtBats,
    sum(bc2.plateApperance) AS RecentPlateAppearance,
    sum(bc2.Walk+bc2.Intent_Walk) AS RecentWalks,
    sum(bc2.Hit_By_Pitch) AS RecentHitByPitch,
    sum(bc2.Sac_Fly) AS RecentSacfly,
    sum(bc2.Single) AS RecentSingle,
    sum(bc2.Double) AS RecentDouble,
    sum(bc2.Triple) AS RecentTriple,
    sum(bc2.Home_Run) AS RecentHomerun,
    sum(bc2.Strikeout+bc2.`Strikeout_-_DP`+bc2.`Strikeout_-_TP`) AS RecentStrikeOut,
    sum(bc2.Double_Play) AS RecentDoublePlay,
    sum(bc2.Triple_Play) AS RecentTriplePlay
	FROM batter_counts_pitcher_date bc
	LEFT JOIN batter_counts_pitcher_date bc2 on bc.batter=bc2.batter
	WHERE (bc2.local_date BETWEEN bc.firstdate AND bc.yesterday)
	GROUP BY batter3, game_id3;

Create Table IF NOT EXISTS BattingJoin
	Select b.*,
	CASE WHEN b2.HitsAgainstPitcher is not null then b2.HitsAgainstPitcher ELSE b.Hits END AS HitsAgainstPitcher,
    CASE WHEN b2.AtBatsAgainstPitcher is not null then b2.AtBatsAgainstPitcher ELSE b.AtBats END AS AtBatsAgainstPitcher,
    CASE WHEN b2.PlateAppearanceAgainstPitcher is not null then b2.PlateAppearanceAgainstPitcher ELSE b.PlateAppearance END AS PlateAppearanceAgainstPitcher,
    CASE WHEN b2.WalksAgainstPitcher is not null then b2.WalksAgainstPitcher ELSE b.Walks END AS WalksAgainstPitcher,
    CASE WHEN b2.HitByPitchPitcher is not null then b2.HitByPitchPitcher ELSE b.HitByPitch END AS HitByPitchPitcher,
    CASE WHEN b2.SacflyAgainstPitcher is not null then b2.SacflyAgainstPitcher ELSE b.SacflyAgainst END AS SacflyAgainstPitcher,
    CASE WHEN b2.SingleAgainstPitcher is not null then b2.SingleAgainstPitcher ELSE b.SingleAgainst END AS SingleAgainstPitcher,
    CASE WHEN b2.DoubleAgainstPitcher is not null then b2.DoubleAgainstPitcher ELSE b.DoubleAgainst END AS DoubleAgainstPitcher,
    CASE WHEN b2.TripleAgainstPitcher is not null then b2.TripleAgainstPitcher ELSE b.TripleAgainst END AS TripleAgainstPitcher,
    CASE WHEN b2.HomerunAgainstPitcher is not null then b2.HomerunAgainstPitcher ELSE b.HomerunAgainst END AS HomerunAgainstPitcher,
    CASE WHEN b2.StrikeOutAgainstPitcher is not null then b2.StrikeOutAgainstPitcher ELSE b.StrikeOutAgainst END AS StrikeOutAgainstPitcher,
    CASE WHEN b2.DoublePlayAgainstPitcher is not null then b2.DoublePlayAgainstPitcher ELSE b.DoublePlayAgainst END AS DoublePlayAgainstPitcher,
    CASE WHEN b2.TriplePlayAgainstPitcher is not null then b2.TriplePlayAgainstPitcher ELSE b.TriplePlayAgainst END AS TriplePlayAgainstPitcher,
	CASE WHEN b3.RecentHits is not null then b3.RecentHits ELSE b.Hits END AS RecentHits,
    CASE WHEN b3.RecentAtBats is not null then b3.RecentAtBats ELSE b.AtBats END AS RecentAtBats,
    CASE WHEN b3.RecentPlateAppearance is not null then b3.RecentPlateAppearance ELSE b.PlateAppearance END AS RecentPlateAppearance,
    CASE WHEN b3.RecentWalks is not null then b3.RecentWalks ELSE b.Walks END AS RecentWalks,
    CASE WHEN b3.RecentHitByPitch is not null then b3.RecentHitByPitch ELSE b.HitByPitch END AS RecentHitByPitch,
    CASE WHEN b3.RecentSacfly is not null then b3.RecentSacfly ELSE b.SacflyAgainst END AS RecentSacfly,
    CASE WHEN b3.RecentSingle is not null then b3.RecentSingle ELSE b.SingleAgainst END AS RecentSingle,
    CASE WHEN b3.RecentDouble is not null then b3.RecentDouble ELSE b.DoubleAgainst END AS RecentDouble,
    CASE WHEN b3.RecentTriple is not null then b3.RecentTriple ELSE b.TripleAgainst END AS RecentTriple,
    CASE WHEN b3.RecentHomerun is not null then b3.RecentHomerun ELSE b.HomerunAgainst END AS RecentHomerun,
    CASE WHEN b3.RecentStrikeOut is not null then b3.RecentStrikeOut ELSE b.StrikeOutAgainst END AS RecentStrikeOut,
    CASE WHEN b3.RecentDoublePlay is not null then b3.RecentDoublePlay ELSE b.DoublePlayAgainst END AS RecentDoublePlay,
    CASE WHEN b3.RecentTriplePlay is not null then b3.RecentTriplePlay ELSE b.TriplePlayAgainst END AS RecentTriplePlay
	FROM batting_history b
	LEFT JOIN batting_against_pitcher b2 on b.batter=b2.batter2 AND b.game_id=b2.game_id2
	LEFT JOIN recent_batting b3 on b.batter=b3.batter3 AND b.game_id=b3.game_id3;
	

#Combine total, recent, and against pitcher batting stats into one table

Create Table IF NOT EXISTS TotalBatting
    SELECT batter,
    game_id,
    team_id,
    homeTeam,
    sum(Hits) as Hits,
    sum(AtBats) as AtBats,
    sum(Walks) as Walks,
    sum(HitByPitch) as HitByPitch,
    sum(SacflyAgainst) as SacFlys,
    sum(SingleAgainst) as Singles,
    sum(DoubleAgainst) as Doubles,
    sum(TripleAgainst) as Triples,
    sum(HomerunAgainst) as Homeruns,
    sum(StrikeOutAgainst) as Strikeouts, 
    sum(HitsAgainstPitcher) as HitsAgainstPitcher,
    sum(AtBatsAgainstPitcher) as AtBatsAgainstPitcher,
    sum(WalksAgainstPitcher) as WalksAgainstPitcher,
    sum(HitByPitchPitcher) as HitByPitchAgainstPitcher,
    sum(SacflyAgainstPitcher) as SacFlysAgainstPitcher,
    sum(SingleAgainstPitcher) as SinglesAgainstPitcher,
    sum(DoubleAgainstPitcher) as DoublesAgainstPitcher,
    sum(TripleAgainstPitcher) as TriplesAgainstPitcher,
    sum(HomerunAgainstPitcher) as HomerunsAgainstPitcher,
    sum(StrikeOutAgainstPitcher) as StrikeoutsAgainstPitcher,  
    sum(RecentHits) as RecentHits,
    sum(RecentAtBats) as RecentAtBats,
    sum(RecentWalks) as RecentWalks,
    sum(RecentHitByPitch) as RecentHitByPitch,
    sum(RecentSacfly) as RecentSacFlys,
    sum(RecentSingle) as RecentSingles,
    sum(RecentDouble) as RecentDoubles,
    sum(RecentTriple) as RecentTriples,
    sum(RecentHomerun) as RecentHomeruns,
    sum(RecentStrikeOut) as RecentStrikeouts,
    sum(Hits)/sum(AtBats) as Batting,
    sum(Walks)/sum(PlateAppearance) as WalkAverage,
    sum(Hits+Walks+HitByPitch)/sum(AtBats+Walks+HitByPitch+SacFlyAgainst) as OnBasePct,
    sum((4*HomerunAgainst)+(3*TripleAgainst)+(2*DoubleAgainst)+(SingleAgainst))/sum(AtBats) as Slugging,
    sum(StrikeOutAgainst)/sum(PlateAppearance) as StrikeoutAverage,
    sum(DoublePlayAgainst+TriplePlayAgainst)/sum(AtBats) as MultiplePlay,
    sum(HitsAgainstPitcher)/sum(AtBatsAgainstPitcher) AS BattingAgainstPitcher,
    sum(WalksAgainstPitcher)/sum(PlateAppearanceAgainstPitcher) AS WalkAverageAgainstPitcher,
    sum(HitsAgainstPitcher+WalksAgainstPitcher+HitByPitchPitcher)/sum(AtBatsAgainstPitcher+WalksAgainstPitcher+HitByPitchPitcher+SacFlyAgainstPitcher) AS OnBasePctAgainstPitcher,
    sum((4*HomerunAgainstPitcher)+(3*TripleAgainstPitcher)+(2*DoubleAgainstPitcher)+(SingleAgainstPitcher))/sum(AtBatsAgainstPitcher) AS SluggingAgainstPitcher,
    sum(StrikeOutAgainstPitcher)/sum(PlateAppearanceAgainstPitcher) AS StrikeoutAveragePitcher,
    sum(DoublePlayAgainstPitcher+TriplePlayAgainstPitcher)/sum(AtBatsAgainstPitcher) AS MultiplePlayAgainstPitcher,
    sum(RecentHits)/sum(RecentAtBats) AS RecentBatting,
    sum(RecentWalks)/sum(RecentPlateAppearance) AS RecentWalkAverage,
    sum(RecentHits+RecentWalks+RecentHitByPitch)/sum(RecentAtBats+RecentWalks+RecentHitByPitch+RecentSacFly) AS RecentOnBasePct,
    sum((4*RecentHomerun)+(3*RecentTriple)+(2*RecentDouble)+(RecentSingle))/sum(RecentAtBats) AS RecentSlugging,
    sum(RecentStrikeOut)/sum(RecentPlateAppearance) AS RecentStrikeoutAverage,
    sum(RecentDoublePlay+RecentTriplePlay)/sum(RecentAtBats) AS RecentMultiplePlayAgainst
	FROM BattingJoin
	Where AtbatsAgainstPitcher>0
	AND RecentAtBats>0
	GROUP BY batter, game_id, team_id, homeTeam;

#average the batting stats for each team for each game

CREATE TABLE IF NOT EXISTS TeamBatting
    SELECT game_id,
    team_id,
    homeTeam,
    avg(Hits) as AvgHits,
    avg(AtBats) as AvgAtBats,
    avg(Walks) as AvgWalks,
    avg(HitByPitch) as AvgHitByPitch,
    avg(Sacflys) as AvgSacFlys,
    avg(Singles) as AvgSingles,
    avg(Doubles) as AvgDoubles,
    avg(Triples) as AvgTriples,
    avg(Homeruns) as AvgHomeruns,
    avg(StrikeOuts) as AvgStrikeouts,
    avg(HitsAgainstPitcher) as AvgHitsAgainstPitcher,
    avg(AtBatsAgainstPitcher) as AvgAtBatsAgainstPitcher,
    avg(WalksAgainstPitcher) as AvgWalksAgainstPitcher,
    avg(HitByPitchAgainstPitcher) as AvgHitByPitchAgainstPitcher,
    avg(SacflysAgainstPitcher) as AvgSacFlysAgainstPitcher,
    avg(SinglesAgainstPitcher) as AvgSinglesAgainstPitcher,
    avg(DoublesAgainstPitcher) as AvgDoublesAgainstPitcher,
    avg(TriplesAgainstPitcher) as AvgTriplesAgainstPitcher,
    avg(HomerunsAgainstPitcher) as AvgHomerunsAgainstPitcher,
    avg(StrikeOutsAgainstPitcher) as AvgStrikeoutsAgainstPitcher,
    avg(RecentHits) as AvgRecentHits,
    avg(RecentAtBats) as AvgRecentAtBats,
    avg(RecentWalks) as AvgRecentWalks,
    avg(RecentHitByPitch) as AvgRecentHitByPitch,
    avg(RecentSacflys) as AvgRecentSacFlys,
    avg(RecentSingles) as AvgRecentSingles,
    avg(RecentDoubles) as AvgRecentDoubles,
    avg(RecentTriples) as AvgRecentTriples,
    avg(RecentHomeruns) as AvgRecentHomeruns,
    avg(RecentStrikeOuts) as AvgRecentStrikeouts,
    avg(Batting) as TeamBattingAvg,
    avg(WalkAverage) as TeamWalkAvg,
    avg(OnBasePct) as TeamOBP,
    avg(Slugging) as TeamSlugging,
    avg(StrikeoutAverage) as TeamStrikeoutAverage,
    avg(MultiplePlay) as TeamMultiplePlay,
    avg(BattingAgainstPitcher) as TeamBattingAgainstPitcher,
    avg(WalkAverageAgainstPitcher) as TeamWalkAvgAgainstPitcher,
    avg(OnBasePctAgainstPitcher) as TeamOBPAgainstPitcher,
    avg(SluggingAgainstPitcher) as TeamSluggingAgainstPitcher,
    avg(StrikeoutAveragePitcher) as TeamStrikeoutAvgAgainstPitcher,
    avg(MultiplePlayAgainstPitcher) as TeamMultiplePlayAgainstPitcher,
    avg(RecentBatting) as RecentTeamBatting,
    avg(RecentWalkAverage) as RecentTeamWalkAvg,
    avg(RecentOnBasePct) as RecentTeamOBP,
    avg(RecentSlugging) as RecentTeamSlugging,
    avg(RecentStrikeoutAverage) as RecentTeamStrickoutAvg,
    avg(RecentMultiplePlayAgainst) as RecentTeamMultiplePlayAgainst
    FROM TotalBatting
    GROUP BY game_id, team_id, homeTeam;

CREATE TABLE IF NOT EXISTS BoxscoreTeams
    SELECT b.game_id,
    g.home_team_id as team_id,
    g.away_team_id as op_team_id,
    bc.homeTeam,
    CASE WHEN b.winner_home_or_away="H" THEN 1
         WHEN b.winner_home_or_away="A" THEN 0
         end AS Win,
    b.home_runs as TeamRuns,
    b.home_hits as TeamHits,
    b.home_errors as TeamErrors,
    g.local_date,
	DATE_ADD(g.local_date, INTERVAL -100 DAY) as firstdate,
	DATE_ADD(g.local_date, INTERVAL -30 DAY) as seconddate,
	DATE_ADD(g.local_date, INTERVAL -1 DAY) as yesterday
	FROM boxscore b
	JOIN game g on b.game_id=g.game_id
	JOIN batter_counts_pitcher_date bc on b.game_id=bc.game_id
	AND g.home_team_id=bc.team_id

    UNION

    SELECT b.game_id,
    g.away_team_id as team_id,
    g.home_team_id as op_team_id,
    bc.homeTeam,
    CASE WHEN b.winner_home_or_away="A" THEN 1
         WHEN b.winner_home_or_away="H" THEN 0
         end AS Win,
    b.away_runs as TeamRuns,
    b.away_hits as TeamHits,
    b.away_errors as TeamErrors,
    g.local_date,
	DATE_ADD(g.local_date, INTERVAL -100 DAY) as firstdate,
	DATE_ADD(g.local_date, INTERVAL -30 DAY) as seconddate,
	DATE_ADD(g.local_date, INTERVAL -1 DAY) as yesterday
	FROM boxscore b
	JOIN game g on b.game_id=g.game_id
	JOIN batter_counts_pitcher_date bc on b.game_id=bc.game_id
	AND g.away_team_id=bc.team_id;

#average recent team performance

CREATE TABLE IF NOT EXISTS BoxscoreTeams100day
    SELECT b.game_id,
    b.team_id,
    avg(b2.Win) as WinPct,
    avg(b2.TeamRuns) as AvgRuns,
    avg(b2.TeamHits) as AvgHits,
    avg(b2.TeamErrors) as AvgErrors
    FROM BoxscoreTeams b
    JOIN BoxscoreTeams b2
    WHERE (b2.local_date BETWEEN b.firstdate AND b.yesterday)
    AND b.team_id=b2.team_id
    GROUP by game_id, team_id;
   
CREATE TABLE IF NOT EXISTS BoxscoreTeams30day
    SELECT b.game_id,
    b.team_id,
    avg(b2.Win) as RecentWinPct,
    avg(b2.TeamRuns) as RecentAvgRuns,
    avg(b2.TeamHits) as RecentAvgHits,
    avg(b2.TeamErrors) as RecentAvgErrors
    FROM BoxscoreTeams b
    JOIN BoxscoreTeams b2
    WHERE (b2.local_date BETWEEN b.seconddate AND b.yesterday)
    AND b.team_id=b2.team_id
    GROUP by game_id, team_id;
   

CREATE TABLE IF NOT EXISTS BoxscoreTeamsHomeAway
    SELECT b.game_id,
    b.team_id,
    avg(b2.Win) as ContextWinPct,
    avg(b2.TeamRuns) as ContextAvgRuns,
    avg(b2.TeamHits) as ContextAvgHits,
    avg(b2.TeamErrors) as ContextAvgErrors
    FROM BoxscoreTeams b
    JOIN BoxscoreTeams b2
    WHERE (b2.local_date BETWEEN b.firstdate AND b.yesterday)
    AND b.team_id=b2.team_id
    AND b.homeTeam=b2.homeTeam
    GROUP by game_id, team_id;
   
 CREATE TABLE IF NOT EXISTS BoxscoreTeamsAgainstTeam
    SELECT b.game_id,
    b.team_id,
    avg(b2.Win) as WinPctAgainstTeam,
    avg(b2.TeamRuns) as AvgRunsAgainstTeam,
    avg(b2.TeamHits) as AvgHitsAgainstTeam,
    avg(b2.TeamErrors) as AvgErrorsAgainstTeam
    FROM BoxscoreTeams b
    JOIN BoxscoreTeams b2
    WHERE b.team_id=b2.team_id
    AND b.op_team_id=b2.op_team_id
    GROUP by game_id, team_id;
   
CREATE TABLE IF NOT EXISTS TotalBoxscore
    SELECT b.game_id,
    b.team_id,
    b.WinPct,
    b.AvgRuns,
    b.AvgHits,
    b.AvgErrors,
    b1.RecentWinPct,
    b1.RecentAvgRuns,
    b1.RecentAvgHits,
    b1.RecentAvgErrors,
    b2.ContextWinPct,
    b2.ContextAvgRuns,
    b2.ContextAvgHits,
    b2.ContextAvgErrors,
    b3.WinPctAgainstTeam,
    b3.AvgRunsAgainstTeam,
    b3.AvgHitsAgainstTeam,
    b3.AvgErrorsAgainstTeam
    FROM BoxscoreTeams100day b
    JOIN BoxscoreTeams30day b1 on b.game_id=b1.game_id AND b.team_id=b1.team_id
    JOIN BoxscoreTeamsHomeAway b2 on b.game_id=b2.game_id AND b.team_id=b2.team_id
    JOIN BoxscoreTeamsAgainstTeam b3 on b.game_id=b3.game_id AND b.team_id=b3.team_id;

CREATE TABLE IF NOT EXISTS pitcher_counts_date
	SELECT pc.*,
	b.Win,
	b.local_date,
	DATE_ADD(b.local_date, INTERVAL -100 DAY) as firstdate,
	DATE_ADD(b.local_date, INTERVAL -1 DAY) as yesterday
	FROM pitcher_counts pc
	LEFT JOIN BoxscoreTeams b
	    ON pc.game_id=b.game_id
	    AND pc.team_id=b.team_id;

Create Table IF NOT EXISTS GamePitching
	Select pc1.game_id,
	pc1.team_id,
	pc1.local_date,
	pc1.firstdate,
	pc1.yesterday,
	avg(pc.Win) as PitcherWinPct,
	avg(pc.outsPlayed) as PitcherAverageOutsPlayed,
	sum(pc.Hit) as PitcherHits,
	sum(pc.Walk+pc.Intent_Walk) as PitcherWalks,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`) as PitcherStrikeouts,
	sum(pc.Hit_By_Pitch) as PitcherHitByPitch,
	sum(pc.Single) as PitcherSingles,
	sum(pc.Double) as PitcherDoubles,
	sum(pc.Triple) as PitcherTriples,
	sum(pc.Home_Run) as PitcherHomeRuns,
	sum(pc.Hit)/sum(pc.outsPlayed) as PitcherHitsPerOutsPlayed,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`)/sum(pc.outsPlayed) as PitcherStrikeoutsPerOutsPlayed,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.outsPlayed) as PitcherWalksPerOutsPlayed,
	sum(pc.Hit)/sum(pc.atBat) AS PitcherBattingAverageAgainstTotal,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.plateApperance) as PitcherWalkAverage,
	sum(pc.Hit+pc.Hit_By_Pitch+pc.Intent_walk+pc.Walk)/sum(pc.atBat+pc.Intent_Walk+pc.Walk+pc.Hit_By_Pitch+pc.Sac_Fly) as PitcherOBP,
	sum((4*pc.Home_run)+(3*pc.Triple)+(2*pc.`Double`)+(pc.Single))/sum(pc.atBat) as PitcherSlugging
	FROM pitcher_counts_date pc1
	JOIN pitcher_counts_date pc on pc.pitcher=pc1.pitcher
	WHERE pc1.local_date>pc.local_date
	AND pc1.startingPitcher=1
	GROUP by game_id, team_id, local_date, firstdate, yesterday;

Create Table IF NOT EXISTS GameBullPenPitching
	Select pc1.game_id,
	pc1.team_id,
	sum(pc.Hit) as BullPenPitcherHits,
	sum(pc.Walk+pc.Intent_Walk) as BullPenPitcherWalks,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`) as BullPenPitcherStrikeouts,
	sum(pc.Hit_By_Pitch) as BullPenPitcherHitByPitch,
	sum(pc.Single) as BullPenPitcherSingles,
	sum(pc.Double) as BullPenPitcherDoubles,
	sum(pc.Triple) as BullPenPitcherTriples,
	sum(pc.Home_Run) as BullPenPitcherHomeRuns,
	sum(pc.Hit)/sum(pc.outsPlayed) as BullPenPitcherHitsPerOutsPlayed,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`)/sum(pc.outsPlayed) as BullPenPitcherStrikeoutsPerOutsPlayed,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.outsPlayed) as BullPenPitcherWalksPerOutsPlayed,
	sum(pc.Hit)/sum(pc.atBat) AS BullPenPitcherBattingAverageAgainstTotal,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.plateApperance) as BullPenPitcherWalkAverage,
	sum(pc.Hit+pc.Hit_By_Pitch+pc.Intent_walk+pc.Walk)/sum(pc.atBat+pc.Intent_Walk+pc.Walk+pc.Hit_By_Pitch+pc.Sac_Fly) as BullPenPitcherOBP,
	sum((4*pc.Home_run)+(3*pc.Triple)+(2*pc.`Double`)+(pc.Single))/sum(pc.atBat) as BullPenPitcherSlugging
	FROM GamePitching pc1
	JOIN pitcher_counts_date pc on pc.team_id=pc1.team_id
	WHERE pc1.local_date>pc.local_date
	AND pc.startingPitcher=0
	GROUP by game_id, team_id;

Create Table IF NOT EXISTS RecentGamePitching
	Select pc1.game_id,
	pc1.team_id,
	pc1.local_date,
	pc1.firstdate,
	pc1.yesterday,
	avg(pc.Win) as RecentPitcherWinPct,
	avg(pc.outsPlayed) as RecentPitcherAverageOutsPlayed,
	sum(pc.Hit) as RecentPitcherHits,
	sum(pc.Walk+pc.Intent_Walk) as RecentPitcherWalks,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`) as RecentPitcherStrikeouts,
	sum(pc.Hit_By_Pitch) as RecentPitcherHitByPitch,
	sum(pc.Single) as RecentPitcherSingles,
	sum(pc.Double) as RecentPitcherDoubles,
	sum(pc.Triple) as RecentPitcherTriples,
	sum(pc.Home_Run) as RecentPitcherHomeRuns,
	sum(pc.Hit)/sum(pc.outsPlayed) as RecentPitcherHitsPerOutsPlayed,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`)/sum(pc.outsPlayed) as RecentPitcherStrikeoutsPerOutsPlayed,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.outsPlayed) as RecentPitcherWalksPerOutsPlayed,
	sum(pc.Hit)/sum(pc.atBat) AS RecentPitcherBattingAverageAgainstTotal,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.plateApperance) as RecentPitcherWalkAverage,
	sum(pc.Hit+pc.Hit_By_Pitch+pc.Intent_walk+pc.Walk)/sum(pc.atBat+pc.Intent_Walk+pc.Walk+pc.Hit_By_Pitch+pc.Sac_Fly) as RecentPitcherOBP,
	sum((4*pc.Home_run)+(3*pc.Triple)+(2*pc.`Double`)+(pc.Single))/sum(pc.atBat) as RecentPitcherSlugging
	FROM pitcher_counts_date pc1
	JOIN pitcher_counts_date pc on pc.pitcher=pc1.pitcher
	WHERE (pc.local_date BETWEEN pc1.firstdate AND pc1.yesterday)
	AND pc1.startingPitcher=1
	AND pc.atBat>0
	GROUP by game_id, team_id, local_date, firstdate, yesterday;


Create Table IF NOT EXISTS RecentGameBullPenPitching
	Select pc1.game_id,
	pc1.team_id,
	sum(pc.Hit) as RecentBullPenPitcherHits,
	sum(pc.Walk+pc.Intent_Walk) as RecentBullPenPitcherWalks,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`) as RecentBullPenPitcherStrikeouts,
	sum(pc.Hit_By_Pitch) as RecentBullPenPitcherHitByPitch,
	sum(pc.Single) as RecentBullPenPitcherSingles,
	sum(pc.Double) as RecentBullPenPitcherDoubles,
	sum(pc.Triple) as RecentBullPenPitcherTriples,
	sum(pc.Home_Run) as RecentBullPenPitcherHomeRuns,
	sum(pc.Hit)/sum(pc.outsPlayed) as RecentBullPenPitcherHitsPerOutsPlayed,
	sum(pc.Strikeout+pc.`Strikeout_-_DP`+pc.`Strikeout_-_TP`)/sum(pc.outsPlayed) as RecentBullPenPitcherStrikeoutsPerOutsPlayed,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.outsPlayed) as RecentBullPenPitcherWalksPerOutsPlayed,
	sum(pc.Hit)/sum(pc.atBat) AS RecentBullPenPitcherBattingAverageAgainstTotal,
	sum(pc.Walk+pc.Intent_Walk)/sum(pc.plateApperance) as RecentBullPenPitcherWalkAverage,
	sum(pc.Hit+pc.Hit_By_Pitch+pc.Intent_walk+pc.Walk)/sum(pc.atBat+pc.Intent_Walk+pc.Walk+pc.Hit_By_Pitch+pc.Sac_Fly) as RecentBullPenPitcherOBP,
	sum((4*pc.Home_run)+(3*pc.Triple)+(2*pc.`Double`)+(pc.Single))/sum(pc.atBat) as RecentBullPenPitcherSlugging
	FROM RecentGamePitching pc1
	JOIN pitcher_counts_date pc on pc.team_id=pc1.team_id
	WHERE (pc.local_date BETWEEN pc1.firstdate AND pc1.yesterday)
	AND pc.startingPitcher=0
	GROUP by game_id, team_id;

CREATE TABLE IF NOT EXISTS TotalPitching
    SELECT pc.game_id,
	pc.team_id,
	pc.PitcherWinPct,
	pc.PitcherAverageOutsPlayed,
	pc.PitcherHits,
	pc.PitcherWalks,
	pc.PitcherStrikeouts,
	pc.PitcherHitByPitch,
	pc.PitcherSingles,
	pc.PitcherDoubles,
	pc.PitcherTriples,
	pc.PitcherHomeRuns,
	pc.PitcherHitsPerOutsPlayed,
	pc.PitcherStrikeoutsPerOutsPlayed,
	pc.PitcherWalksPerOutsPlayed,
	pc.PitcherBattingAverageAgainstTotal,
	pc.PitcherWalkAverage,
	pc.PitcherOBP,
	pc.PitcherSlugging,
	pc1.BullPenPitcherHits,
	pc1.BullPenPitcherWalks,
	pc1.BullPenPitcherStrikeouts,
	pc1.BullPenPitcherHitByPitch,
	pc1.BullPenPitcherSingles,
	pc1.BullPenPitcherDoubles,
	pc1.BullPenPitcherTriples,
	pc1.BullPenPitcherHomeRuns,
	pc1.BullPenPitcherHitsPerOutsPlayed,
	pc1.BullPenPitcherStrikeoutsPerOutsPlayed,
	pc1.BullPenPitcherWalksPerOutsPlayed,
	pc1.BullPenPitcherBattingAverageAgainstTotal,
	pc1.BullPenPitcherWalkAverage,
	pc1.BullPenPitcherOBP,
	pc1.BullPenPitcherSlugging,
	CASE 
		WHEN pc2.RecentPitcherWinPct is not null THEN pc2.RecentPitcherWinPct 
		ELSE pc.PitcherWinPct 
		END as RecentPitcherWinPct,
	CASE 
		WHEN pc2.RecentPitcherAverageOutsPlayed is not null THEN pc2.RecentPitcherAverageOutsPlayed 
		ELSE pc.PitcherAverageOutsPlayed 
		END as RecentPitcherAverageOutsPlayed,
	CASE 
		WHEN pc2.RecentPitcherHits is not null THEN pc2.RecentPitcherHits
		ELSE pc.PitcherHits 
		END as RecentPitcherHits,
	CASE 
		WHEN pc2.RecentPitcherWalks is not null THEN pc2.RecentPitcherWalks
		ELSE pc.PitcherWalks
		END as RecentPitcherWalks,	
	CASE 
		WHEN pc2.RecentPitcherStrikeouts is not null THEN pc2.RecentPitcherStrikeouts
		ELSE pc.PitcherStrikeouts
		END as RecentPitcherStrikeouts,		
	CASE 
		WHEN pc2.RecentPitcherHitByPitch is not null THEN pc2.RecentPitcherHitByPitch
		ELSE pc.PitcherHitByPitch
		END as RecentPitcherHitByPitch,		
	CASE 
		WHEN pc2.RecentPitcherSingles is not null THEN pc2.RecentPitcherSingles
		ELSE pc.PitcherSingles
		END as RecentPitcherSingles,		
	CASE 
		WHEN pc2.RecentPitcherDoubles is not null THEN pc2.RecentPitcherDoubles
		ELSE pc.PitcherDoubles
		END as RecentPitcherDoubles,	
	CASE 
		WHEN pc2.RecentPitcherTriples is not null THEN pc2.RecentPitcherTriples
		ELSE pc.PitcherTriples
		END as RecentPitcherTriples,	
	CASE 
		WHEN pc2.RecentPitcherHomeRuns is not null THEN pc2.RecentPitcherHomeRuns
		ELSE pc.PitcherHomeRuns
		END as RecentPitcherHomeRuns,		
	CASE 
		WHEN pc2.RecentPitcherHitsPerOutsPlayed is not null THEN pc2.RecentPitcherHitsPerOutsPlayed 
		ELSE pc.PitcherHitsPerOutsPlayed 
		END as RecentPitcherHitsPerOutsPlayed,	
	CASE 
		WHEN pc2.RecentPitcherStrikeoutsPerOutsPlayed is not null THEN pc2.RecentPitcherStrikeoutsPerOutsPlayed 
		ELSE pc.PitcherStrikeoutsPerOutsPlayed 
		END as RecentPitcherStrikeoutsPerOutsPlayed,	
	CASE 
		WHEN pc2.RecentPitcherWalksPerOutsPlayed is not null THEN pc2.RecentPitcherWalksPerOutsPlayed 
		ELSE pc.PitcherWalksPerOutsPlayed 
		END as RecentPitcherWalksPerOutsPlayed,
	CASE 
		WHEN pc2.RecentPitcherBattingAverageAgainstTotal is not null THEN pc2.RecentPitcherBattingAverageAgainstTotal 
		ELSE pc.PitcherBattingAverageAgainstTotal 
		END as RecentPitcherBattingAverageAgainstTotal,
	CASE 
		WHEN pc2.RecentPitcherWalkAverage is not null THEN pc2.RecentPitcherWalkAverage 
		ELSE pc.PitcherWalkAverage 
		END as RecentPitcherWalkAverage,
	CASE 
		WHEN pc2.RecentPitcherOBP is not null THEN pc2.RecentPitcherOBP 
		ELSE pc.PitcherOBP 
		END as RecentPitcherOBP,
	CASE 
		WHEN pc2.RecentPitcherSlugging is not null THEN pc2.RecentPitcherSlugging 
		ELSE pc.PitcherSlugging 
		END as RecentPitcherSlugging,
	CASE 
		WHEN pc3.RecentBullPenPitcherHits is not null THEN pc3.RecentBullPenPitcherHits
		ELSE pc1.BullPenPitcherHits 
		END as RecentBullPenPitcherHits,
	CASE 
		WHEN pc3.RecentBullPenPitcherWalks is not null THEN pc3.RecentBullPenPitcherWalks
		ELSE pc1.BullPenPitcherWalks
		END as RecentBullPenPitcherWalks,	
	CASE 
		WHEN pc3.RecentBullPenPitcherStrikeouts is not null THEN pc3.RecentBullPenPitcherStrikeouts
		ELSE pc1.BullPenPitcherStrikeouts
		END as RecentBullPenPitcherStrikeouts,		
	CASE 
		WHEN pc3.RecentBullPenPitcherHitByPitch is not null THEN pc3.RecentBullPenPitcherHitByPitch
		ELSE pc1.BullPenPitcherHitByPitch
		END as RecentBullPenPitcherHitByPitch,		
	CASE 
		WHEN pc3.RecentBullPenPitcherSingles is not null THEN pc3.RecentBullPenPitcherSingles
		ELSE pc1.BullPenPitcherSingles
		END as RecentBullPenPitcherSingles,		
	CASE 
		WHEN pc3.RecentBullPenPitcherDoubles is not null THEN pc3.RecentBullPenPitcherDoubles
		ELSE pc1.BullPenPitcherDoubles
		END as RecentBullPenPitcherDoubles,	
	CASE 
		WHEN pc3.RecentBullPenPitcherTriples is not null THEN pc3.RecentBullPenPitcherTriples
		ELSE pc1.BullPenPitcherTriples
		END as RecentBullPenPitcherTriples,	
	CASE 
		WHEN pc3.RecentBullPenPitcherHomeRuns is not null THEN pc3.RecentBullPenPitcherHomeRuns
		ELSE pc1.BullPenPitcherHomeRuns
		END as RecentBullPenPitcherHomeRuns,		
	CASE 
		WHEN pc3.RecentBullPenPitcherHitsPerOutsPlayed is not null THEN pc3.RecentBullPenPitcherHitsPerOutsPlayed 
		ELSE pc1.BullPenPitcherHitsPerOutsPlayed 
		END as RecentBullPenPitcherHitsPerOutsPlayed,	
	CASE 
		WHEN pc3.RecentBullPenPitcherStrikeoutsPerOutsPlayed is not null THEN pc3.RecentBullPenPitcherStrikeoutsPerOutsPlayed 
		ELSE pc1.BullPenPitcherStrikeoutsPerOutsPlayed 
		END as RecentBullPenPitcherStrikeoutsPerOutsPlayed,	
	CASE 
		WHEN pc3.RecentBullPenPitcherWalksPerOutsPlayed is not null THEN pc3.RecentBullPenPitcherWalksPerOutsPlayed 
		ELSE pc1.BullPenPitcherWalksPerOutsPlayed 
		END as RecentBullPenPitcherWalksPerOutsPlayed,
	CASE 
		WHEN pc3.RecentBullPenPitcherBattingAverageAgainstTotal is not null THEN pc3.RecentBullPenPitcherBattingAverageAgainstTotal 
		ELSE pc1.BullPenPitcherBattingAverageAgainstTotal 
		END as RecentBullPenPitcherBattingAverageAgainstTotal,
	CASE 
		WHEN pc3.RecentBullPenPitcherWalkAverage is not null THEN pc3.RecentBullPenPitcherWalkAverage 
		ELSE pc1.BullPenPitcherWalkAverage 
		END as RecentBullPenPitcherWalkAverage,
	CASE 
		WHEN pc3.RecentBullPenPitcherOBP is not null THEN pc3.RecentBullPenPitcherOBP 
		ELSE pc1.BullPenPitcherOBP 
		END as RecentBullPenPitcherOBP,
	CASE 
		WHEN pc3.RecentBullPenPitcherSlugging is not null THEN pc3.RecentBullPenPitcherSlugging 
		ELSE pc1.BullPenPitcherSlugging 
		END as RecentBullPenPitcherSlugging
	FROM GamePitching pc
	LEFT JOIN GameBullPenPitching pc1 on pc.game_id=pc1.game_id AND pc.team_id=pc1.team_id
	LEFT JOIN RecentGamePitching pc2 on pc.game_id=pc2.game_id AND pc.team_id=pc2.game_id
	LEFT JOIN RecentGameBullPenPitching pc3 on pc.game_id=pc3.game_id AND pc.team_id=pc3.team_id;

CREATE TABLE IF NOT EXISTS TOTALSTATSHOME
    SELECT
    b.game_id,
    b.AvgHits as HomeAvgHits,
    b.AvgAtBats as HomeAvgAtBats,
    b.AvgWalks as HomeAvgWalks,
    b.AvgHitByPitch as HomeAvgHitByPitch,
    b.AvgSacFlys as HomeAvgSacFlys,
    b.AvgSingles as HomeAvgSingles,
    b.AvgDoubles as HomeAvgDoubles,
    b.AvgTriples as HomeAvgTriples,
    b.AvgHomeruns as HomeAvgHomeruns,
    b.AvgStrikeouts as HomeAvgStrikeouts,
    b.AvgHitsAgainstPitcher as HomeAvgHitsAgainstPitcher,
    b.AvgAtBatsAgainstPitcher as HomeAvgAtBatsAgainstPitcher,
    b.AvgWalksAgainstPitcher as HomeAvgWalksAgainstPitcher,
    b.AvgHitByPitchAgainstPitcher as HomeAvgHitByPitchAgainstPitcher,
    b.AvgSacFlysAgainstPitcher as HomeAvgSacFlysAgainstPitcher,
    b.AvgSinglesAgainstPitcher as HomeAvgSinglesAgainstPitcher,
    b.AvgDoublesAgainstPitcher as HomeAvgDoublesAgainstPitcher,
    b.AvgTriplesAgainstPitcher as HomeAvgTriplesAgainstPitcher,
    b.AvgHomerunsAgainstPitcher as HomeAvgHomerunsAgainstPitcher,
    b.AvgStrikeoutsAgainstPitcher as HomeAvgStrikeoutsAgainstPitcher,
    b.AvgRecentHits as HomeAvgRecentHits,
    b.AvgRecentAtBats as HomeAvgRecentAtBats,
    b.AvgRecentWalks as HomeAvgRecentWalks,
    b.AvgRecentHitByPitch as HomeAvgRecentHitByPitch,
    b.AvgRecentSacFlys as HomeAvgRecentSacFlys,
    b.AvgRecentSingles as HomeAvgRecentSingles,
    b.AvgRecentDoubles as HomeAvgRecentDoubles,
    b.AvgRecentTriples as HomeAvgRecentTriples,
    b.AvgRecentHomeruns as HomeAvgRecentHomeRuns,
    b.AvgRecentStrikeouts as HomeAvgRecentStrikeouts,
    b.TeamBattingAvg as HomeTeamBattingAvg,
    b.TeamWalkAvg as HomeTeamWalkAvg,
    b.TeamOBP as HomeTeamOBP,
    b.TeamSlugging as HomeTeamSlugging,
    b.TeamStrikeoutAverage as HomeTeamStrikeoutAvg,
    b.TeamMultiplePlay as HomeTeamMultiplePlayAvg,
    b.TeamBattingAgainstPitcher as HomeTeamBattingAgainstPitcher,
    b.TeamWalkAvgAgainstPitcher as HomeTeamWalkAvgAgainstPitcher,
    b.TeamOBPAgainstPitcher as HomeTeamOBPAgainstPitcher,
    b.TeamSluggingAgainstPitcher as HomeTeamSluggingAgainstPitcher,
    b.TeamStrikeoutAvgAgainstPitcher as HomeTeamStrikeoutAvgAgainstPitcher,
    b.TeamMultiplePlayAgainstPitcher as HomeTeamMultiplePlayAvgAgainstPitcher,
    b.RecentTeamBatting as RecentHomeTeamBatting,
    b.RecentTeamWalkAvg as RecentHomeTeamWalkAvg,
    b.RecentTeamOBP as RecentHomeTeamOBP,
    b.RecentTeamSlugging as RecentHomeTeamSlugging,
    b.RecentTeamStrickoutAvg as RecentHomeTeamStrikeoutAvg,
    b.RecentTeamMultiplePlayAgainst as RecentHomeTeamMultiplePlayAvg,
    p.PitcherWinPct as HomePitcherWinPct,
	p.PitcherAverageOutsPlayed as HomePitcherAverageOutsPlayed,
	p.PitcherHits as HomePitcherHits,
	p.PitcherWalks as HomePitcherWalks,
	p.PitcherStrikeouts as HomePitcherStrikeouts,
	p.PitcherHitByPitch as HomePitcherHitByPitch,
	p.PitcherSingles as HomePitcherSingles,
	p.PitcherDoubles as HomePitcherDoubles,
	p.PitcherTriples as HomePitcherTriples,
	p.PitcherHomeRuns as HomePitcherHomeRuns,
	p.PitcherHitsPerOutsPlayed as HomePitcherHitsPerOutsPlayed,
	p.PitcherStrikeoutsPerOutsPlayed as HomePitcherStrikeoutsPerOutsPlayed,
	p.PitcherWalksPerOutsPlayed as HomePitcherWalksPerOutsPlayed,
	p.PitcherBattingAverageAgainstTotal as HomePitcherBattingAverageAgainst,
	p.PitcherWalkAverage as HomePitcherWalkAverageAgainst,
	p.PitcherOBP as HomePitcherOBPAgainst,
	p.PitcherSlugging as HomePitcherSluggingAgainst,
	p.BullPenPitcherHits as HomeBullPenPitcherHits,
	p.BullPenPitcherWalks as HomeBullPenPitcherWalks,
	p.BullPenPitcherStrikeouts as HomeBullPenPitcherStrikeouts,
	p.BullPenPitcherHitByPitch as HomeBullPenPitcherHitByPitch,
	p.BullPenPitcherSingles as HomeBullPenPitcherSingles,
	p.BullPenPitcherDoubles as HomeBullPenPitcherDoubles,
	p.BullPenPitcherTriples as HomeBullPenPitcherTriples,
	p.BullPenPitcherHomeRuns as HomeBullPenPitcherHomeRuns,
	p.BullPenPitcherHitsPerOutsPlayed as HomeBullPenPitcherHitsPerOutsPlayed,
	p.BullPenPitcherStrikeoutsPerOutsPlayed as HomeBullPenPitcherStrikeoutsPerOutsPlayed,
	p.BullPenPitcherWalksPerOutsPlayed as HomeBullPenPitcherWalksPerOutsPlayed,
	p.BullPenPitcherBattingAverageAgainstTotal as HomeBullPenPitcherBattingAverageAgainstTotal,
	p.BullPenPitcherWalkAverage as HomeBullPenPitcherWalkAverage,
	p.BullPenPitcherOBP as HomeBullPenPitcherOBP,
	p.BullPenPitcherSlugging as HomeBullPenPitcherSlugging,
	p.RecentPitcherWinPct as RecentHomePitcherWinPct,
	p.RecentPitcherAverageOutsPlayed as RecentHomePitcherAverageOutsPlayed,
	p.RecentPitcherHits as RecentHomePitcherHits,
	p.RecentPitcherWalks as RecentHomePitcherWalks,	
	p.RecentPitcherStrikeouts as RecentHomePitcherStrikeouts,		
	p.RecentPitcherHitByPitch as RecentHomePitcherHitByPitch,		
	p.RecentPitcherSingles as RecentHomePitcherSingles,		
	p.RecentPitcherDoubles as RecentHomePitcherDoubles,	
	p.RecentPitcherTriples as RecentHomePitcherTriples,	
	p.RecentPitcherHomeRuns as RecentHomePitcherHomeRuns,
	p.RecentPitcherHitsPerOutsPlayed as RecentHomePitcherHitsPerOutsPlayed,
	p.RecentPitcherStrikeoutsPerOutsPlayed as RecentHomePitcherStrikeoutsPerOutsPlayed,
	p.RecentPitcherWalksPerOutsPlayed as RecentHomePitcherWalksPerOutsPlayed,
	p.RecentPitcherBattingAverageAgainstTotal as RecentHomePitcherBattingAverageAgainst,
	p.RecentPitcherWalkAverage as RecentHomePitcherWalkAverageAgainst,
	p.RecentPitcherOBP as RecentHomePitcherOBPAgainst,
	p.RecentPitcherSlugging as RecentHomePitcherSluggingAgainst,
	p.RecentBullPenPitcherHits as RecentHomeBullPenPitcherHits,
	p.RecentBullPenPitcherWalks as RecentHomeBullPenPitcherWalks,
	p.RecentBullPenPitcherStrikeouts as RecentHomeBullPenPitcherStrikeouts,
	p.RecentBullPenPitcherHitByPitch as RecentHomeBullPenPitcherHitByPitch,
	p.RecentBullPenPitcherSingles as RecentHomeBullPenPitcherSingles,
	p.RecentBullPenPitcherDoubles as RecentHomeBullPenPitcherDoubles,
	p.RecentBullPenPitcherTriples as RecentHomeBullPenPitcherTriples,
	p.RecentBullPenPitcherHomeRuns as RecentHomeBullPenPitcherHomeRuns,
	p.RecentBullPenPitcherHitsPerOutsPlayed as RecentHomeBullPenPitcherHitsPerOutsPlayed,
	p.RecentBullPenPitcherStrikeoutsPerOutsPlayed as RecentHomeBullPenPitcherStrikeoutsPerOutsPlayed,
	p.RecentBullPenPitcherWalksPerOutsPlayed as RecentHomeBullPenPitcherWalksPerOutsPlayed,
	p.RecentBullPenPitcherBattingAverageAgainstTotal as RecentHomeBullPenPitcherBattingAverageAgainstTotal,
	p.RecentBullPenPitcherWalkAverage as RecentHomeBullPenPitcherWalkAverage,
	p.RecentBullPenPitcherOBP as RecentHomeBullPenPitcherOBP,
	p.RecentBullPenPitcherSlugging as RecentHomeBullPenPitcherSlugging,
	s.WinPct as HomeTeamWinPct,
    s.AvgRuns as HomeTeamAvgRuns,
    s.AvgHits as HomeTeamAvgHits,
    s.AvgErrors as HomeTeamAvgErrors,
    s.RecentWinPct as HomeTeamRecentWinPct,
    s.RecentAvgRuns as HomeTeamRecentAvgRuns,
    s.RecentAvgHits as HomeTeamRecentAvgHits,
    s.RecentAvgErrors as HomeTeamRecentAvgErrors,
    s.ContextWinPct as HomeTeamHomeWinPct,
    s.ContextAvgRuns as HomeTeamHomeAvgRuns,
    s.ContextAvgHits as HomeTeamHomeAvgHits,
    s.ContextAvgErrors as HomeTeamHomeAvgErrors,
    s.WinPctAgainstTeam as HomeTeamWinPctAgainstTeam,
    s.AvgRunsAgainstTeam as HomeTeamAvgRunsAgainstTeam,
    s.AvgHitsAgainstTeam as HomeTeamAvgHitsAgainstTeam,
    s.AvgErrorsAgainstTeam as HomeTeamAvgErrorsAgainstTeam
    FROM TeamBatting b
    INNER JOIN TotalPitching p on b.game_id=p.game_id AND b.team_id=p.team_id
    INNER JOIN TotalBoxscore s on b.game_id=s.game_id AND b.team_id=s.team_id
    Where b.homeTeam=1;

CREATE TABLE IF NOT EXISTS TOTALSTATSAway
    SELECT
    b.game_id,
    b.AvgHits as AwayAvgHits,
    b.AvgAtBats as AwayAvgAtBats,
    b.AvgWalks as AwayAvgWalks,
    b.AvgHitByPitch as AwayAvgHitByPitch,
    b.AvgSacFlys as AwayAvgSacFlys,
    b.AvgSingles as AwayAvgSingles,
    b.AvgDoubles as AwayAvgDoubles,
    b.AvgTriples as AwayAvgTriples,
    b.AvgHomeruns as AwayAvgHomeruns,
    b.AvgStrikeouts as AwayAvgStrikeouts,
    b.AvgHitsAgainstPitcher as AwayAvgHitsAgainstPitcher,
    b.AvgAtBatsAgainstPitcher as AwayAvgAtBatsAgainstPitcher,
    b.AvgWalksAgainstPitcher as AwayAvgWalksAgainstPitcher,
    b.AvgHitByPitchAgainstPitcher as AwayAvgHitByPitchAgainstPitcher,
    b.AvgSacFlysAgainstPitcher as AwayAvgSacFlysAgainstPitcher,
    b.AvgSinglesAgainstPitcher as AwayAvgSinglesAgainstPitcher,
    b.AvgDoublesAgainstPitcher as AwayAvgDoublesAgainstPitcher,
    b.AvgTriplesAgainstPitcher as AwayAvgTriplesAgainstPitcher,
    b.AvgHomerunsAgainstPitcher as AwayAvgHomerunsAgainstPitcher,
    b.AvgStrikeoutsAgainstPitcher as AwayAvgStrikeoutsAgainstPitcher,
    b.AvgRecentHits as AwayAvgRecentHits,
    b.AvgRecentAtBats as AwayAvgRecentAtBats,
    b.AvgRecentWalks as AwayAvgRecentWalks,
    b.AvgRecentHitByPitch as AwayAvgRecentHitByPitch,
    b.AvgRecentSacFlys as AwayAvgRecentSacFlys,
    b.AvgRecentSingles as AwayAvgRecentSingles,
    b.AvgRecentDoubles as AwayAvgRecentDoubles,
    b.AvgRecentTriples as AwayAvgRecentTriples,
    b.AvgRecentHomeruns as AwayAvgRecentHomeRuns,
    b.AvgRecentStrikeouts as AwayAvgRecentStrikeouts,
    b.TeamBattingAvg as AwayTeamBattingAvg,
    b.TeamWalkAvg as AwayTeamWalkAvg,
    b.TeamOBP as AwayTeamOBP,
    b.TeamSlugging as AwayTeamSlugging,
    b.TeamStrikeoutAverage as AwayTeamStrikeoutAvg,
    b.TeamMultiplePlay as AwayTeamMultiplePlayAvg,
    b.TeamBattingAgainstPitcher as AwayTeamBattingAgainstPitcher,
    b.TeamWalkAvgAgainstPitcher as AwayTeamWalkAvgAgainstPitcher,
    b.TeamOBPAgainstPitcher as AwayTeamOBPAgainstPitcher,
    b.TeamSluggingAgainstPitcher as AwayTeamSluggingAgainstPitcher,
    b.TeamStrikeoutAvgAgainstPitcher as AwayTeamStrikeoutAvgAgainstPitcher,
    b.TeamMultiplePlayAgainstPitcher as AwayTeamMultiplePlayAvgAgainstPitcher,
    b.RecentTeamBatting as RecentAwayTeamBatting,
    b.RecentTeamWalkAvg as RecentAwayTeamWalkAvg,
    b.RecentTeamOBP as RecentAwayTeamOBP,
    b.RecentTeamSlugging as RecentAwayTeamSlugging,
    b.RecentTeamStrickoutAvg as RecentAwayTeamStrikeoutAvg,
    b.RecentTeamMultiplePlayAgainst as RecentAwayTeamMultiplePlayAvg,
    p.PitcherWinPct as AwayPitcherWinPct,
	p.PitcherAverageOutsPlayed as AwayPitcherAverageOutsPlayed,
	p.PitcherHits as AwayPitcherHits,
	p.PitcherWalks as AwayPitcherWalks,
	p.PitcherStrikeouts as AwayPitcherStrikeouts,
	p.PitcherHitByPitch as AwayPitcherHitByPitch,
	p.PitcherSingles as AwayPitcherSingles,
	p.PitcherDoubles as AwayPitcherDoubles,
	p.PitcherTriples as AwayPitcherTriples,
	p.PitcherHomeRuns as AwayPitcherHomeRuns,
	p.PitcherHitsPerOutsPlayed as AwayPitcherHitsPerOutsPlayed,
	p.PitcherStrikeoutsPerOutsPlayed as AwayPitcherStrikeoutsPerOutsPlayed,
	p.PitcherWalksPerOutsPlayed as AwayPitcherWalksPerOutsPlayed,
	p.PitcherBattingAverageAgainstTotal as AwayPitcherBattingAverageAgainst,
	p.PitcherWalkAverage as AwayPitcherWalkAverageAgainst,
	p.PitcherOBP as AwayPitcherOBPAgainst,
	p.PitcherSlugging as AwayPitcherSluggingAgainst,
	p.BullPenPitcherHits as AwayBullPenPitcherHits,
	p.BullPenPitcherWalks as AwayBullPenPitcherWalks,
	p.BullPenPitcherStrikeouts as AwayBullPenPitcherStrikeouts,
	p.BullPenPitcherHitByPitch as AwayBullPenPitcherHitByPitch,
	p.BullPenPitcherSingles as AwayBullPenPitcherSingles,
	p.BullPenPitcherDoubles as AwayBullPenPitcherDoubles,
	p.BullPenPitcherTriples as AwayBullPenPitcherTriples,
	p.BullPenPitcherHomeRuns as AwayBullPenPitcherHomeRuns,
	p.BullPenPitcherHitsPerOutsPlayed as AwayBullPenPitcherHitsPerOutsPlayed,
	p.BullPenPitcherStrikeoutsPerOutsPlayed as AwayBullPenPitcherStrikeoutsPerOutsPlayed,
	p.BullPenPitcherWalksPerOutsPlayed as AwayBullPenPitcherWalksPerOutsPlayed,
	p.BullPenPitcherBattingAverageAgainstTotal as AwayBullPenPitcherBattingAverageAgainstTotal,
	p.BullPenPitcherWalkAverage as AwayBullPenPitcherWalkAverage,
	p.BullPenPitcherOBP as AwayBullPenPitcherOBP,
	p.BullPenPitcherSlugging as AwayBullPenPitcherSlugging,
	p.RecentPitcherWinPct as RecentAwayPitcherWinPct,
	p.RecentPitcherAverageOutsPlayed as RecentAwayPitcherAverageOutsPlayed,
	p.RecentPitcherHits as RecentAwayPitcherHits,
	p.RecentPitcherWalks as RecentAwayPitcherWalks,	
	p.RecentPitcherStrikeouts as RecentAwayPitcherStrikeouts,		
	p.RecentPitcherHitByPitch as RecentAwayPitcherHitByPitch,		
	p.RecentPitcherSingles as RecentAwayPitcherSingles,		
	p.RecentPitcherDoubles as RecentAwayPitcherDoubles,	
	p.RecentPitcherTriples as RecentAwayPitcherTriples,	
	p.RecentPitcherHomeRuns as RecentAwayPitcherHomeRuns,
	p.RecentPitcherHitsPerOutsPlayed as RecentAwayPitcherHitsPerOutsPlayed,
	p.RecentPitcherStrikeoutsPerOutsPlayed as RecentAwayPitcherStrikeoutsPerOutsPlayed,
	p.RecentPitcherWalksPerOutsPlayed as RecentAwayPitcherWalksPerOutsPlayed,
	p.RecentPitcherBattingAverageAgainstTotal as RecentAwayPitcherBattingAverageAgainst,
	p.RecentPitcherWalkAverage as RecentAwayPitcherWalkAverageAgainst,
	p.RecentPitcherOBP as RecentAwayPitcherOBPAgainst,
	p.RecentPitcherSlugging as RecentAwayPitcherSluggingAgainst,
	p.RecentBullPenPitcherHits as RecentAwayBullPenPitcherHits,
	p.RecentBullPenPitcherWalks as RecentAwayBullPenPitcherWalks,
	p.RecentBullPenPitcherStrikeouts as RecentAwayBullPenPitcherStrikeouts,
	p.RecentBullPenPitcherHitByPitch as RecentAwayBullPenPitcherHitByPitch,
	p.RecentBullPenPitcherSingles as RecentAwayBullPenPitcherSingles,
	p.RecentBullPenPitcherDoubles as RecentAwayBullPenPitcherDoubles,
	p.RecentBullPenPitcherTriples as RecentAwayBullPenPitcherTriples,
	p.RecentBullPenPitcherHomeRuns as RecentAwayBullPenPitcherHomeRuns,
	p.RecentBullPenPitcherHitsPerOutsPlayed as RecentAwayBullPenPitcherHitsPerOutsPlayed,
	p.RecentBullPenPitcherStrikeoutsPerOutsPlayed as RecentAwayBullPenPitcherStrikeoutsPerOutsPlayed,
	p.RecentBullPenPitcherWalksPerOutsPlayed as RecentAwayBullPenPitcherWalksPerOutsPlayed,
	p.RecentBullPenPitcherBattingAverageAgainstTotal as RecentAwayBullPenPitcherBattingAverageAgainstTotal,
	p.RecentBullPenPitcherWalkAverage as RecentAwayBullPenPitcherWalkAverage,
	p.RecentBullPenPitcherOBP as RecentAwayBullPenPitcherOBP,
	p.RecentBullPenPitcherSlugging as RecentAwayBullPenPitcherSlugging,
	s.WinPct as AwayTeamWinPct,
    s.AvgRuns as AwayTeamAvgRuns,
    s.AvgHits as AwayTeamAvgHits,
    s.AvgErrors as AwayTeamAvgErrors,
    s.RecentWinPct as AwayTeamRecentWinPct,
    s.RecentAvgRuns as AwayTeamRecentAvgRuns,
    s.RecentAvgHits as AwayTeamRecentAvgHits,
    s.RecentAvgErrors as AwayTeamRecentAvgErrors,
    s.ContextWinPct as AwayTeamAwayWinPct,
    s.ContextAvgRuns as AwayTeamAwayAvgRuns,
    s.ContextAvgHits as AwayTeamAwayAvgHits,
    s.ContextAvgErrors as AwayTeamAwayAvgErrors,
    s.WinPctAgainstTeam as AwayTeamWinPctAgainstTeam,
    s.AvgRunsAgainstTeam as AwayTeamAvgRunsAgainstTeam,
    s.AvgHitsAgainstTeam as AwayTeamAvgHitsAgainstTeam,
    s.AvgErrorsAgainstTeam as AwayTeamAvgErrorsAgainstTeam
    FROM TeamBatting b
    INNER JOIN TotalPitching p on b.game_id=p.game_id AND b.team_id=p.team_id
    INNER JOIN TotalBoxscore s on b.game_id=s.game_id AND b.team_id=s.team_id
    Where b.homeTeam=0;
   
DROP TABLE if Exists FINALSTATS;

CREATE TABLE IF NOT EXISTS FINALSTATS
    SELECT
    CASE WHEN b.winner_home_or_away="H" THEN 1
         WHEN b.winner_home_or_away="A" THEN 0
         end AS HomeTeamWin,    
    h.HomeAvgHits,
    a.AwayAvgHits,
    h.HomeAvgHits - a.AwayAvgHits as HitsDiff,
    h.HomeAvgAtBats,
    a.AwayAvgAtBats,
    h.HomeAvgAtBats - a.AwayAvgAtBats as AtBatsDiff,
    h.HomeAvgWalks,
    a.AwayAvgWalks,
    h.HomeAvgWalks - a.AwayAvgWalks as WalksDiff,
    h.HomeAvgHitByPitch,
    a.AwayAvgHitByPitch,
    h.HomeAvgHitByPitch - a.AwayAvgHitByPitch as HitByPitchDiff,  
    h.HomeAvgSacFlys,
    a.AwayAvgSacFlys,
    h.HomeAvgSacFlys - a.AwayAvgSacFlys as SacFlyDiff,   
    h.HomeAvgSingles,
    a.AwayAvgSingles,
    h.HomeAvgSingles - a.AwayAvgSingles as SinglesDiff,    
    h.HomeAvgDoubles,
    a.AwayAvgDoubles,
    h.HomeAvgDoubles - a.AwayAvgDoubles as DoublesDiff,   
    h.HomeAvgTriples,
    a.AwayAvgTriples,
    h.HomeAvgTriples - a.AwayAvgTriples as TriplesDiff,
    h.HomeAvgHomeruns,
    a.AwayAvgHomeruns,
    h.HomeAvgHomeruns - a.AwayAvgHomeruns as HomeRunsDiff,
    h.HomeAvgStrikeouts,
    a.AwayAvgStrikeouts,
    h.HomeAvgStrikeouts - a.AwayAvgStrikeouts as StrikeoutsDiff,    
    h.HomeAvgHitsAgainstPitcher,
    a.AwayAvgHitsAgainstPitcher,
    h.HomeAvgHitsAgainstPitcher - a.AwayAvgHitsAgainstPitcher as HitsAgainstPitcherDiff,
    h.HomeAvgAtBatsAgainstPitcher,
    a.AwayAvgAtBatsAgainstPitcher,
    h.HomeAvgAtBatsAgainstPitcher - a.AwayAvgAtBatsAgainstPitcher as AtBatsAgainstPitcherDiff,
    h.HomeAvgWalksAgainstPitcher,
    a.AwayAvgWalksAgainstPitcher,
    h.HomeAvgWalksAgainstPitcher - a.AwayAvgWalksAgainstPitcher as WalksAgainstPitcherDiff,
    h.HomeAvgHitByPitchAgainstPitcher,
    a.AwayAvgHitByPitchAgainstPitcher,
    h.HomeAvgHitByPitchAgainstPitcher - a.AwayAvgHitByPitchAgainstPitcher as HitByPitchAgainstPitcherDiff,  
    h.HomeAvgSacFlysAgainstPitcher,
    a.AwayAvgSacFlysAgainstPitcher,
    h.HomeAvgSacFlysAgainstPitcher - a.AwayAvgSacFlysAgainstPitcher as SacFlyAgainstPitcherDiff,   
    h.HomeAvgSinglesAgainstPitcher,
    a.AwayAvgSinglesAgainstPitcher,
    h.HomeAvgSinglesAgainstPitcher - a.AwayAvgSinglesAgainstPitcher as SinglesAgainstPitcherDiff,    
    h.HomeAvgDoublesAgainstPitcher,
    a.AwayAvgDoublesAgainstPitcher,
    h.HomeAvgDoublesAgainstPitcher - a.AwayAvgDoublesAgainstPitcher as DoublesAgainstPitcherDiff,   
    h.HomeAvgTriplesAgainstPitcher,
    a.AwayAvgTriplesAgainstPitcher,
    h.HomeAvgTriplesAgainstPitcher - a.AwayAvgTriplesAgainstPitcher as TriplesAgainstPitcherDiff,
    h.HomeAvgHomerunsAgainstPitcher,
    a.AwayAvgHomerunsAgainstPitcher,
    h.HomeAvgHomerunsAgainstPitcher - a.AwayAvgHomerunsAgainstPitcher as HomeRunsAgainstPitcherDiff,
    h.HomeAvgStrikeoutsAgainstPitcher,
    a.AwayAvgStrikeoutsAgainstPitcher,
    h.HomeAvgStrikeoutsAgainstPitcher - a.AwayAvgStrikeoutsAgainstPitcher as StrikeoutsAgainstPitcherDiff,  
    h.HomeAvgRecentHits,
    a.AwayAvgRecentHits,
    h.HomeAvgRecentHits - a.AwayAvgRecentHits as RecentHitsDiff,
    h.HomeAvgRecentAtBats,
    a.AwayAvgRecentAtBats,
    h.HomeAvgRecentAtBats - a.AwayAvgRecentAtBats as RecentAtBatsDiff,
    h.HomeAvgRecentWalks,
    a.AwayAvgRecentWalks,
    h.HomeAvgRecentWalks - a.AwayAvgRecentWalks as RecentWalksDiff,
    h.HomeAvgRecentHitByPitch,
    a.AwayAvgRecentHitByPitch,
    h.HomeAvgRecentHitByPitch - a.AwayAvgRecentHitByPitch as RecentHitByPitchDiff,  
    h.HomeAvgRecentSacFlys,
    a.AwayAvgRecentSacFlys,
    h.HomeAvgRecentSacFlys - a.AwayAvgRecentSacFlys as RecentSacFlyDiff,   
    h.HomeAvgRecentSingles,
    a.AwayAvgRecentSingles,
    h.HomeAvgRecentSingles - a.AwayAvgRecentSingles as RecentSinglesDiff,    
    h.HomeAvgRecentDoubles,
    a.AwayAvgRecentDoubles,
    h.HomeAvgRecentDoubles - a.AwayAvgRecentDoubles as RecentDoublesDiff,   
    h.HomeAvgRecentTriples,
    a.AwayAvgRecentTriples,
    h.HomeAvgRecentTriples - a.AwayAvgRecentTriples as RecentTriplesDiff,
    h.HomeAvgRecentHomeruns,
    a.AwayAvgRecentHomeruns,
    h.HomeAvgRecentHomeruns - a.AwayAvgRecentHomeruns as RecentHomeRunsDiff,
    h.HomeAvgRecentStrikeouts,
    a.AwayAvgRecentStrikeouts,
    h.HomeAvgRecentStrikeouts - a.AwayAvgRecentStrikeouts as RecentStrikeoutsDiff,           
	h.HomeTeamBattingAvg,
    a.AwayTeamBattingAvg,
    h.HomeTeamBattingAvg-a.AwayTeamBattingAvg as TeamBattingAvgDiff,
    h.HomeTeamWalkAvg,
    a.AwayTeamWalkAvg,
    h.HomeTeamWalkAvg-a.AwayTeamWalkAvg as TeamWalkAvgDiff,
    h.HomeTeamOBP,
    a.AwayTeamOBP,
    h.HomeTeamOBP - a.AwayTeamOBP as TeamOBPDiff,
    h.HomeTeamSlugging,
    a.AwayTeamSlugging,
    h.HomeTeamSlugging - a.AwayTeamSlugging as TeamSluggingDiff,
    h.HomeTeamStrikeoutAvg,
    a.AwayTeamStrikeoutAvg,
    h.HomeTeamStrikeoutAvg - a.AwayTeamStrikeoutAvg as TeamStrikeoutDiff,
    h.HomeTeamBattingAgainstPitcher,
    a.AwayTeamBattingAgainstPitcher,
    h.HomeTeamBattingAgainstPitcher - a.AwayTeamBattingAgainstPitcher as TeamBattingAgainstPitcherDiff,
    h.HomeTeamWalkAvgAgainstPitcher,
    a.AwayTeamWalkAvgAgainstPitcher,
    h.HomeTeamWalkAvgAgainstPitcher - a.AwayTeamWalkAvgAgainstPitcher as TeamWalkAvgAgainstPitcherDiff,
    h.HomeTeamOBPAgainstPitcher,
    a.AwayTeamOBPAgainstPitcher,
    h.HomeTeamOBPAgainstPitcher - a.AwayTeamOBPAgainstPitcher as TeamOBPAgainstPitcherDiff,
    h.HomeTeamSluggingAgainstPitcher,
    a.AwayTeamSluggingAgainstPitcher,
    h.HomeTeamSluggingAgainstPitcher - a.AwayTeamSluggingAgainstPitcher as TeamSluggingAgainstPitcherDiff,
    h.HometeamStrikeoutAvgAgainstPitcher,
    a.AwayTeamStrikeoutAvgAgainstPitcher,
    h.HometeamStrikeoutAvgAgainstPitcher - a.AwayTeamStrikeoutAvgAgainstPitcher as TeamStrikeoutAvgAgainstPitcherDiff,
    h.RecentHomeTeamBatting,
    a.RecentAwayTeamBatting,
    h.RecentHomeTeamBatting - a.RecentAwayTeamBatting as RecentTeamBattingDiff,
    h.RecentHomeTeamWalkAvg,
    a.RecentAwayTeamWalkAvg,
    h.RecentHomeTeamWalkAvg - a.RecentAwayTeamWalkAvg as RecentWalkAvgDiff,
    h.RecentHomeTeamOBP,
    a.RecentAwayTeamOBP,
    h.RecentHomeTeamOBP - a.RecentAwayTeamOBP as RecentOBPDiff,
    h.RecentHomeTeamSlugging,
    a.RecentAwayTeamSlugging,
    h.RecentHomeTeamSlugging - a.RecentAwayTeamSlugging as RecentSluggingDiff,
    h.RecentHomeTeamStrikeoutAvg,
    a.RecentAwayTeamStrikeoutAvg,
    h.RecentHomeTeamStrikeoutAvg - a.RecentAwayTeamStrikeoutAvg as RecentStrikeoutAvgDiff,
    h.HomePitcherWinPct,
    a.AwayPitcherWinPct,
    h.HomePitcherWinPct - a.AwayPitcherWinPct as PitcherWinPctDiff,
    h.HomePitcherAverageOutsPlayed,
	a.AwayPitcherAverageOutsPlayed,
	h.HomePitcherAverageOutsPlayed - a.AwayPitcherAverageOutsPlayed as PitcherOutsPlayedDiff,
	h.HomePitcherHits,
	a.AwayPitcherHits,
	h.HomePitcherHits - a.AwayPitcherHits as PitcherHitsDiff,
	h.HomePitcherWalks,
	a.AwayPitcherWalks,
	h.HomePitcherWalks - a.AwayPitcherWalks as PitcherWalksDiff,
	h.HomePitcherStrikeouts,
	a.AwayPitcherStrikeouts,
	h.HomePitcherStrikeouts - a.AwayPitcherStrikeouts as PitcherStrikeoutsDiff,
	h.HomePitcherHitByPitch,
	a.AwayPitcherHitByPitch,
	h.HomePitcherHitByPitch - a.AwayPitcherHitByPitch as PitcherHitByPitchDiff,
	h.HomePitcherSingles,
	a.AwayPitcherSingles,
	h.HomePitcherSingles - a.AwayPitcherSingles as PitcherSinglesDiff,
	h.HomePitcherDoubles,
	a.AwayPitcherDoubles,
	h.HomePitcherDoubles - a.AwayPitcherDoubles as PitcherDoublesDiff,
	h.HomePitcherTriples,
	a.AwayPitcherTriples,
	h.HomePitcherTriples - a.AwayPitcherTriples as PitcherTriplesDiff,
	h.HomePitcherHomeRuns,
	a.AwayPitcherHomeRuns,
	h.HomePitcherHomeRuns - a.AwayPitcherHomeRuns as PitcherHomeRunsDiff,
	h.HomePitcherHitsPerOutsPlayed,
	a.AwayPitcherHitsPerOutsPlayed,
	h.HomePitcherHitsPerOutsPlayed - a.AwayPitcherHitsPerOutsPlayed as PitcherHitsPerOutDiff,
	h.HomePitcherWalksPerOutsPlayed,
	a.AwayPitcherWalksPerOutsPlayed,
	h.HomePitcherWalksPerOutsPlayed - a.AwayPitcherWalksPerOutsPlayed as PitcherWalksPerOutDiff,
	h.HomePitcherStrikeoutsPerOutsPlayed,
	a.AwayPitcherStrikeoutsPerOutsPlayed,
	h.HomePitcherStrikeoutsPerOutsPlayed - a.AwayPitcherStrikeoutsPerOutsPlayed as PitcherStrikeoutsPerOutDiff,
	h.HomePitcherBattingAverageAgainst,
	a.AwayPitcherBattingAverageAgainst,
	h.HomePitcherBattingAverageAgainst - a.AwayPitcherBattingAverageAgainst as PitcherBattingAgainstDiff,
	h.HomePitcherWalkAverageAgainst,
	a.AwayPitcherWalkAverageAgainst,
	h.HomePitcherWalkAverageAgainst - a.AwayPitcherWalkAverageAgainst as PitcherWalkAvgAgainstDiff,
	h.HomePitcherOBPAgainst,
	a.AwayPitcherOBPAgainst,
	h.HomePitcherOBPAgainst - a.AwayPitcherOBPAgainst as PitcherOBPAgainstDiff,
	h.HomePitcherSluggingAgainst,
	a.AwayPitcherSluggingAgainst,
	h.HomePitcherSluggingAgainst - a.AwayPitcherSluggingAgainst as PitcherSluggingAgainstDiff,
	h.HomeBullPenPitcherHits,
	a.AwayBullPenPitcherHits,
	h.HomeBullPenPitcherHits - a.AwayBullPenPitcherHits as BullPenPitcherHitsDiff,
	h.HomeBullPenPitcherWalks,
	a.AwayBullPenPitcherWalks,
	h.HomeBullPenPitcherWalks - a.AwayBullPenPitcherWalks as BullPenPitcherWalksDiff,
	h.HomeBullPenPitcherStrikeouts,
	a.AwayBullPenPitcherStrikeouts,	
	h.HomeBullPenPitcherStrikeouts - a.AwayBullPenPitcherStrikeouts as BullPenPitcherStrikeoutsDiff,	
	h.HomeBullPenPitcherHitByPitch,
	a.AwayBullPenPitcherHitByPitch,	
	h.HomeBullPenPitcherHitByPitch - a.AwayBullPenPitcherHitByPitch as BullPenPitcherHitByPitchDiff,	
	h.HomeBullPenPitcherSingles,
	a.AwayBullPenPitcherSingles,
	h.HomeBullPenPitcherSingles - a.AwayBullPenPitcherSingles as BullPenPitcherSinglesDiff,
	h.HomeBullPenPitcherDoubles,
	a.AwayBullPenPitcherDoubles,	
	h.HomeBullPenPitcherDoubles - a.AwayBullPenPitcherDoubles as BullPenPitcherDoublesDiff,
	h.HomeBullPenPitcherTriples,
	a.AwayBullPenPitcherTriples,	
	h.HomeBullPenPitcherTriples - a.AwayBullPenPitcherTriples as BullPenPitcherTriplesDiff,	
	h.HomeBullPenPitcherHomeRuns,
	a.AwayBullPenPitcherHomeRuns,	
	h.HomeBullPenPitcherHomeRuns - a.AwayBullPenPitcherHomeRuns as BullPenPitcherHomeRunsDiff,	
	h.HomeBullPenPitcherHitsPerOutsPlayed,
	a.AwayBullPenPitcherHitsPerOutsPlayed,	
	h.HomeBullPenPitcherHitsPerOutsPlayed - a.AwayBullPenPitcherHitsPerOutsPlayed as BullPenPitcherHitsPerOutsPlayedDiff,	
	h.HomeBullPenPitcherStrikeoutsPerOutsPlayed,
	a.AwayBullPenPitcherStrikeoutsPerOutsPlayed,	
	h.HomeBullPenPitcherStrikeoutsPerOutsPlayed - a.AwayBullPenPitcherStrikeoutsPerOutsPlayed as BullPenPitcherStrikeoutsPerOutsPlayedDiff,	
	h.HomeBullPenPitcherWalksPerOutsPlayed,
	a.AwayBullPenPitcherWalksPerOutsPlayed,	
	h.HomeBullPenPitcherWalksPerOutsPlayed - a.AwayBullPenPitcherWalksPerOutsPlayed as BullPenPitcherWalksPerOutsPlayedDiff,	
	h.HomeBullPenPitcherBattingAverageAgainstTotal,
	a.AwayBullPenPitcherBattingAverageAgainstTotal,	
	h.HomeBullPenPitcherBattingAverageAgainstTotal - a.AwayBullPenPitcherBattingAverageAgainstTotal as BullPenPitcherBattingAverageAgainstDiff,	
	h.HomeBullPenPitcherWalkAverage,
	a.AwayBullPenPitcherWalkAverage,	
	h.HomeBullPenPitcherWalkAverage - a.AwayBullPenPitcherWalkAverage as BullPenPitcherWalkAverageDiff,
	h.HomeBullPenPitcherOBP,
	a.AwayBullPenPitcherOBP,	
	h.HomeBullPenPitcherOBP - a.AwayBullPenPitcherOBP as BullPenPitcherOBPDiff,	
	h.HomeBullPenPitcherSlugging,
	a.AwayBullPenPitcherSlugging,	
	h.HomeBullPenPitcherSlugging - a.AwayBullPenPitcherSlugging as BullPenPitcherSluggingDiff,	
	h.RecentHomePitcherWinPct,
	a.RecentAwayPitcherWinPct,
	h.RecentHomePitcherWinPct - a.RecentAwayPitcherWinPct as RecentPitcherWinPctDiff,
	h.RecentHomePitcherAverageOutsPlayed,
	a.RecentAwayPitcherAverageOutsPlayed,
	h.RecentHomePitcherAverageOutsPlayed - a.RecentAwayPitcherAverageOutsPlayed as RecentPitcherAverageOutsPlayedDiff,
	h.RecentHomePitcherHits,
	a.RecentAwayPitcherHits,
	h.RecentHomePitcherHits - a.RecentAwayPitcherHits as RecentPitcherHitsDiff,
	h.RecentHomePitcherWalks,	
	a.RecentAwayPitcherWalks,	
	h.RecentHomePitcherWalks - a.RecentAwayPitcherWalks as RecentPitcherWalksDiff,
	h.RecentHomePitcherStrikeouts,		
	a.RecentAwayPitcherStrikeouts,
	h.RecentHomePitcherStrikeouts - a.RecentAwayPitcherStrikeouts as RecentPitcherStrikeoutsDiff,
	h.RecentHomePitcherHitByPitch,	
	a.RecentAwayPitcherHitByPitch,	
	h.RecentHomePitcherHitByPitch - a.RecentAwayPitcherHitByPitch as RecentPitcherHitByPitchDiff,	
	h.RecentHomePitcherSingles,		
	a.RecentAwayPitcherSingles,
	h.RecentHomePitcherSingles - a.RecentAwayPitcherSingles as RecentPitcherSinglesDiff,
	h.RecentHomePitcherDoubles,	
	a.RecentAwayPitcherDoubles,	
	h.RecentHomePitcherDoubles - a.RecentAwayPitcherDoubles as RecentPitcherDoublesDiff,	
	h.RecentHomePitcherTriples,	
	a.RecentAwayPitcherTriples,	
	h.RecentHomePitcherTriples - a.RecentAwayPitcherTriples as RecentPitcherTriplesDiff,	
	h.RecentHomePitcherHomeRuns,
	a.RecentAwayPitcherHomeRuns,
	h.RecentHomePitcherHomeRuns - a.RecentAwayPitcherHomeRuns as RecentPitcherHomeRunsDiff,
	h.RecentHomePitcherHitsPerOutsPlayed,
	a.RecentAwayPitcherHitsPerOutsPlayed,
	h.RecentHomePitcherHitsPerOutsPlayed - a.RecentAwayPitcherHitsPerOutsPlayed as RecentPitcherHitsPerOutDiff,
	h.RecentHomePitcherWalksPerOutsPlayed,
	a.RecentAwayPitcherWalksPerOutsPlayed,
	h.RecentHomePitcherWalksPerOutsPlayed - a.RecentAwayPitcherWalksPerOutsPlayed as RecentPitcherWalksPerOutDiff,
	h.RecentHomePitcherStrikeoutsPerOutsPlayed,
	a.RecentAwayPitcherStrikeoutsPerOutsPlayed,
	h.RecentHomePitcherStrikeoutsPerOutsPlayed - a.RecentAwayPitcherStrikeoutsPerOutsPlayed as RecentPitcherStrikeoutsPerOutDiff,
	h.RecentHomePitcherBattingAverageAgainst,
	a.RecentAwayPitcherBattingAverageAgainst,
	h.RecentHomePitcherBattingAverageAgainst - a.RecentAwayPitcherBattingAverageAgainst as RecentPitcherBattingAgainstDiff,
	h.RecentHomePitcherWalkAverageAgainst,
	a.RecentAwayPitcherWalkAverageAgainst,
	h.RecentHomePitcherWalkAverageAgainst - a.RecentAwayPitcherWalkAverageAgainst as RecentPitcherWalkAvgAgainstDiff,
	h.RecentHomePitcherOBPAgainst,
	a.RecentAwayPitcherOBPAgainst,
	h.RecentHomePitcherOBPAgainst - a.RecentAwayPitcherOBPAgainst as RecentPitcherOBPAgainstDiff,
	h.RecentHomePitcherSluggingAgainst,
	a.RecentAwayPitcherSluggingAgainst,
	h.RecentHomePitcherSluggingAgainst - a.RecentAwayPitcherSluggingAgainst as RecentPitcherSluggingAgainstDiff,
	h.RecentHomeBullPenPitcherHits,
	a.RecentAwayBullPenPitcherHits,
	h.RecentHomeBullPenPitcherHits - a.RecentAwayBullPenPitcherHits as RecentBullPenPitcherHitsDiff,
	h.RecentHomeBullPenPitcherWalks,
	a.RecentAwayBullPenPitcherWalks,
	h.RecentHomeBullPenPitcherWalks - a.RecentAwayBullPenPitcherWalks as RecentBullPenPitcherWalksDiff,
	h.RecentHomeBullPenPitcherStrikeouts,
	a.RecentAwayBullPenPitcherStrikeouts,	
	h.RecentHomeBullPenPitcherStrikeouts - a.RecentAwayBullPenPitcherStrikeouts as RecentBullPenPitcherStrikeoutsDiff,	
	h.RecentHomeBullPenPitcherHitByPitch,
	a.RecentAwayBullPenPitcherHitByPitch,	
	h.RecentHomeBullPenPitcherHitByPitch - a.RecentAwayBullPenPitcherHitByPitch as RecentBullPenPitcherHitByPitchDiff,	
	h.RecentHomeBullPenPitcherSingles,
	a.RecentAwayBullPenPitcherSingles,
	h.RecentHomeBullPenPitcherSingles - a.RecentAwayBullPenPitcherSingles as RecentBullPenPitcherSinglesDiff,
	h.RecentHomeBullPenPitcherDoubles,
	a.RecentAwayBullPenPitcherDoubles,	
	h.RecentHomeBullPenPitcherDoubles - a.RecentAwayBullPenPitcherDoubles as RecentBullPenPitcherDoublesDiff,
	h.RecentHomeBullPenPitcherTriples,
	a.RecentAwayBullPenPitcherTriples,	
	h.RecentHomeBullPenPitcherTriples - a.RecentAwayBullPenPitcherTriples as RecentBullPenPitcherTriplesDiff,	
	h.RecentHomeBullPenPitcherHomeRuns,
	a.RecentAwayBullPenPitcherHomeRuns,	
	h.RecentHomeBullPenPitcherHomeRuns - a.RecentAwayBullPenPitcherHomeRuns as RecentBullPenPitcherHomeRunsDiff,	
	h.RecentHomeBullPenPitcherHitsPerOutsPlayed,
	a.RecentAwayBullPenPitcherHitsPerOutsPlayed,	
	h.RecentHomeBullPenPitcherHitsPerOutsPlayed - a.RecentAwayBullPenPitcherHitsPerOutsPlayed as RecentBullPenPitcherHitsPerOutsPlayedDiff,	
	h.RecentHomeBullPenPitcherStrikeoutsPerOutsPlayed,
	a.RecentAwayBullPenPitcherStrikeoutsPerOutsPlayed,	
	h.RecentHomeBullPenPitcherStrikeoutsPerOutsPlayed - a.RecentAwayBullPenPitcherStrikeoutsPerOutsPlayed as RecentBullPenPitcherStrikeoutsPerOutsPlayedDiff,	
	h.RecentHomeBullPenPitcherWalksPerOutsPlayed,
	a.RecentAwayBullPenPitcherWalksPerOutsPlayed,	
	h.RecentHomeBullPenPitcherWalksPerOutsPlayed - a.RecentAwayBullPenPitcherWalksPerOutsPlayed as RecentBullPenPitcherWalksPerOutsPlayedDiff,	
	h.RecentHomeBullPenPitcherBattingAverageAgainstTotal,
	a.RecentAwayBullPenPitcherBattingAverageAgainstTotal,	
	h.RecentHomeBullPenPitcherBattingAverageAgainstTotal - a.RecentAwayBullPenPitcherBattingAverageAgainstTotal as RecentBullPenPitcherBattingAverageAgainstDiff,	
	h.RecentHomeBullPenPitcherWalkAverage,
	a.RecentAwayBullPenPitcherWalkAverage,	
	h.RecentHomeBullPenPitcherWalkAverage - a.RecentAwayBullPenPitcherWalkAverage as RecentBullPenPitcherWalkAverageDiff,
	h.RecentHomeBullPenPitcherOBP,
	a.RecentAwayBullPenPitcherOBP,	
	h.RecentHomeBullPenPitcherOBP - a.RecentAwayBullPenPitcherOBP as RecentBullPenPitcherOBPDiff,	
	h.RecentHomeBullPenPitcherSlugging,
	a.RecentAwayBullPenPitcherSlugging,	
	h.RecentHomeBullPenPitcherSlugging - a.RecentAwayBullPenPitcherSlugging as RecentBullPenPitcherSluggingDiff,	
	h.HomeTeamWinPct,
	a.AwayTeamWinPct,
	h.HomeTeamWinPct - a.AwayTeamWinPct as TeamWinPctDiff,
	h.HomeTeamAvgRuns,
    a.AwayTeamAvgRuns,
    h.HomeTeamAvgRuns - a.AwayTeamAvgRuns as TeamAvgRunDiff,
    h.HomeTeamAvgHits,
    a.AwayTeamAvgHits,
    h.HomeTeamAvgHits - a.AwayTeamAvgHits as TeamAvgHitDiff,
    h.HomeTeamAvgErrors,
    a.AwayTeamAvgErrors,
    h.HomeTeamAvgErrors - a.AwayTeamAvgErrors as TeamAvgErrorDiff,
    h.HomeTeamRecentWinPct,
	a.AwayTeamRecentWinPct,
	h.HomeTeamRecentWinPct - a.AwayTeamRecentWinPct as RecentTeamWinPctDiff,
	h.HomeTeamRecentAvgRuns,
    a.AwayTeamRecentAvgRuns,
    h.HomeTeamRecentAvgRuns - a.AwayTeamRecentAvgRuns as RecentTeamAvgRunDiff,
    h.HomeTeamRecentAvgHits,
    a.AwayTeamRecentAvgHits,
    h.HomeTeamRecentAvgHits - a.AwayTeamRecentAvgHits as RecentTeamAvgHitDiff,
    h.HomeTeamRecentAvgErrors,
    a.AwayTeamRecentAvgErrors,
    h.HomeTeamRecentAvgErrors - a.AwayTeamRecentAvgErrors as RecentTeamAvgErrorDiff, 
    h.HomeTeamHomeWinPct,
    a.AwayTeamAwayWinPct,
    h.HomeTeamHomeWinPct - a.AwayTeamAwayWinPct as ContextTeamWinPctDiff,
    h.HomeTeamHomeAvgRuns,
    a.AwayTeamAwayAvgRuns,
    h.HomeTeamHomeAvgRuns - a.AwayTeamAwayAvgRuns as ContextTeamAvgRunDiff,
    h.HomeTeamHomeAvgHits,
    a.AwayTeamAwayAvgHits,
    h.HomeTeamHomeAvgHits - a.AwayTeamAwayAvgHits as ContextTeamAvgHitDiff,
    h.HomeTeamHomeAvgErrors,
    a.AwayTeamAwayAvgErrors,
    h.HomeTeamHomeAvgErrors - a.AwayTeamAwayAvgErrors as ContextTeamAvgErrorDiff,
    h.HomeTeamWinPctAgainstTeam,
    h.HomeTeamAvgRunsAgainstTeam,
    a.AwayTeamAvgRunsAgainstTeam,
    h.HomeTeamAvgRunsAgainstTeam - a.AwayTeamAvgRunsAgainstTeam as TeamRunsAgainstTeamDiff,
    h.HomeTeamAvgHitsAgainstTeam,
    a.AwayTeamAvgHitsAgainstTeam,
    h.HomeTeamAvgHitsAgainstTeam - a.AwayTeamAvgHitsAgainstTeam as TeamHitsAgainstTeamDiff,
    h.HomeTeamAvgErrorsAgainstTeam,
	a.AwayTeamAvgErrorsAgainstTeam,
	h.HomeTeamAvgErrorsAgainstTeam - a.AwayTeamAvgErrorsAgainstTeam as TeamErrorsAgainstTeamDiff
    FROM TOTALSTATSHOME h
    INNER JOIN TOTALSTATSAway a on h.game_id=a.game_id
   	INNER JOIN boxscore b on h.game_id=b.game_id;
    