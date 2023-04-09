WITH flattened_nba_data AS (
SELECT
    raw_data:"_id"."$oid" AS id,
    raw_data:"date"."$date" AS date,
    team.value:abbreviation AS abbreviation,
    team.value:city AS city,
    team.value:home AS is_home_team,
    team.value:score AS team_score,
    team.value:won AS team_won,
    team.value:name AS name,
    player.value:ast AS ast,
    player.value:blk AS blk,
    player.value:drb AS drb,
    player.value:fg AS fg,
    player.value:fg3 AS fg3,
    player.value:fg3_pct AS fg3_pct,
    player.value:fg3a AS fg3a,
    player.value:fg_pct AS fg_pct,
    player.value:fga AS fga,
    player.value:ft AS ft,
    player.value:ft_pct AS ft_pct,
    player.value:fta AS fta,
    player.value:mp AS mp,
    player.value:orb AS orb,
    player.value:pf AS pf,
    player.value:player AS player,
    player.value:pts AS pts,
    player.value:stl AS stl,
    player.value:tov AS tov,
    player.value:trb AS trb
FROM
    {{ ref('source_nba_games') }},
    LATERAL FLATTEN(raw_data:teams) team,
    LATERAL FLATTEN(team.value:players) player
)
SELECT * from  flattened_nba_data;