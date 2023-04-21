WITH flattened_nba_data AS (
SELECT 
    raw_data:"_id"."$oid"::VARCHAR AS guid,
    raw_data:"date"."$date" AS datex,
    --to_timestamp_ntz(datex, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZHTZM') AS DATE_new,
    team.value:abbreviation::VARCHAR AS abbreviation,
    team.value:city::VARCHAR AS city,
    team.value:home::BOOLEAN AS is_home,
    team.value:name::VARCHAR AS team_name,
    team.value:score::INT AS team_score,
    CASE WHEN (team.value:won) = 1 THEN TRUE ELSE FALSE END AS is_team_won,
    player.value:player::STRING AS player_name,
    player.value:ast::INT AS assist,
    player.value:blk::INT AS block,
    player.value:drb::INT AS dribble,
    player.value:fg::INT AS field_goal,
    ---REPLACE (player.value:fg_pct, '" "') AS field_goal_percent_1,
    /*
    Replace "" with empty string, 
    NULLIF is used to convert any empty strings to null, 
    and then COALESCE is used to return a default value of 0 if the value is null. 
    Finally, ::FLOAT is used to cast the resulting value to a float.
    */
    COALESCE(NULLIF(REPLACE(player.value:fg_pct, '" '), ''), 0)::FLOAT AS field_goal_percent,
    player.value:fga::INT AS field_goal_assist,
    player.value:fg3::INT AS three_point_field_goal,
    COALESCE(NULLIF(REPLACE(player.value:fg3_pct, '" '), ''), 0)::FLOAT AS three_point_field_goal_percent,
    player.value:fg3a::INT AS three_point_field_goal_assist,
    player.value:ft::INT AS free_throws,
    COALESCE(NULLIF(REPLACE(player.value:ft_pct, '" '), ''), 0)::FLOAT AS free_throws_percent,
    player.value:fta::INT AS free_throws_attempt,
    REPLACE(player.value:mp, '"') AS minute_played,
    player.value:orb::INT AS offensive_rebound,
    player.value:pf::INT AS personal_fouls,
    player.value:pts::INT AS points_scored,
    player.value:stl::INT AS steals,
    player.value:tov::INT AS turnovers,
    player.value:trb::INT AS total_rebounds
    
FROM {{ ref('source_nba_games') }},
    LATERAL FLATTEN(raw_data:teams) team,
    LATERAL FLATTEN(team.value:players) player
)
SELECT 
* EXCLUDE (datex),
to_timestamp(flattened_nba_data.datex::string, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZHTZM') AS created_at
from flattened_nba_data