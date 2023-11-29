SELECT cast(v:repo.name as string), count() FROM github_events WHERE cast(v:type as string) = 'WatchEvent' AND cast(v:repo.name as string) LIKE '%_/_%' GROUP BY cast(v:repo.name as string) ORDER BY length(cast(v:repo.name as string)) ASC, cast(v:repo.name as string) LIMIT 50
