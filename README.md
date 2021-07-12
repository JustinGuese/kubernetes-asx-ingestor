# complicated sql query for first price

SELECT *
FROM asx_data
where symbol IN ('YAL') and "timestamp" between '2021-01-24' and '2021-01-25'
order by "timestamp" asc
limit 1;