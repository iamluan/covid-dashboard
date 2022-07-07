IF EXISTS (SELECT * FROM pg_table WHERE tablename=country_dim) THEN
    drop table country_dim;
IF EXISTS (SELECT * FROM date_dim) THEN
    drop table date_dim;
IF EXISTS (SELECT * FROM vietnamprovince_dim) THEN
    drop table vietnamprovince_dim;