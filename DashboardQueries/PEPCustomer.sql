select sector_risk, case when country='UK' then 'UKR' else country end, count(*) from databricksaml_cat.gold.pepcustomeralert
group by sector_risk, country
having sector_risk in ('High') and count(*) > 1 