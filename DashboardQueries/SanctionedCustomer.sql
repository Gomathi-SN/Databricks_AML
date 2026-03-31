select c1.sanctionedName, count(c1.client_id)
 from databricksaml_cat.gold.custsanctionalert c1
where sector_risk ='High'
group by c1.sanctionedName