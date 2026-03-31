select c.client_name, sum(TotalAmount)
from databricksaml_cat.gold.highvoltrans h
join databricksaml_cat.bronze.accounts a on h.Sender_account=a.sender_account
join databricksaml_cat.bronze.clients c on a.client_id=c.client_id
where h.is_HighVolTrans=1 and c.sector_risk='High'
group by c.client_name
order by sum(TotalAmount) desc limit 10