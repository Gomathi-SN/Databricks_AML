select p.client_name,sum(h.TotalAmount) from 
databricksaml_cat.gold.pepcustomeralert p
join databricksaml_cat.bronze.accounts a on p.client_id=a.client_id
join databricksaml_cat.gold.highvoltrans h on a.sender_account=h.Sender_account
where h.is_HighVolTrans=1
group by  p.client_name
order by sum(h.TotalAmount)  desc limit 10