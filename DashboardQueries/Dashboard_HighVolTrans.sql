select TransactionDate,sum(TotalAmount), sum(NumTransactions)
 from databricksaml_cat.gold.HighVolTrans HV
 where is_HighVolTrans= 1
 group by TransactionDate
