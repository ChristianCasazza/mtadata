---
title: Bond Information
---

## Choose a Bond

```unique_bonds
SELECT DISTINCT general_ledger
FROM mta.bond_payment_info
```


<Dropdown
    name=unique_bonds
    data={unique_bonds}
    value=general_ledger
    title="Select a Bond" 
    defaultValue="TBTA General Revenue Bonds"
/>


```bond_info
select * 
from mta.bond_payment_info
where general_ledger = '${inputs.unique_bonds.value}' 
```

<BigValue 
  data={bond_info} 
  value=total_payments
  fmt=num0
  title='Total Payments'
/>

<BigValue 
  data={bond_info} 
  value=total_payments
  fmt=num0
  title='Total Payments'
/>

<BigValue 
  data={bond_info} 
  value=average_payment
  fmt=usd
  title='Average Payment'
/>
<BigValue 
  data={bond_info} 
  value=median_payment
  fmt=usd
  title='Median Payment'
/>
<BigValue 
  data={bond_info} 
  value=first_payment_date
  fmt='mmm d/yy'
  title='First Payment Date'
/>
<BigValue 
  data={bond_info} 
  value=last_payment_date
  fmt='mmm d/yy'
  title='Last Payment Date'
/>
<BigValue 
  data={bond_info} 
  value=last_payment_amount
  fmt=usd
  title='Last Payment Amount'
/>