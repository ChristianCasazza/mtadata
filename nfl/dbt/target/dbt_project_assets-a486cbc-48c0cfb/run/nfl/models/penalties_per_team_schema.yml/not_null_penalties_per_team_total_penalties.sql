select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select total_penalties
from "database_raw"."main"."penalties_per_team"
where total_penalties is null



      
    ) dbt_internal_test