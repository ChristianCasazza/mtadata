select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select penalty_team
from "database_raw"."main"."penalties_per_team"
where penalty_team is null



      
    ) dbt_internal_test