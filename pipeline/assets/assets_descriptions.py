# Descriptions for mta_subway_origin_destination_2023 table


# Descriptions for mta_daily_ridership table
descriptions_mta_daily_ridership = {
    "date": "The date of travel (MM/DD/YYYY).",
    "subways_total_ridership": "The daily total estimated subway ridership.",
    "subways_pct_pre_pandemic": "The daily ridership estimate as a percentage of subway ridership on an equivalent day prior to the COVID-19 pandemic.",
    "buses_total_ridership": "The daily total estimated bus ridership.",
    "buses_pct_pre_pandemic": "The daily ridership estimate as a percentage of bus ridership on an equivalent day prior to the COVID-19 pandemic.",
    "lirr_total_ridership": "The daily total estimated LIRR ridership. Blank value indicates that the ridership data was not or is not currently available or applicable.",
    "lirr_pct_pre_pandemic": "The daily ridership estimate as a percentage of LIRR ridership on an equivalent day prior to the COVID-19 pandemic.",
    "metro_north_total_ridership": "The daily total estimated Metro-North ridership. Blank value indicates that the ridership data was not or is not currently available or applicable.",
    "metro_north_pct_pre_pandemic": "The daily ridership estimate as a percentage of Metro-North ridership on an equivalent day prior to the COVID-19 pandemic.",
    "access_a_ride_total_trips": "The daily total scheduled Access-A-Ride trips. Blank value indicates that the ridership data was not or is not currently available or applicable.",
    "access_a_ride_pct_pre_pandemic": "The daily total scheduled trips as a percentage of total scheduled trips on an equivalent day prior to the COVID-19 pandemic.",
    "bridges_tunnels_total_traffic": "The daily total Bridges and Tunnels traffic. Blank value indicates that the ridership data was not or is not currently available or applicable.",
    "bridges_tunnels_pct_pre_pandemic": "The daily total traffic as a percentage of total traffic on an equivalent day prior to the COVID-19 pandemic.",
    "staten_island_railway_total_ridership": "The daily total estimated SIR ridership.",
    "staten_island_railway_pct_pre_pandemic": "The daily ridership estimate as a percentage of SIR ridership on an equivalent day prior to the COVID-19 pandemic."
}

# Descriptions for mta_hourly_subway_socrata table
descriptions_mta_hourly_subway_socrata = {
    "transit_timestamp": "Timestamp payment took place in local time. All transactions are rounded down to the nearest hour (e.g., 1:37pm → 1pm).",
    "transit_mode": "Distinguishes between subway, Staten Island Railway, and Roosevelt Island Tram.",
    "station_complex_id": "A unique identifier for station complexes.",
    "station_complex": "Subway complex where an entry swipe or tap took place (e.g., Zerega Av (6)).",
    "borough": "Represents the borough (Bronx, Brooklyn, Manhattan, Queens) serviced by the subway system.",
    "payment_method": "Specifies whether the payment method was OMNY or MetroCard.",
    "fare_class_category": "Class of fare payment used for the trip (e.g., MetroCard – Fair Fare, OMNY – Full Fare).",
    "ridership": "Total number of riders that entered a subway complex via OMNY or MetroCard at a specific hour and for that specific fare type.",
    "transfers": "Number of individuals who entered a subway complex via a free bus-to-subway or out-of-network transfer. Already included in the total ridership column.",
    "latitude": "Latitude of the subway complex.",
    "longitude": "Longitude of the subway complex.",
    "geom_wkt": "Open Data platform-generated geocoding information, supplied in 'POINT ( )' format."
}

# Descriptions for mta_operations_statement table
descriptions_mta_operations_statement = {
    "fiscal_year": "The Fiscal Year of the data (i.e., 2023, 2024).",
    "timestamp": "The timestamp for the data, rounded up to the first day of the month.",
    "scenario": "The type of budget scenario, such as whether the data is actuals (Actual) or budgeted (Adopted Budget, July Plan, November Plan).",
    "financial_plan_year": "The year the budget scenario was published. For actuals, the Financial Plan Year will always equal the fiscal year.",
    "expense_type": "Whether the expense was reimbursable (REIMB) or non-reimbursable (NREIMB).",
    "agency": "The agency where the expenses or revenue are accounted for. Examples include NYC Transit (NYCT), Long Island Rail Road (LIRR), Metro-North Railroad (MNR), Bridges & Tunnels (BT), etc.",
    "type": "Distinguishes between revenue and expenses.",
    "subtype": "Populated for expenses. Distinguishes between labor, non-labor, debt service, or extraordinary events expenses.",
    "general_ledger": "Aggregates the chart of accounts into meaningful categories consistently published monthly by the MTA in its Accrual Statement of Operations.",
    "amount": "The financial amount, can be a decimal or negative for transfers within the agency, in dollars."
}

descriptions_sf_air_traffic_landings = {
    "activity_period": "The year and month at which passenger, cargo or landings activity took place.",
    "activity_period_start_date": "Start date of the year and month at which passenger, cargo or landings activity took place.",
    "operating_airline": "Airline name for the operator of aircraft with landing activity.",
    "operating_airline_iata_code": "The International Air Transport Association (IATA) two-letter designation for the Operating Airline.",
    "published_airline": "Airline name that issues the ticket and books revenue for landings activity.",
    "published_airline_iata_code": "The International Air Transport Association (IATA) two-letter designation for the Published Airline.",
    "geo_summary": "Designates whether the passenger, cargo or landings activity in relation to SFO arrived from or departed to a location within the United States (“domestic”), or outside the United States (“international”) without stops.",
    "geo_region": "Provides a more detailed breakdown of the GEO Summary field to designate the region in the world where activity in relation to SFO arrived from or departed to without stops.",
    "landing_aircraft_type": "A designation for three types of aircraft that landed at SFO, which includes passenger aircraft, cargo-only aircraft (“freighters”) or combination aircraft (“combi”).",
    "aircraft_body_type": "A designation that is independent from Landing Aircraft Type, which determines whether commercial aircraft landed at SFO is a wide body jet, narrow body jet, regional jet or a propeller operated aircraft.",
    "aircraft_manufacturer": "Manufacturer name for the aircraft that landed at SFO.",
    "aircraft_model": "Model designation of aircraft by the manufacturer.",
    "aircraft_version": "Variations of the Aircraft Model, also known as the 'dash number', designated by the manufacturer to segregate unique versions of the same model.",
    "landing_count": "The number of aircraft landings associated with General and Landings Statistics attribute fields.",
    "total_landed_weight": "The aircraft landed weight (in pounds) associated with General and Landings Statistics attribute fields."
}

descriptions_sf_air_traffic_passenger_stats = {
    "activity_period": "The year and month when this activity occurred.",
    "activity_period_start_date": "Start date of the activity period.",
    "operating_airline": "Airline name for the operator of aircraft with passenger activity.",
    "operating_airline_iata_code": "The International Air Transport Association (IATA) two-letter designation for the Operating Airline.",
    "published_airline": "Airline name that issues the ticket and books revenue for passenger activity.",
    "published_airline_iata_code": "The International Air Transport Association (IATA) two-letter designation for the Published Airline.",
    "geo_summary": "Designates whether the passenger activity in relation to SFO arrived from or departed to a location within the United States (“domestic”), or outside the United States (“international”) without stops.",
    "geo_region": "Provides a more detailed breakdown of the GEO Summary field to designate the region in the world where activity in relation to SFO arrived from or departed to without stops.",
    "activity_type_code": "A description of the physical action a passenger took in relation to a flight, which includes boarding a flight (“enplanements”), getting off a flight (“deplanements”) and transiting to another location (“intransit”).",
    "price_category_code": "A categorization of whether a Published Airline is a low-cost carrier or not a low-cost carrier.",
    "terminal": "Name of a terminal of the airport.",
    "boarding_area": "Letter that represents a boarding area.",
    "passenger_count": "Total number of passengers this month.",
    "data_as_of": "Datetime of data as of.",
    "data_loaded_at": "Datetime of data loaded at."
}

descriptions_sf_air_traffic_cargo = {
    "activity_period": "The year and month when this activity occurred.",
    "activity_period_start_date": "Start date of the year and month when this activity occurred.",
    "operating_airline": "Airline name for the operator of aircraft with cargo activity.",
    "operating_airline_iata_code": "The International Air Transport Association (IATA) two-letter designation for the Operating Airline.",
    "published_airline": "Airline name that issues the ticket and books revenue for cargo activity.",
    "published_airline_iata_code": "The International Air Transport Association (IATA) two-letter designation for the Published Airline.",
    "geo_summary": "An airport-defined high-level geographical categorization of the flight operations.",
    "geo_region": "An airport-defined world region of the flight operations.",
    "activity_type_code": "Short code that represents the type of activities such as enplaned, deplaned, and transit.",
    "cargo_type_code": "Short code that represents a broad categorization of the type of cargo such as 'mail'.",
    "cargo_aircraft_type": "Short code that represents the type of the cargo aircraft.",
    "cargo_weight_lbs": "The weight (in pounds) of air cargo associated with General and Cargo Statistics attribute fields.",
    "cargo_metric_tons": "The weight (in metric tons) of air cargo associated with General and Cargo Statistics attribute fields.",
    "data_as_of": "Datetime of the data as of.",
    "data_loaded_at": "Datetime the data is loaded."
}

descriptions_mta_bus_wait_time = {
    "month": "Represents the time period in which the wait assessment is being calculated (yyyy-mm-dd).",
    "borough": "Represents the five boroughs of New York City (Bronx, Brooklyn, Manhattan, Queens, Staten Island).",
    "day_type": "Represents 1 as weekday and 2 as weekend.",
    "trip_type": "The type of bus service provided: EXP (Express), LCL/LTD (Local/Limited), SBS (Select Bus Service).",
    "route_id": "Identifies each individual bus route, as well as cumulative totals for all bus routes (identified as ALL).",
    "period": "Represents both the peak and off-peak service periods.",
    "number_of_trips_passing_wait": "The number of trips that are no more than three minutes over their scheduled intervals for each bus route, monthly, during peak and off-peak hours.",
    "number_of_scheduled_trips": "The number of scheduled trips for each bus route, monthly, during peak and off-peak hours.",
    "wait_assessment": "The percentage of trips that pass the wait assessment on each bus route, monthly, during peak and off-peak hours."
}

descriptions_mta_bus_speeds = {
    "month": "Represents the time period in which the average speed is being calculated (yyyy-mm-dd).",
    "borough": "Represents the five boroughs of New York City (Bronx, Brooklyn, Manhattan, Queens, Staten Island).",
    "day_type": "Represents 1 as weekday and 2 as weekend.",
    "trip_type": "The type of bus service provided: EXP (Express), LCL/LTD (Local/Limited), SBS (Select Bus Service).",
    "route_id": "Identifies each individual bus route, as well as cumulative totals for all bus routes (identified as ALL).",
    "period": "Represents both the peak and off-peak service periods.",
    "total_mileage": "The total mileage for each bus route, monthly, during peak and off-peak hours.",
    "total_operating_time": "The total operating time for each bus route, monthly, during peak and off-peak hours.",
    "average_speed": "The average speed is based on the total operating time and total mileage, per bus route, per month."
}


# Automatically generate table_descriptions
table_descriptions = {
    var_name.replace('descriptions_', ''): var_value
    for var_name, var_value in globals().items()
    if var_name.startswith('descriptions_')
}
