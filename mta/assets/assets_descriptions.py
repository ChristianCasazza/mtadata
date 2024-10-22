# Descriptions for mta_subway_origin_destination_2023 table
descriptions_mta_subway_origin_destination_2023 = {
    "year": "The year in which the subway trips occurred.",
    "month": "The month in which the subway trips occurred.",
    "day_of_week": "The day of the week on which the subway trips occurred (Monday, Tuesday, etc.).",
    "hour_of_day": "The hour of the day in which the subway trips occurred. All trip times are rounded down to the nearest hour.",
    "timestamp": "Representative datetime for the year, month, day of week, and hour of day in which the subway trips occurred.",
    "origin_station_complex_id": "The unique identifier for the subway station complex where the trips originated.",
    "origin_station_complex_name": "The name of the subway station complex where the trips originated.",
    "origin_latitude": "The latitude of the subway station complex where the trips originated.",
    "origin_longitude": "The longitude of the subway station complex where the trips originated.",
    "destination_station_complex_id": "The unique identifier for the subway station complex where the trips are inferred to have ended.",
    "destination_station_complex_name": "The name of the subway station complex where the trips are inferred to have ended.",
    "destination_latitude": "The latitude of the subway station complex where the trips are inferred to have ended.",
    "destination_longitude": "The longitude of the subway station complex where the trips are inferred to have ended.",
    "estimated_average_ridership": "The estimated ridership for an origin-destination pair and hour of day, averaged by day of week over the calendar month.",
    "origin_point": "Geocoding information for the origin station. Point-type location representing the centroid of the station complex.",
    "destination_point": "Geocoding information for the destination station. Point-type location representing the centroid of the station complex."
}

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

# Automatically generate table_descriptions
table_descriptions = {
    var_name.replace('descriptions_', ''): var_value
    for var_name, var_value in globals().items()
    if var_name.startswith('descriptions_')
}
