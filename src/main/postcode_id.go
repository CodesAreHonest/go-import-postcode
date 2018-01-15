package main

import (
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/lib/pq"

)

type postcode_id struct { 
	pos_detail_id int64
	pos_county_id int64 
	pos_lac_id int64 
	pos_ward_id int64 
	pos_country_id int64 
	pos_region_id int64 
	pos_par_cons_id int64 
	pos_eer_id 				 int64 
	pos_pct_id 				 int64 
	pos_lsoa_id 			 int64 
	pos_msoa_id 			 int64 
	pos_oac_id				 int64 
	pos_greek_coordinate_id  int64 
}

