package main

import (
	"fmt" 
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

var (
	detail_id [] int64
	county_id [] int64 
	lac_id [] int64 
	ward_id [] int64 
	country_id [] int64 
	region_id [] int64 
	par_cons_id [] int64 
	eer_id [] int64 
	pct_id [] int64 
	lsoa_id [] int64 
	msoa_id [] int64 
	oac_id [] int64 
	greek_coordinate_id [] int64 
)

func retrieve_detail() {
	
	rows, err := db.Query("SELECT pos_detail_id FROM nspl_rawdata AS rawdata JOIN postcode_detail AS detail ON detail.pos1 = rawdata.postcode1 AND detail.pos2 = rawdata.postcode2 AND detail.pos3 = rawdata.postcode3 AND detail.pos_date_introduce = rawdata.date_introduce AND detail.pos_usertype = rawdata.usertype AND detail.position_quality = rawdata.position_quality AND detail.pos_spatial_accuracy = rawdata.spatial_accuracy AND     detail.pos_location = rawdata.location AND detail.pos_socrataid = rawdata.socrataid AND detail.pos_last_upload = rawdata.last_upload;" )

	checkErr(err, "Error on query DB")	

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_detail_id)
			checkErr(err, "Retrieve pos_detail_id key")
			
			detail_id = append(detail_id, n.pos_detail_id); 
    }
    
    fmt.Printf("Postcode detail: %d \n", len(detail_id))
    defer rows.Close()
}

func retrieve_county() {
	
	rows, err := db.Query("SELECT pos_county_id FROM nspl_rawdata AS rawdata JOIN postcode_county AS county ON county.pos_county_code = rawdata.countycode AND county.pos_county_name = rawdata.countyname JOIN postcode_local_authority_county AS lac ON lac.pos_lac_code = rawdata.county_lac AND lac.pos_lac_name = rawdata.county_lan;" )

	checkErr(err, "Error on query DB")	

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_county_id)
			checkErr(err, "Retrieve pos_county_id key")
			
			county_id = append(county_id, n.pos_county_id); 
    }
    
    fmt.Printf("Postcode county: %d \n", len(county_id))
    defer rows.Close()
}

func retrieve_local_authority_council() {
	
	rows, err := db.Query("SELECT pos_lac_id FROM nspl_rawdata AS rawdata JOIN postcode_local_authority_county AS lac ON lac.pos_lac_code = rawdata.county_lac AND lac.pos_lac_name = rawdata.county_lan;" )

	checkErr(err, "Error on query DB")	

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_lac_id)
			checkErr(err, "Retrieve pos_lac_id key")
			
			lac_id = append(lac_id, n.pos_lac_id); 
    }
    
    fmt.Printf("Postcode Local Authority Council: %d \n", len(lac_id))
    defer rows.Close()
}

func retrieve_ward() {
	
	rows, err := db.Query("SELECT pos_ward_id FROM nspl_rawdata AS rawdata JOIN postcode_ward AS ward ON ward.pos_ward_code = rawdata.wardcode AND ward.pos_ward_name = rawdata.wardname;" )
	checkErr(err, "Error on query pos_ward_id")	

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_ward_id)
			checkErr(err, "Retrieve pos_ward_id key")
			
			ward_id = append(ward_id, n.pos_ward_id); 
    }
    
    fmt.Printf("Postcode Ward: %d \n", len(ward_id))
    defer rows.Close()
}

func retrieve_country() {
	
	rows, err := db.Query("SELECT pos_country_id FROM nspl_rawdata AS rawdata JOIN postcode_country AS country ON country.pos_country_code = rawdata.countrycode AND country.pos_country_name = rawdata.countryname;" )
	checkErr(err, "Error on query pos_country_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_country_id)
			checkErr(err, "Retrieve pos_country_id key")
			
			country_id = append(country_id, n.pos_country_id); 
    }
    
    fmt.Printf("Postcode Country: %d \n", len(country_id))
    defer rows.Close()
}

func retrieve_region() {
	
	rows, err := db.Query("SELECT pos_region_id FROM nspl_rawdata AS rawdata JOIN postcode_region AS region ON region.pos_region_code = rawdata.region_code AND region.pos_region_name = rawdata.region_name;" )
	checkErr(err, "Error on query pos_region_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_region_id)
			checkErr(err, "Retrieve pos_region_id key")
			
			region_id = append(region_id, n.pos_region_id); 
    }
    
    fmt.Printf("Postcode Region: %d \n", len(region_id))
    defer rows.Close()
}

func retrieve_parliament_constituency() {
	
	rows, err := db.Query("SELECT pos_par_cons_id FROM nspl_rawdata AS rawdata JOIN postcode_parliament_constituency AS ppc ON ppc.pos_par_cons_code = rawdata.par_cons_code AND ppc.pos_par_cons_name = rawdata.par_cons_name;" )
	checkErr(err, "Error on query pos_par_cons_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_par_cons_id)
			checkErr(err, "Retrieve pos_par_cons_id key")
			
			par_cons_id = append(par_cons_id, n.pos_par_cons_id); 
    }
    
    fmt.Printf("Postcode Parliament Constituency: %d \n", len(par_cons_id))
    defer rows.Close()
}

func retrieve_euro_electoral_region() {
	
	rows, err := db.Query("SELECT pos_eer_id FROM nspl_rawdata AS rawdata JOIN postcode_euro_electoral_region AS eer ON eer.pos_eer_code = rawdata.eerc AND eer.pos_eer_name = rawdata.eern;" )
	checkErr(err, "Error on query pos_eer_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_eer_id)
			checkErr(err, "Retrieve pos_eer_id key")
			
			eer_id = append(eer_id, n.pos_eer_id); 
    }
    
    fmt.Printf("Postcode Euro Electoral Region: %d \n", len(eer_id))
    defer rows.Close()
}

func retrieve_primary_care_trust() {
	
	rows, err := db.Query("SELECT pos_pct_id FROM nspl_rawdata AS rawdata JOIN postcode_primary_care_trust AS pct ON pct.pos_pct_code = rawdata.pctc AND pct.pos_pct_name = rawdata.pctn;" )
	checkErr(err, "Error on query pos_pct_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_pct_id)
			checkErr(err, "Retrieve pos_pct_id key")
			
			pct_id = append(pct_id, n.pos_pct_id); 
    }
    
    fmt.Printf("Postcode Primary Care Trust: %d \n", len(pct_id))
    defer rows.Close()
}

func retrieve_lower_super_output_area () {
	
	rows, err := db.Query("SELECT pos_lsoa_id FROM nspl_rawdata AS rawdata JOIN postcode_lower_super_output_area AS lsoa ON lsoa.pos_lsoa_code = rawdata.isoac AND lsoa.pos_lsoa_name = rawdata.isoan;" )
	checkErr(err, "Error on query pos_lsoa_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_lsoa_id)
			checkErr(err, "Retrieve pos_lsoa_id key")
			
			lsoa_id = append(lsoa_id, n.pos_lsoa_id); 
    }
    
    fmt.Printf("Postcode Lower Super Output Area: %d \n", len(lsoa_id))
    defer rows.Close()
}

func retrieve_middle_super_output_area() {
	
	rows, err := db.Query("SELECT pos_msoa_id FROM nspl_rawdata AS rawdata JOIN postcode_middle_super_output_area AS msoa ON msoa.pos_msoa_code = rawdata.msoac AND msoa.pos_msoa_name = rawdata.msoan;" )
	checkErr(err, "Error on query pos_msoa_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_msoa_id)
			checkErr(err, "Retrieve pos_msoa_id key")
			
			msoa_id = append(msoa_id, n.pos_msoa_id); 
    }
    
    fmt.Printf("Postcode Middle Super Output Area: %d \n", len(msoa_id))
    defer rows.Close()
}

func retrieve_output_area_classification() {
	
	rows, err := db.Query("SELECT pos_oac_id FROM nspl_rawdata AS rawdata JOIN postcode_output_area_classification AS oac ON oac.pos_oac_code = rawdata.oacc AND oac.pos_oac_name = rawdata.oacn;" )
	checkErr(err, "Error on query pos_oac_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_oac_id)
			checkErr(err, "Retrieve pos_oac_id key")
			
			oac_id = append(oac_id, n.pos_oac_id); 
    }
    
    fmt.Printf("Postcode Output Area Classification: %d \n", len(oac_id))
    defer rows.Close()
}

func retrieve_greek_coordinate() {
	
	rows, err := db.Query("SELECT pos_greek_coordinate_id FROM nspl_rawdata AS rawdata JOIN postcode_greek_coordinate AS pgc ON pgc.pos_longitude = rawdata.longitude AND pgc.pos_latitude = rawdata.latitude;" )
	checkErr(err, "Error on query pos_greek_coordinate_id")	 

    for rows.Next() {
	 
            var n postcode_id
            
            err = rows.Scan(&n.pos_greek_coordinate_id)
			checkErr(err, "Retrieve pos_greek_coordinate_id key")
			
			greek_coordinate_id = append(greek_coordinate_id, n.pos_greek_coordinate_id); 
    }
    
    fmt.Printf("Postcode Greek Coordinate: %d \n", len(greek_coordinate_id))
    defer rows.Close()
}