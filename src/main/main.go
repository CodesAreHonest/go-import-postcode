package main

import (
	"log"
	"fmt" 
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"database/sql"
	"sync"
)

const (
	DB_USER     = "yinghua"
	DB_PASSWORD = "123"
	DB_NAME     = "postcode"
	ENTRIES	    = 1754882 
)

var ( 
	db *sql.DB
	sqlStatement = "INSERT INTO postcode (pos_detail_id, pos_county_id, pos_lac_id, pos_ward_id, pos_country_id, pos_region_id, pos_par_cons_id, pos_eer_id, pos_pct_id, pos_lsoa_id, pos_msoa_id, pos_oac_id, pos_greek_coordinate_id) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);";

)

//====================================================
//function to check error and print error messages
//====================================================
func checkErr(err error, message string) {
	if err != nil {
		panic(message + " err: " + err.Error())
	}
}

//====================================================
// initialize connection with database
//====================================================
func initDB() {

	dbInfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		DB_USER, DB_PASSWORD, DB_NAME)
	sqldb, err := sql.Open("postgres", dbInfo)
	checkErr(err, "Initialize database")
	
	sqldb.SetMaxOpenConns(90)
	db = sqldb
}

func main() { 
	initDB()
	retrieve_key()
	insert_key()
	
	fmt.Println("The postcode data had migrated complete")
}

func insert_key() {
	
	fmt.Println("Begin to migrate postcode data")
	
	stmt, err := db.Prepare(sqlStatement)
	checkErr(err, "Prepare insert statement")
	
	wg := sync.WaitGroup{}
	
	// ensure all routines finish before returning
	defer wg.Wait()
	
	for i := ENTRIES; i > 0 ; i-- { 
		wg.Add(1)
		go func () {
			defer wg.Done()
			res, err := stmt.Exec(detail_id[i],  county_id[i], lac_id[i], ward_id[i], country_id[i], region_id[i], par_cons_id[i], eer_id[i], pct_id[i], lsoa_id[i], msoa_id[i], oac_id[i], greek_coordinate_id[i])
			checkErr(err, "Insert statement execution error") 
//			stmt.Close()
			if res == nil { 
				log.Fatal(err)
			}
		}()
	} 
}


func retrieve_key() { 
	retrieve_detail()
	retrieve_county()
	retrieve_local_authority_council()
	retrieve_ward() 
	retrieve_country()
	retrieve_region() 
	retrieve_parliament_constituency()
	retrieve_euro_electoral_region()
	retrieve_primary_care_trust()
	retrieve_lower_super_output_area()
	retrieve_middle_super_output_area()
	retrieve_output_area_classification()
	retrieve_greek_coordinate()
}

