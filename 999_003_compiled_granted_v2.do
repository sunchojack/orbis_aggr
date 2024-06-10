clear all
set more off

global path="C:\wiiw Dropbox\Mahdi Ghodsi\Wien\Data\Orbis\Patent\2024-04-22\"
global db="C:\wiiw Dropbox\Mahdi Ghodsi\Wien\Data\Orbis\Patent\2024-04-22\DB2\"
global odbc="C:\wiiw Dropbox\Mahdi Ghodsi\Wien\Data\Orbis\Data\ODBC\ODBC\"
global cc="C:\wiiw Dropbox\Mahdi Ghodsi\Wien\Data\Country Codes"
global eu="C:\wiiw Dropbox\Mahdi Ghodsi\Wien\Data\EU\"

capture noisily mkdir"$path\out\"
capture noisily mkdir"$path\out\Combined_granted\"
capture noisily mkdir"$path\out\Combined_granted\activity\"
capture noisily mkdir"$path\out\Combined_granted\activity\LMS\"

capture noisily mkdir"$path\out\Combined_granted\guo\"
capture noisily mkdir"$path\out\Combined_granted\guo\LMS\"

forvalues d=1890(10)2020 {
	capture noisily mkdir "$path\out\\`d's\\Combined_granted\"
}
/*
* Loop through decades, without dates
forvalues d=1890(10)2020 {
	display "`d'"
	capture noisily mkdir "$path\out\\`d's\"
	
	global decade_path = "$db\\`d's\\"
    global decade_out = "$path\out\\`d's\\"
	
    * Change directory
    cd "$decade_path\"
	
    * List all items in the directory
	clear
	global folders: dir "$decade_path" dirs "*"
	local n=1
	gen loc=""
    foreach f of global folders {
		set obs `n'
		replace loc="`f'" in `n'
		local n= `n' + 1
	}
	merge 1:1 loc using "$path\\dates.dta", keep(1) nogen
	drop if loc =="application_filing date"
	levelsof loc, local(fold)
	foreach fd of local fold {
		capture findfile "`fd'_`d's.zip", path("$path\out\\`d's\\`fd'\\") all
		if `"`r(fn)'"'=="" {
			capture findfile "`fd'_`d's.dta", path("$path\out\\`d's\\`fd'\\") all
			if `"`r(fn)'"'=="" {
				display "`fd'"
				capture noisily mkdir "$path\out\\`d's\\`fd'\"
				import delimited "$decade_path\\`fd'\\`fd'_`d's.csv", varnames(1) case(preserve) clear
				sort Publicationnumber
				compress
				save "$path\out\\`d's\\`fd'\\`fd'_`d's.dta", replace
			}
			cd "$path\out\\`d's\\`fd'\\"
			zipfile "`fd'_`d's.dta", saving("$path\out\\`d's\\`fd'\\`fd'_`d's.zip", replace)
			erase "`fd'_`d's.dta"
		}
    }
}

*-------------------------------------------------------------------------------------------------------------------------------------------
* Loop through decades, without dates
forvalues d=1890(10)2020 {
	display "`d'"
	capture noisily mkdir "$path\out\\`d's\"
	
	global decade_path = "$db\\`d's\\"
    global decade_out = "$path\out\\`d's\\"
	
    * Change directory
    cd "$decade_path\"
    * List all items in the directory
	clear
	global folders: dir "$decade_path" dirs "*"
	local n=1
	gen loc=""
    foreach f of global folders {
		set obs `n'
		replace loc="`f'" in `n'
		local n= `n' + 1
	}
	merge 1:1 loc using "$path\\dates.dta", keep(1) nogen
	keep if loc =="application_filing date"
	levelsof loc, local(fold)
	foreach fd of local fold {
		capture findfile "`fd'_`d's.zip", path("$path\out\\`d's\\`fd'\\") all
		if `"`r(fn)'"'=="" {
			capture findfile "`fd'_`d's.dta", path("$path\out\\`d's\\`fd'\\") all
			if `"`r(fn)'"'=="" {
				display "`fd'"
				capture noisily mkdir "$path\out\\`d's\\`fd'\"
				import delimited "$decade_path\\`fd'\\`fd'_`d's.csv", varnames(1) case(preserve) clear
				sort Publicationnumber
				gen year=substr(Applicationfilingdate ,1,4)
				gen month=substr(Applicationfilingdate ,6,2)
				gen day =substr(Applicationfilingdate ,9,2)
				gen date = date(year + "-" + month + "-" + day, "YMD")
				format date %td
				destring year, force replace
				keep Publicationnumber date year 
				sort Publicationnumber date year
				capture ren date date_application
				capture ren year year_application
				compress
				save "$path\out\\`d's\\`fd'\\`fd'_`d's.dta", replace
			}
			cd "$path\out\\`d's\\`fd'\\"
			zipfile "`fd'_`d's.dta", saving("$path\out\\`d's\\`fd'\\`fd'_`d's.zip", replace)
			erase "`fd'_`d's.dta"
		}
    }
}
*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
* Loop through decades for date variables
forvalues d=1890(10)2020 {
	display "`d'"
	capture noisily mkdir "$path\out\\`d's\"
	
	global decade_path = "$db\\`d's\\"
    global decade_out = "$path\out\\`d's\\"
	
    * Change directory
    cd "$decade_path\"
    * List all items in the directory
	clear
	global folders: dir "$decade_path" dirs "*"
	local n=1
	gen loc=""
    foreach f of global folders {
		set obs `n'
		replace loc="`f'" in `n'
		local n= `n' + 1
	}
	merge 1:1 loc using "$path\\dates.dta", keep(3) nogen
	*keep if loc=="publication date"
	local l=0
	levelsof loc, local(fold)
	foreach fd of local fold {
	local l=`l'+1
		capture findfile "`fd'_`d's.zip", path("$path\out\\`d's\\`fd'\\") all
		if `"`r(fn)'"'=="" {
			capture findfile "`fd'_`d's.dta", path("$path\out\\`d's\\`fd'\\") all
			if `"`r(fn)'"'=="" {
				display "`fd'"
				capture noisily mkdir "$path\out\\`d's\\`fd'\"
				import delimited "$decade_path\\`fd'\\`fd'_`d's.csv", varnames(1) case(preserve) clear
				if `l'==1 {
					display "Expirationdate"
					capture confirm string variable Expirationdate
				}
				if `l'==2 {
					display "Grantdate"
					capture confirm string variable Grantdate
				}
				if `l'==3 {
					display "Prioritydate"
					capture confirm string variable Prioritydate
				}
				if `l'==4 {
					display "Publicationdate"
					capture confirm string variable Publicationdate
				}
				if _rc == 0 {
					* Commands to execute if Expirationdate is a string
					capture destring Expirationdate, generate(date) force
					capture destring Grantdate, generate(date) force
					capture destring Prioritydate, generate(date) force
					capture destring Publicationdate, generate(date) force
					
					replace date = date - 21916 
					format date %td

					* Generate the year from the date
					generate year = year(date)

					* Calculate the length of the original Expirationdate string
					capture generate length = strlen(Expirationdate)
					capture generate length = strlen(Grantdate)
					capture generate length = strlen(Prioritydate)
					capture generate length = strlen(Publicationdate)

					* Calculate the starting position of the year in Expirationdate
					generate length_year = length - 4 + 1

					* Extract the year as a string
					capture generate year_string = substr(Expirationdate, length_year, 4)
					capture generate year_string = substr(Grantdate, length_year, 4)
					capture generate year_string = substr(Prioritydate, length_year, 4)
					capture generate year_string = substr(Publicationdate, length_year, 4)
					

					* Convert the extracted year to a numeric variable
					destring year_string, generate(year_number) force

					* Replace year with year_number where date is missing and year_number is not missing
					replace year = year_number if date == . & year_number != .

					* Clean up the dataset
					capture drop Expirationdate length length_year year_string year_number
					capture drop Grantdate length length_year year_string year_number
					capture drop Prioritydate length length_year year_string year_number
					capture drop Publicationdate length length_year year_string year_number
				}
				else {
					capture ren Expirationdate date
					capture ren Grantdate date
					capture ren Prioritydate date
					capture ren Publicationdate date
					replace date = date - 21916 
					format date %td
					generate year = year(date)
				}
				* Sort by Publicationnumber and year
				sort Publicationnumber date year
				ren date date_`l'
				ren year year_`l'
				capture ren date_1 date_expiration
				capture ren date_2 date_grant
				capture ren date_3 date_priority
				capture ren date_4 date_publication
				capture ren year_1 year_expiration
				capture ren year_2 year_grant
				capture ren year_3 year_priority
				capture ren year_4 year_publication
				compress
				save "$path\out\\`d's\\`fd'\\`fd'_`d's.dta", replace
			}
			cd "$path\out\\`d's\\`fd'\\"
			zipfile "`fd'_`d's.dta", saving("$path\out\\`d's\\`fd'\\`fd'_`d's.zip", replace)
			erase "`fd'_`d's.dta"
		}
    }
}
*/
*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
***Merging all
forvalues d=1900(10)2020 {
	capture findfile "owner_year_granted_`d's_v2.zip", path("$path\out\\`d's\\Combined_granted\") all
	if `"`r(fn)'"'=="" {
		capture findfile "owner_year_granted_`d's_v2.dta", path("$path\out\\`d's\\Combined_granted\") all
		if `"`r(fn)'"'=="" {
			capture findfile "owner_publication_allDirectOwners_granted_`d's_v2.zip", path("$path\out\\`d's\\Combined_granted\\") all
			if `"`r(fn)'"'=="" {
				capture findfile "owner_publication_allDirectOwners_granted_`d's_v2.dta", path("$path\out\\`d's\\Combined_granted\\") all
				if `"`r(fn)'"'=="" {
					capture findfile "owner_publication_allDirectOwners_`d's_v2.zip", path("$path\out\\`d's\\Combined\\") all
					if `"`r(fn)'"'=="" {
						capture findfile "owner_publication_allDirectOwners_`d's_v2.dta", path("$path\out\\`d's\\Combined\\") all
						if `"`r(fn)'"'=="" {
							cd "$path\out\\`d's\\current direct owner(s) bvd id number(s)\"
							capture findfile "current direct owner(s) bvd id number(s)_`d's.dta", path("$path\out\\`d's\\current direct owner(s) bvd id number(s)\") all
							if `"`r(fn)'"'=="" {
								unzipfile "current direct owner(s) bvd id number(s)_`d's.zip", replace
							}
							use "$path\out\\`d's\\current direct owner(s) bvd id number(s)\current direct owner(s) bvd id number(s)_`d's.dta", clear
							erase "$path\out\\`d's\\current direct owner(s) bvd id number(s)\current direct owner(s) bvd id number(s)_`d's.dta"
							ren CurrentdirectownersBvDIDNumbers owner
							split owner, p(",")
							local nr=r(k_new)
							preserve
							keep owner1 Publicationnumber 
							drop if owner1==""
							ren owner1 owner
							save "$path\out\\`d's\\Combined\\owner_publication_tmp1_v2.dta", replace
							restore
							capture drop if owner2==""
							forvalues n=2(1)`nr'{
								preserve
								keep owner`n' Publicationnumber 
								drop if owner`n'==""
								ren owner`n' owner
								save "$path\out\\`d's\\Combined\\owner_publication_tmp`n'_v2.dta", replace
								restore
							}
							use "$path\out\\`d's\\Combined\\owner_publication_tmp1_v2.dta", clear
							forvalues n=2(1)`nr'{
								append using "$path\out\\`d's\\Combined\\owner_publication_tmp`n'_v2.dta"
							}
							forvalues n=1(1)`nr'{
								erase "$path\out\\`d's\\Combined\\owner_publication_tmp`n'_v2.dta"
							}
							duplicates tag Publicationnumber , gen(dup)
							compress
							save "$path\out\\`d's\\Combined\\owner_publication_allDirectOwners_`d's_v2.dta", replace
						}
						cd "$path\out\\`d's\\Combined\\"
						zipfile "owner_publication_allDirectOwners_`d's_v2.dta", saving("owner_publication_allDirectOwners_`d's_v2.zip", replace)
					}
				
					cd "$path\out\\`d's\\Combined\"
					capture findfile "owner_publication_allDirectOwners_`d's_v2.dta", path("$path\out\\`d's\\Combined\") all
					if `"`r(fn)'"'=="" {
						unzipfile "owner_publication_allDirectOwners_`d's_v2.zip", replace
					}
					
					cd "$path\out\\`d's\\grant date\"
					capture findfile "grant date_`d's.dta", path("$path\out\\`d's\\grant date\") all
					if `"`r(fn)'"'=="" {
						unzipfile "grant date_`d's.zip", replace
					}
					use "$path\out\\`d's\\Combined\\owner_publication_allDirectOwners_`d's_v2.dta", clear
					merge m:1 Publicationnumber using "grant date_`d's.dta", keep(3) nogen
					erase "grant date_`d's.dta"
					keep Publicationnumber owner dup
					duplicates drop
					compress
					save "$path\out\\`d's\\Combined_granted\\owner_publication_allDirectOwners_granted_`d's_v2.dta", replace
				}
				cd "$path\out\\`d's\\Combined_granted\\"
				zipfile "owner_publication_allDirectOwners_granted_`d's_v2.dta", saving("owner_publication_allDirectOwners_granted_`d's_v2.zip", replace)
			}
			cd "$path\out\\`d's\\Combined_granted\"
			capture findfile "owner_publication_allDirectOwners_granted_`d's_v2.dta", path("$path\out\\`d's\\Combined_granted\") all
			if `"`r(fn)'"'=="" {
				unzipfile "owner_publication_allDirectOwners_granted_`d's_v2.zip", replace
			}
					
			cd "$path\out\\`d's\\application_filing date\"
			capture findfile "application_filing date_`d's.dta", path("$path\out\\`d's\\application_filing date\") all
			if `"`r(fn)'"'=="" {
				unzipfile "application_filing date_`d's.zip", replace
			}
			use "$path\out\\`d's\\Combined_granted\\owner_publication_allDirectOwners_granted_`d's_v2.dta", clear
			merge m:1 Publicationnumber using "application_filing date_`d's.dta", keep(1 3) nogen
			erase "application_filing date_`d's.dta"
			duplicates drop
			gen applied=1/(dup+1)
			*merge 1:1 Publicationnumber using "$path\out\\`d's\\Combined_granted\bw_cite.dta", keep(1 3) nogen
			*merge 1:1 Publicationnumber using "$path\out\\`d's\\Combined_granted\fw_cite.dta", keep(1 3) nogen
			collapse (rawsum) applied , by(owner year_application )
			drop if year_application==.
			ren year_application year
			order owner year applied 
			sort owner year 
			compress
			save "$path\out\\`d's\\Combined_granted\\owner_year_application_granted_`d's_v2.dta", replace
			
			
			
			cd "$path\out\\`d's\\priority date\"
			capture findfile "priority date_`d's.dta", path("$path\out\\`d's\\priority date\") all
			if `"`r(fn)'"'=="" {
				unzipfile "priority date_`d's.zip", replace
			}
			use "$path\out\\`d's\\Combined_granted\\owner_publication_allDirectOwners_granted_`d's_v2.dta", clear
			merge m:1 Publicationnumber using "priority date_`d's.dta", keep(1 3) nogen
			erase "priority date_`d's.dta"
			gen piority=1/(dup+1)
			collapse (rawsum) piority , by(owner year_priority )
			drop if year_priority==.
			ren year_priority year
			order owner year piority 
			sort owner year 
			compress
			save "$path\out\\`d's\\Combined_granted\owner_year_priority_granted_`d's_v2.dta", replace
			
			
			
			cd "$path\out\\`d's\\publication date\"
			capture findfile "publication date_`d's.dta", path("$path\out\\`d's\\publication date\") all
			if `"`r(fn)'"'=="" {
				unzipfile "publication date_`d's.zip", replace
			}
			use "$path\out\\`d's\\Combined_granted\\owner_publication_allDirectOwners_granted_`d's_v2.dta", clear
			merge m:1 Publicationnumber using "publication date_`d's.dta", keep(1 3) nogen
			erase "publication date_`d's.dta"
			gen published=1/(dup+1)
			collapse (rawsum) published , by(owner year_publication )
			drop if year_publication==.
			ren year_publication year
			order owner year published 
			sort owner year 
			compress
			save "$path\out\\`d's\\Combined_granted\owner_year_publication_granted_`d's_v2.dta", replace
			
			
			
			cd "$path\out\\`d's\\grant date\"
			capture findfile "grant date_`d's.dta", path("$path\out\\`d's\\grant date\") all
			if `"`r(fn)'"'=="" {
				unzipfile "grant date_`d's.zip", replace
			}
			use "$path\out\\`d's\\Combined_granted\\owner_publication_allDirectOwners_granted_`d's_v2.dta", clear
			merge m:1 Publicationnumber using "grant date_`d's.dta", keep(1 3) nogen
			erase "grant date_`d's.dta"
			gen granted=1/(dup+1)
			collapse (rawsum) granted , by(owner year_grant )
			drop if year_grant==.
			ren year_grant year
			order owner year granted 
			sort owner year 
			compress
			save "$path\out\\`d's\\Combined_granted\owner_year_grant_granted_`d's_v2.dta", replace
			
			
			
			cd "$path\out\\`d's\\expiration date\"
			capture findfile "expiration date_`d's.dta", path("$path\out\\`d's\\expiration date\") all
			if `"`r(fn)'"'=="" {
				unzipfile "expiration date_`d's.zip", replace
			}
			use "$path\out\\`d's\\Combined_granted\\owner_publication_allDirectOwners_granted_`d's_v2.dta", clear
			merge m:1 Publicationnumber using "expiration date_`d's.dta", keep(1 3) nogen
			erase "expiration date_`d's.dta"
			gen expired=1/(dup+1)
			collapse (rawsum) expired , by(owner year_expiration )
			drop if year_expiration==.
			ren year_expiration year
			order owner year expired 
			sort owner year 
			compress
			save "$path\out\\`d's\\Combined_granted\owner_year_expiration_granted_`d's_v2.dta", replace
			
			**Now merging them
			use "$path\out\\`d's\\Combined_granted\owner_year_application_granted_`d's_v2.dta", clear
			merge 1:1 owner year using "$path\out\\`d's\\Combined_granted\owner_year_priority_granted_`d's_v2.dta", nogen
			merge 1:1 owner year using "$path\out\\`d's\\Combined_granted\owner_year_publication_granted_`d's_v2.dta", nogen
			merge 1:1 owner year using "$path\out\\`d's\\Combined_granted\owner_year_grant_granted_`d's_v2.dta", nogen
			merge 1:1 owner year using "$path\out\\`d's\\Combined_granted\owner_year_expiration_granted_`d's_v2.dta", nogen
			
			label var applied "Applied number of patents for those published in 1990s"
			label var piority "Priority number of patents for those published in 1990s"
			label var published "Published number of patents for those published in 1990s"
			label var granted "Granted number of patents for those published in 1990s"
			label var expired "Expired number of patents for those published in 1990s"
			
			foreach v in applied piority published granted expired {
				ren `v' `v'_`d'
			}
			erase "$path\out\\`d's\\Combined_granted\\owner_publication_allDirectOwners_granted_`d's_v2.dta"
			erase "$path\out\\`d's\\Combined\\owner_publication_allDirectOwners_`d's_v2.dta"
			erase "$path\out\\`d's\\Combined_granted\owner_year_application_granted_`d's_v2.dta"
			erase "$path\out\\`d's\\Combined_granted\owner_year_priority_granted_`d's_v2.dta"
			erase "$path\out\\`d's\\Combined_granted\owner_year_publication_granted_`d's_v2.dta"
			erase "$path\out\\`d's\\Combined_granted\owner_year_grant_granted_`d's_v2.dta"
			erase "$path\out\\`d's\\Combined_granted\owner_year_expiration_granted_`d's_v2.dta"
			
			order owner year
			sort owner year 
			compress
			save "$path\out\\`d's\\Combined_granted\owner_year_granted_`d's_v2.dta", replace
		}
		cd "$path\out\\`d's\\Combined_granted\"
		zipfile "owner_year_granted_`d's_v2.dta", saving("owner_year_granted_`d's_v2.zip", replace)
		erase "owner_year_granted_`d's_v2.dta"
	}
}

*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
***All applicants separated
forvalues d=1900(10)2020 {
	capture findfile "applicant(s) bvd id number(s)_`d's_v2.zip", path("$path\out\\`d's\\Combined\") all
	if `"`r(fn)'"'=="" {
		capture findfile "applicant(s) bvd id number(s)_`d's_v2.dta", path("$path\out\\`d's\\Combined\") all
		if `"`r(fn)'"'=="" {
			cd "$path\out\\`d's\\applicant(s) bvd id number(s)\"
			capture findfile "applicant(s) bvd id number(s)_`d's.dta", path("$path\out\\`d's\\applicant(s) bvd id number(s)\") all
			if `"`r(fn)'"'=="" {
				unzipfile "applicant(s) bvd id number(s)_`d's.zip", replace
			}
			use "$path\out\\`d's\\applicant(s) bvd id number(s)\applicant(s) bvd id number(s)_`d's.dta", clear
			erase "$path\out\\`d's\\applicant(s) bvd id number(s)\applicant(s) bvd id number(s)_`d's.dta"
			ren ApplicantsBvDIDNumbers applicant
			split applicant, p(",")
			local nr=r(k_new)
			preserve
			keep applicant1 Publicationnumber 
			drop if applicant1==""
			ren applicant1 applicant
			save "$path\out\\`d's\\Combined\\applicant_publication_tmp1_v2.dta", replace
			restore
			capture drop if applicant2==""
			forvalues n=2(1)`nr'{
				preserve
				keep applicant`n' Publicationnumber 
				drop if applicant`n'==""
				ren applicant`n' applicant
				save "$path\out\\`d's\\Combined\\applicant_publication_tmp`n'_v2.dta", replace
				restore
			}
			use "$path\out\\`d's\\Combined\\applicant_publication_tmp1_v2.dta", clear
			erase "$path\out\\`d's\\Combined\\applicant_publication_tmp1_v2.dta"
			forvalues n=2(1)`nr'{
				append using "$path\out\\`d's\\Combined\\applicant_publication_tmp`n'_v2.dta"
				erase "$path\out\\`d's\\Combined\\applicant_publication_tmp`n'_v2.dta"
			}
			duplicates tag Publicationnumber , gen(dup)
			compress
			save "$path\out\\`d's\\Combined\\applicant(s) bvd id number(s)_`d's_v2.dta", replace
		}
		cd "$path\out\\`d's\\Combined\\"
		zipfile "applicant(s) bvd id number(s)_`d's_v2.dta", saving("applicant(s) bvd id number(s)_`d's_v2.zip", replace)
		erase "applicant(s) bvd id number(s)_`d's_v2.dta"
	}
}

*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
*Appending all decades?
capture findfile "owner_year_granted_1990s_v2.dta", path("$path\out\\1990s\\Combined_granted\") all
if `"`r(fn)'"'=="" {
	cd "$path\out\\1990s\\Combined_granted\"
	unzipfile "owner_year_granted_1990s_v2.zip", replace
}
use "owner_year_granted_1990s_v2.dta", clear
erase "owner_year_granted_1990s_v2.dta"
foreach v in applied piority published granted expired {
	ren `v'_1990 `v' 
}
forvalues d=1910(10)2020 {
	capture findfile "owner_year_granted_`d's_v2.dta", path("$path\out\\`d's\\Combined_granted\") all
	if `"`r(fn)'"'=="" {
		cd "$path\out\\`d's\\Combined_granted\"
		unzipfile "owner_year_granted_`d's_v2.zip", replace
	}
	merge 1:1 owner year using "owner_year_granted_`d's_v2.dta", nogen
	erase "owner_year_granted_`d's_v2.dta"
	foreach v in applied piority published granted expired {
		replace `v'=0 if `v'==.
		replace `v'_`d'=0 if `v'_`d'==.
		replace `v'=`v' + `v'_`d'
		drop `v'_`d'
	}
}
compress
label variable applied "Applied number of patents"
label variable piority "Priority number of patents"
label variable published "Published number of patents"
label variable granted "Granted number of patents"
label variable expired "Expired number of patents"
ren piority priority
sort owner year 
save "$path\out\Combined_granted\owner_year_granted_v2.dta", replace
cd "$path\out\Combined_granted\"
zipfile "owner_year_granted_v2.dta", saving("owner_year_granted_v2.zip", replace)
erase "owner_year_granted_v2.dta"


*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
*Creating a stock number of patents
cd "$path\out\Combined_granted\"
capture findfile "owner_year_granted_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_granted_v2.zip, replace
}
use owner_year_granted_v2.dta, clear
bys owner : egen min_year=min(year )
keep owner min_year
duplicates drop
replace min_year =1900 if min_year <1900
forvalues y=1900(1)2024{
	gen byte y_`y'=.
}
reshape long y_, i(owner min_year ) j(year)
drop y_
compress
sort owner min_year year 
save "$path\out\Combined_granted\owner_year_long_granted_v2.dta"
cd "$path\out\Combined_granted\"
zipfile "owner_year_long_granted_v2.dta", saving("owner_year_long_granted_v2.zip", replace)
erase "owner_year_long_granted_v2.dta"


cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_granted_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_granted_v2.zip, replace
}
use owner_year_long_granted_v2, clear
drop if year <min_year
merge 1:1 owner year using owner_year_granted_v2.dta, keep(1 3) nogen
drop if owner =="v"
compress
sort owner min_year year 
egen id=group(owner)
xtset id year
compress
save "$path\out\Combined_granted\owner_year_long_minyear_granted_v2.dta", replace
cd "$path\out\Combined_granted\"
zipfile "owner_year_long_minyear_granted_v2.dta", saving("owner_year_long_minyear_granted_v2.zip", replace)
erase "owner_year_long_minyear_granted_v2.dta"


**Creating the stock of the vars:
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_minyear_granted_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_minyear_granted_v2.zip, replace
}
use owner_year_long_minyear_granted_v2, clear
ed
foreach v in applied priority published granted expired{
	replace `v'=0 if `v'==.
	gen stock_`v'=0
	replace stock_`v'=`v' if min_year==year
	local n=0
	forvalues y=1901(1)2024{
		local n=`n'+1
		replace stock_`v'=`v'+l.stock_`v' if year==`y' & l.stock_`v'!=.
	}
}
label variable stock_applied "Stock of applied number of patents"
label variable stock_priority "Stock of priority number of patents"
label variable stock_published "Stock of published number of patents"
label variable stock_granted "Stock of granted number of patents"
label variable stock_expired "Stock of expired number of patents"
drop min_year 
xtset id year
order id owner year 
compress
save "$path\out\Combined_granted\owner_year_long_stock_granted_v2.dta", replace
cd "$path\out\Combined_granted\"
zipfile "owner_year_long_stock_granted_v2.dta", saving("owner_year_long_stock_granted_v2.zip", replace)
erase "owner_year_long_stock_granted_v2.dta"


*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
*-------------------------------------------------------------------------------------------------------------------------------------------
/*
**Done in earlier do file "000_compiled_v2.do"
*Adding NACE activities for each firm
*Frist keeping the bvdids of owners:
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_v2.zip, replace
}
use owner_year_long_stock_v2.dta, clear
erase owner_year_long_stock_v2.dta
keep owner
duplicates drop
ren owner bvdid
sort bvdid
compress
save "$path\out\Combined_granted\owner_year_long_stock_v2_bvdid.dta", replace
cd "$path\out\Combined_granted\"
zipfile "owner_year_long_stock_v2_bvdid.dta", saving("owner_year_long_stock_v2_bvdid.zip", replace)
erase "owner_year_long_stock_v2_bvdid.dta"

***separating firms from ODBC that own patents across 75 countries
use "$cc\downloaded_countries_20240117.dta", clear
levelsof ISO2, local(iso2)
foreach i of local iso2 {
	capture findfile "industry_owners_`i'.zip", path("$path\out\Combined_granted\activity\LMS\") all
	if `"`r(fn)'"'=="" {
		capture findfile "industry_owners_`i'.dta", path("$path\out\Combined_granted\activity\LMS\") all
		if `"`r(fn)'"'=="" {
			cd "$odbc\activity\LMS\clean\"
			capture findfile "industry_clean_`i'.dta", path("$path\out\Combined_granted\") all
			if `"`r(fn)'"'=="" {
				unzipfile industry_clean_`i'.zip, replace
			}
			use industry_clean_`i'.dta, clear
			erase industry_clean_`i'.dta
			cd "$path\out\Combined_granted\"
			capture findfile "owner_year_long_stock_v2_bvdid.dta", path("$path\out\Combined_granted\") all
			if `"`r(fn)'"'=="" {
				unzipfile owner_year_long_stock_v2_bvdid.zip, replace
			}
			merge m:1 bvdid using "$path\out\Combined_granted\owner_year_long_stock_v2_bvdid.dta", keep(3) nogen
			erase owner_year_long_stock_v2_bvdid.dta
			sort bvdid ctryiso nace2
			cd "$path\out\Combined_granted\activity\LMS\"
			save industry_owners_`i'.dta, replace
		}
		zipfile industry_owners_`i'.dta, saving(industry_owners_`i'.zip , replace)
		erase industry_owners_`i'.dta
	}
}

**Appending owners activities across 75 countries
use "$cc\downloaded_countries_20240117.dta", clear
levelsof ISO2, local(iso2)
clear
foreach i of local iso2 {
	cd "$path\out\Combined_granted\activity\LMS\"
	capture findfile "industry_owners_`i'.dta", path("$path\out\Combined_granted\activity\LMS\") all
	if `"`r(fn)'"'=="" {
		unzipfile industry_owners_`i'.zip, replace
	}
	append using industry_owners_`i'.dta
	erase industry_owners_`i'.dta
}
cd "$path\out\Combined_granted\activity\"
save industry_owners.dta, replace
zipfile industry_owners.dta, saving(industry_owners.zip , replace)
erase industry_owners.dta

***----------------------------------------------------------------------------------------------
**Merging the patent data with the firms' activities
**First check the missing firms in ODBC
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_v2.zip, replace
}
use owner_year_long_stock_v2.dta, clear
*erase owner_year_long_stock_v2.dta

cd "$path\out\Combined_granted\activity\"
capture findfile "industry_owners.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile industry_owners.zip, replace
}
ren owner bvdid
merge m:1 bvdid using industry_owners.dta, 
*erase industry_owners.dta
keep bvdid
duplicates drop
compress
save "$path\out\Combined_granted\activity\industry_owners_missing.dta", replace
export delimited using "$path\out\Combined_granted\activity\industry_owners_missing.txt", replace

***----------------------------------------------------------------------------------------------
**The missing firms are now downlaoded from Orbis IPR, they should be imported and added to the data above
forvalues i=1(1)4{
	capture findfile "industry_owners_missing`i'.dta", path("$path\out\Combined_granted\activity\Missing Ones DL\\") all
	if `"`r(fn)'"'=="" {
		import excel "$path\out\Combined_granted\activity\Missing Ones DL\\industry_owners_missing`i'.xlsx", sheet("Results") firstrow clear
		replace NACERev2corecode4digits =NACERev2primarycodes if NACERev2corecode4digits =="" & A!=""
		replace NACERev2corecode4digits =NACERev2secondarycodes if NACERev2corecode4digits =="" & A!=""
		keep if A!=""
		ren BvDIDnumber bvdid
		ren CountryISOcode ctryiso
		ren NACERev2corecode4digits nace2
		keep ctryiso bvdid nace2
		duplicates drop
		drop if bvdid==""
		drop if ctryiso =="" & nace2==""
		order bvdid ctryiso nace2
		sort bvdid ctryiso nace2
		compress
		save "$path\out\Combined_granted\activity\Missing Ones DL\\industry_owners_missing`i'.dta", replace
	}
}
clear
forvalues i=1(1)4{
	append using "$path\out\Combined_granted\activity\Missing Ones DL\\industry_owners_missing`i'.dta"
}
order bvdid ctryiso nace2
sort bvdid ctryiso nace2
compress
save "$path\out\Combined_granted\activity\Missing Ones DL\\industry_owners_missing.dta", replace


use "$path\out\Combined_granted\activity\Missing Ones DL\\industry_owners_missing.dta", clear
cd "$path\out\Combined_granted\activity\"
capture findfile "owner_year_long_stock_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile industry_owners.zip, replace
}
gen missin=1
append using "$path\out\Combined_granted\activity\industry_owners.dta"
replace missin =0 if missin ==.
sort bvdid ctryiso nace2
duplicates tag bvdid , gen(dup)
gen nace2d=substr(nace2 ,1,2)
duplicates tag bvdid nace2d , gen(dup2)
drop if dup >0 & dup2 ==0 & missin ==0
duplicates drop bvdid ctryiso nace2d , force
duplicates tag bvdid , gen(dup3)
drop if dup3 ==1 & missin ==0
keep bvdid ctryiso nace2d
duplicates drop
sort bvdid ctryiso nace2
cd "$path\out\Combined_granted\activity\"
save industry_owners_missingDL_aded.dta, replace
zipfile industry_owners_missingDL_aded.dta, saving(industry_owners_missingDL_aded.zip , replace)
erase industry_owners_missingDL_aded.dta
erase "industry_owners.dta
***----------------------------------------------------------------------------------------------
*/
**Merging the patent data with the firms' activities
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2.zip, replace
}
use owner_year_long_stock_granted_v2.dta, clear
erase owner_year_long_stock_granted_v2.dta

cd "$path\out\Combined\activity\"
capture findfile "industry_owners_missingDL_aded.dta", path("$path\out\Combined\") all
if `"`r(fn)'"'=="" {
	unzipfile industry_owners_missingDL_aded.zip, replace
}
ren owner bvdid
merge m:1 bvdid using industry_owners_missingDL_aded.dta, keep(3) nogen
erase industry_owners_missingDL_aded.dta
replace ctryiso =substr(bvdid ,1,2) if ctryiso ==""
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired {
	local l`v' : variable label `v'
}
collapse (rawsum) applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired, by( ctryiso nace2d year)
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired {
	label variable `v' "`l`v'' in country sector"
}
drop if ctryiso ==""
compress
sort ctryiso nace2d year
order ctryiso nace2d year
cd "$path\out\Combined_granted\"
save pantets_industry_country_year_granted.dta, replace
zipfile pantets_industry_country_year_granted.dta, saving(pantets_industry_country_year_granted.zip , replace)
erase pantets_industry_country_year_granted.dta

***----------------------------------------------------------------------------------------------
***Merging the patent data with their global ultimate owners (GUOs)
***----------------------------------------------------------------------------------------------
*/
**Merging the patent data with their global ultimate owners (GUOs)
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2.zip, replace
}
use owner_year_long_stock_granted_v2.dta, clear
erase owner_year_long_stock_granted_v2.dta

***-von hier
cd "$odbc\GUO\"
capture findfile "GUO_bvdid_onlyForeignOwnedBvDIDs.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile GUO_bvdid_onlyForeignOwnedBvDIDs.zip, replace
}
ren owner bvdid
*merge m:1 bvdid using industry_owners_missingDL_aded.dta, keep(3) nogen
merge m:1 bvdid using "$odbc\GUO\\GUO_bvdid_onlyForeignOwnedBvDIDs.dta", keep(1) nogen
keep bvdid
duplicates drop
compress
save "$path\out\Combined_granted\guo\guo_owners_missing.dta", replace
export delimited using "$path\out\Combined_granted\guo\guo_owners_missing.txt", replace
erase "$odbc\GUO\\GUO_bvdid_onlyForeignOwnedBvDIDs.dta"

***I Aked Alex to downbload these owners
**Therefore this is the new download for those that were missing in the earlier data here: C:\wiiw Dropbox\Mahdi Ghodsi\Wien\Data\Orbis\Data\ODBC\ODBC\GUO\GUO_bvdid.dta
import delimited "$path\out\Combined_granted\guo\DL\companies_missing_owners_final_merged.csv", case(preserve) stringcols(7) clear
ren GUODirect direct
gen direct1 =subinstr(direct, ">","", .)
replace direct1 =subinstr(direct1, "<","", .)
replace direct1 =subinstr(direct1, "±","", .)
replace direct1 =subinstr(direct1, "-","", .)
destring direct1 ,replace force
replace direct1 =100 if direct =="BR" | direct =="FC" | direct =="WO" | direct =="VE"
replace direct1 =50.01 if direct =="MO"

ren GUOTotal total
gen total1 =subinstr(total, ">","", .)
replace total1 =subinstr(total1, "<","", .)
replace total1 =subinstr(total1, "±","", .)
replace total1 =subinstr(total1, "-","", .)
destring total1 ,replace force
replace total1 =100 if total =="BR" | total =="FC" | total =="WO" | total =="VE"
replace total1 =50.01 if total =="MO"
keep BvDIDnumber GUOBvDIDnumber GUOCountryISOcode GUONACECorecode direct1 total1
ren direct1 direct
ren total1 total
ren GUOBvDIDnumber guo
ren BvDIDnumber bvdid
ren GUONACECorecode guo_iso3
ren guo_iso3 GUONACECorecode
ren GUONACECorecode guo_nace4
ren GUOCountryISOcode guo_iso3
compress
sort bvdid guo
save "$path\out\Combined_granted\guo\DL\companies_missing_owners_final_merged.dta", replace
cd "$path\out\Combined_granted\guo\DL\"
zipfile companies_missing_owners_final_merged.dta, saving(companies_missing_owners_final_merged.zip , replace)
erase companies_missing_owners_final_merged.dta

**Now merge the older ODBC GUO data with the new download;
cd "$odbc\GUO\"
capture findfile "GUO_bvdid.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile GUO_bvdid.zip, replace
}
use GUO_bvdid.dta, clear
erase "$odbc\GUO\\GUO_bvdid.dta"
drop if bvdid ==""
drop if guo ==""
ren guo_9108 direct
ren guo_9109 total
ren guo_9102 guo_iso3

gen direct1 =subinstr(direct, ">","", .)
replace direct1 =subinstr(direct1, "<","", .)
replace direct1 =subinstr(direct1, "±","", .)
replace direct1 =subinstr(direct1, "-","", .)
destring direct1 ,replace force
replace direct1 =100 if direct =="BR" | direct =="FC" | direct =="WO" | direct =="VE"
replace direct1 =50.01 if direct =="MO"
gen total1 =subinstr(total, ">","", .)
replace total1 =subinstr(total1, "<","", .)
replace total1 =subinstr(total1, "±","", .)
replace total1 =subinstr(total1, "-","", .)
destring total1 ,replace force
replace total1 =100 if total =="BR" | total =="FC" | total =="WO" | total =="VE"
replace total1 =50.01 if total =="MO"
drop direct total size ctryiso 
ren direct1 direct
ren total1 total
compress
sort bvdid guo
collapse (mean) total direct , by( bvdid guo guo_iso3 )
duplicates tag bvdid guo , gen(dup)
drop if dup ==1 & guo_iso3 =="-"
drop dup
compress
sort bvdid guo
save "$odbc\GUO\\GUO_bvdid_clean.dta", replace
cd "$odbc\GUO\"
zipfile GUO_bvdid_clean.dta, saving(GUO_bvdid_clean.zip , replace)
erase GUO_bvdid_clean.dta
**------------------------------------------------------------------------------------
**Now mergin the two GUO data
cd "$odbc\GUO\"
capture findfile "GUO_bvdid_clean.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile GUO_bvdid_clean.zip, replace
}
use GUO_bvdid_clean.dta, clear

erase GUO_bvdid_clean.dta
cd "$path\out\Combined_granted\guo\DL\"
capture findfile "companies_missing_owners_final_merged.dta", path("$path\out\Combined_granted\guo\DL\") all
if `"`r(fn)'"'=="" {
	unzipfile companies_missing_owners_final_merged.zip, replace
}
append using companies_missing_owners_final_merged.dta
erase companies_missing_owners_final_merged.dta
collapse (mean) total direct , by( bvdid guo guo_iso3 )
compress
sort bvdid guo
save "$odbc\GUO\\GUO_bvdid_clean_merged_patentOwnerGUO.dta", replace
cd "$odbc\GUO\"
zipfile GUO_bvdid_clean_merged_patentOwnerGUO.dta, saving(GUO_bvdid_clean_merged_patentOwnerGUO.zip , replace)
erase GUO_bvdid_clean_merged_patentOwnerGUO.dta
***----------------------------------------------------------------------------------------------
***----------------------------------------------------------------------------------------------
***----------------------------------------------------------------------------------------------
***----------------------------------------------------------------------------------------------
***----------------------------------------------------------------------------------------------
**Finally merging the patent data with their global ultimate owners (GUOs) and producing Foreign-owned patents in each country-sector
*Foreign-owned will be divided into total, EU, and Asian GUOs
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2.dta", path("$path\out\Combined_granted\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2.zip, replace
}
use owner_year_long_stock_granted_v2.dta, clear

cd "$odbc\GUO\"
capture findfile "GUO_bvdid_clean_merged_patentOwnerGUO.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile GUO_bvdid_clean_merged_patentOwnerGUO.zip, replace
}
ren owner bvdid
joinby bvdid using "$odbc\GUO\\GUO_bvdid_clean_merged_patentOwnerGUO.dta", unm(master)
drop _merge 
compress
cd "$path\out\Combined\activity\"
capture findfile "industry_owners_missingDL_aded.dta", path("$path\out\Combined\") all
if `"`r(fn)'"'=="" {
	unzipfile industry_owners_missingDL_aded.zip, replace
}
merge m:1 bvdid using "$path\out\Combined\activity\industry_owners_missingDL_aded.dta", keep(3) nogen
erase "$path\out\Combined\activity\industry_owners_missingDL_aded.dta"
replace ctryiso =substr(bvdid ,1,2) if ctryiso ==""
replace guo_iso3 =substr(guo ,1,2) if guo_iso3 ==""
drop if guo_iso3 ==""
keep if guo_iso3 !=ctryiso
replace total =direct if total ==. & direct !=.
compress
cd "$path\out\Combined_granted\"
save owner_year_long_stock_granted_v2_GUO_FO_temp.dta, replace
zipfile owner_year_long_stock_granted_v2_GUO_FO_temp.dta, saving(owner_year_long_stock_granted_v2_GUO_FO_temp.zip , replace)


**Total foreign-owned patents
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2_GUO_FO_temp.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2_GUO_FO_temp.zip, replace
}
use "$path\out\Combined_granted\owner_year_long_stock_granted_v2_GUO_FO_temp.dta", clear
duplicates tag bvdid year , gen(dup)
replace dup =dup +1
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired {
	replace `v'=`v'/dup
	local l`v' : variable label `v'
}
foreach v in stock_applied stock_priority stock_published stock_granted {
	gen `v'_l= `v'- stock_expired
	label var `v'_l "Live `l`v''"
	local l`v'_l : variable label `v'_l
}
collapse (rawsum) applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l, by(ctryiso nace2d year)
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l {
	label variable `v' "Foreign-owned `l`v'' in country sector"
}
drop if ctryiso ==""
drop if nace2d ==""
compress
sort ctryiso nace2d year
order ctryiso nace2d year
cd "$path\out\Combined_granted\"
save owner_year_long_stock_granted_v2_GUO_FO.dta, replace
zipfile owner_year_long_stock_granted_v2_GUO_FO.dta, saving(owner_year_long_stock_granted_v2_GUO_FO.zip , replace)
erase owner_year_long_stock_granted_v2_GUO_FO.dta


**Foreign-owned patents owned by EU GUOs 
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2_GUO_FO_temp.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2_GUO_FO_temp.zip, replace
}
use "$path\out\Combined_granted\owner_year_long_stock_granted_v2_GUO_FO_temp.dta", clear
duplicates tag bvdid year , gen(dup)
replace dup =dup +1
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired {
	replace `v'=`v'/dup
	local l`v' : variable label `v'
}
ren guo_iso3 iso2c
merge m:1 iso2c year using "$eu\EUN_euro_ctry2024_xr.dta" , keep(1 3) nogen
keep if EUN ==1
foreach v in stock_applied stock_priority stock_published stock_granted {
	gen `v'_l= `v'- stock_expired
	label var `v'_l "Live `l`v''"
	local l`v'_l : variable label `v'_l
}
collapse (rawsum) applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l, by(ctryiso nace2d year)
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l {
	label variable `v' "Foreign-owned `l`v'' in country sector"
}
drop if ctryiso ==""
drop if nace2d ==""
compress
sort ctryiso nace2d year
order ctryiso nace2d year
cd "$path\out\Combined_granted\"
save owner_year_long_stock_granted_v2_GUO_FO_EU.dta, replace
zipfile owner_year_long_stock_granted_v2_GUO_FO_EU.dta, saving(owner_year_long_stock_granted_v2_GUO_FO_EU.zip , replace)
erase owner_year_long_stock_granted_v2_GUO_FO_EU.dta


**Foreign-owned patents owned by advanced GUOs 
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2_GUO_FO_temp.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2_GUO_FO_temp.zip, replace
}
use "$path\out\Combined_granted\owner_year_long_stock_granted_v2_GUO_FO_temp.dta", clear
duplicates tag bvdid year , gen(dup)
replace dup =dup +1
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired {
	replace `v'=`v'/dup
	local l`v' : variable label `v'
}
ren guo_iso3 iso2c
merge m:m iso2c using "$cc\isos_country.dta" , keep(1 3) nogen
ren iso3c country
merge m:1 country using "$cc\Country Group\groups.dta", keep(1 3) nogen
keep if imindg=="AIE"
foreach v in stock_applied stock_priority stock_published stock_granted {
	gen `v'_l= `v'- stock_expired
	label var `v'_l "Live `l`v''"
	local l`v'_l : variable label `v'_l
}
collapse (rawsum) applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l, by(ctryiso nace2d year)
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l {
	label variable `v' "Foreign-owned `l`v'' in country sector"
}
drop if ctryiso ==""
drop if nace2d ==""
compress
sort ctryiso nace2d year
order ctryiso nace2d year
cd "$path\out\Combined_granted\"
save owner_year_long_stock_granted_v2_GUO_FO_AIE.dta, replace
zipfile owner_year_long_stock_granted_v2_GUO_FO_AIE.dta, saving(owner_year_long_stock_granted_v2_GUO_FO_AIE.zip , replace)
erase owner_year_long_stock_granted_v2_GUO_FO_AIE.dta


**Foreign-owned patents owned by non-advanced GUOs 
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2_GUO_FO_temp.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2_GUO_FO_temp.zip, replace
}
use "$path\out\Combined_granted\owner_year_long_stock_granted_v2_GUO_FO_temp.dta", clear
duplicates tag bvdid year , gen(dup)
replace dup =dup +1
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired {
	replace `v'=`v'/dup
	local l`v' : variable label `v'
}
ren guo_iso3 iso2c
merge m:m iso2c using "$cc\isos_country.dta" , keep(1 3) nogen
ren iso3c country
merge m:1 country using "$cc\Country Group\groups.dta", keep(1 3) nogen
keep if imindg!="AIE"
foreach v in stock_applied stock_priority stock_published stock_granted {
	gen `v'_l= `v'- stock_expired
	label var `v'_l "Live `l`v''"
	local l`v'_l : variable label `v'_l
}
collapse (rawsum) applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l, by(ctryiso nace2d year)
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l {
	label variable `v' "Foreign-owned `l`v'' in country sector"
}
drop if ctryiso ==""
drop if nace2d ==""
compress
sort ctryiso nace2d year
order ctryiso nace2d year
cd "$path\out\Combined_granted\"
save owner_year_long_stock_granted_v2_GUO_FO_nonAIE.dta, replace
zipfile owner_year_long_stock_granted_v2_GUO_FO_nonAIE.dta, saving(owner_year_long_stock_granted_v2_GUO_FO_nonAIE.zip , replace)
erase owner_year_long_stock_granted_v2_GUO_FO_nonAIE.dta



**Foreign-owned patents owned by Asian GUOs 
cd "$path\out\Combined_granted\"
capture findfile "owner_year_long_stock_granted_v2_GUO_FO_temp.dta", path("$odbc\GUO\") all
if `"`r(fn)'"'=="" {
	unzipfile owner_year_long_stock_granted_v2_GUO_FO_temp.zip, replace
}
use "$path\out\Combined_granted\owner_year_long_stock_granted_v2_GUO_FO_temp.dta", clear
duplicates tag bvdid year , gen(dup)
replace dup =dup +1
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired {
	replace `v'=`v'/dup
	local l`v' : variable label `v'
}
ren guo_iso3 iso2c
merge m:m iso2c using "$cc\Asian\isos_country_Asian.dta" , keep(1 3) nogen
keep if Asia ==1
foreach v in stock_applied stock_priority stock_published stock_granted {
	gen `v'_l= `v'- stock_expired
	label var `v'_l "Live `l`v''"
	local l`v'_l : variable label `v'_l
}
collapse (rawsum) applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l, by(ctryiso nace2d year)
foreach v in applied priority published granted expired stock_applied stock_priority stock_published stock_granted stock_expired stock_applied_l stock_priority_l stock_published_l stock_granted_l {
	label variable `v' "Foreign-owned `l`v'' in country sector"
}
drop if ctryiso ==""
drop if nace2d ==""
compress
sort ctryiso nace2d year
order ctryiso nace2d year
cd "$path\out\Combined_granted\"
save owner_year_long_stock_granted_v2_GUO_FO_Asia.dta, replace
zipfile owner_year_long_stock_granted_v2_GUO_FO_Asia.dta, saving(owner_year_long_stock_granted_v2_GUO_FO_Asia.zip , replace)
erase owner_year_long_stock_granted_v2_GUO_FO_Asia.dta


capture erase "$path\out\Combined_granted\owner_year_long_stock_granted_v2_GUO_FO_temp.dta"
capture erase "$path\out\Combined_granted\owner_year_long_stock_granted_v2.dta"
capture erase "$odbc\GUO\\GUO_bvdid_clean_merged_patentOwnerGUO.dta"