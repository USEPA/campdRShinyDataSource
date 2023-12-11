## Script for packaging csvs

library(data.table)
library(httr)
library(htmltools)
library(jsonlite)
library(tidyverse)
library(lubridate)
library(zoo)

## store functions

# to get all facility data for downloading off of app
store_facility_data <- function(unitDataBase){

  unitShortData <- unitDataBase
  unitShortData$so2ControlsInstalled <- NA
  unitShortData$noxControlsInstalled <- NA
  unitShortData$particulateMatterControlsInstalled <- NA
  unitShortData$mercuryControlsInstalled <- NA
  unitShortData$primaryFuelInfoOld <- unitShortData$primaryFuelInfo

  for (row in 1:nrow(unitShortData)){

    selectedUnitFac <- unitShortData[row,]

    selectedFacComp <- complianceDataTableForDownload[complianceDataTableForDownload$facilityId==selectedUnitFac$facilityId[1],]

    subjectedPrograms <- unlist(lapply(unique(str_split(unique(selectedUnitFac$programCodeInfo),", ")),function(cell){
      unlist(str_split(cell,","))
    }))

    if ((length(intersect(subjectedPrograms, currentCompliancePrograms$programCode)) == 0) &&
        (is.na(unique(selectedUnitFac$primaryFuelInfo)))){
      unitShortData[row,"primaryFuelInfo"] <- "Not reported"
      unitShortData[row,"so2ControlsInstalled"] <- "Not reported"
      unitShortData[row,"noxControlsInstalled"] <- "Not reported"
      unitShortData[row,"particulateMatterControlsInstalled"] <- "Not reported"
      unitShortData[row,"mercuryControlsInstalled"] <- "Not reported"
    }
    else if (is.na(unique(selectedUnitFac$primaryFuelInfo)) & is.na(selectedFacComp$programCode[1])){
      unitShortData[row,"primaryFuelInfo"] <- "Not reported"
      unitShortData[row,"so2ControlsInstalled"] <- "Not reported"
      unitShortData[row,"noxControlsInstalled"] <- "Not reported"
      unitShortData[row,"particulateMatterControlsInstalled"] <- "Not reported"
      unitShortData[row,"mercuryControlsInstalled"] <- "Not reported"
    }
    else {
      if(!is.na(selectedUnitFac$so2ControlInfo)){
        unitShortData[row,"so2ControlsInstalled"] <- "Yes"
      }
      else{unitShortData[row,"so2ControlsInstalled"] <- "No"}
      if(!is.na(selectedUnitFac$noxControlInfo)){
        unitShortData[row,"noxControlsInstalled"] <- "Yes"
      }
      else{unitShortData[row,"noxControlsInstalled"] <- "No"}
      if(!is.na(selectedUnitFac$pmControlInfo)){
        unitShortData[row,"particulateMatterControlsInstalled"] <- "Yes"
      }
      else{unitShortData[row,"particulateMatterControlsInstalled"] <- "No"}
      if(!is.na(selectedUnitFac$hgControlInfo)){
        unitShortData[row,"mercuryControlsInstalled"] <- "Yes"
      }
      else{unitShortData[row,"mercuryControlsInstalled"] <- "No"}
    }

  }


  facilityTableForDownload <- unitShortData[,c("facilityName","facilityId","stateCode",
                                          "stateName","county",
                                          "latitude","longitude","unitId","operatingStatus",
                                          "primaryFuelInfo","so2ControlsInstalled",
                                          "noxControlsInstalled",
                                          "particulateMatterControlsInstalled",
                                          "mercuryControlsInstalled",
                                          "year","programCodeInfo")]

  facilityTableForDownload
}

# API calls
source("./R/API-calls.R")


### Program year data ###
# Get all programs and storing appropriate emission and compliance years
res = GET(programMdmUrl)
allPrograms <- fromJSON(rawToChar(res$content))

allPrograms$programShorthandDescription <- NA
allPrograms$programShorthandDescription[which(allPrograms$programCode == "ARP")] <- "Acid Rain Program (SO2)"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CAIRNOX")] <- "CAIR NOx Annual Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CAIROS")] <- "CAIR NOx Ozone Season Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CAIRSO2")] <- "CAIR SO2 Annual Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSNOX")] <- "CSAPR NOx Annual Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSNOXOS")] <- "CSAPR NOx Ozone Season Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSOSG1")] <- "CSAPR NOx Ozone Season Program Group 1"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSOSG2")] <- "CSAPR NOx Ozone Season Program Group 2"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSOSG2E")] <- "CSAPR NOx Ozone Season Program Expanded Group 2"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSOSG3")] <- "CSAPR NOx Ozone Season Program Group 3"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSSO2G1")] <- "CSAPR SO2 Annual Program Group 1"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "CSSO2G2")] <- "CSAPR SO2 Annual Program Group 2"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "MATS")] <- "Mercury and Air Toxics Standards"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "NBP")] <- "NOx Budget Trading Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "NHNOX")] <- "New Hampshire NOx Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "NSPS4T")] <- "New Source Performance Standards subpart TTTT"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "OTC")] <- "OTC NOx Budget Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "RGGI")] <- "Regional Greenhouse Gas Initiative"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "SIPNOX")] <- "SIP NOx Program"
allPrograms$programShorthandDescription[which(allPrograms$programCode == "TXSO2")] <- "Texas SO2 Trading Program"


# Get all allowance programs
allAllowancePrograms <- allPrograms#[allPrograms$allowanceUIFilter == TRUE,]

# adding emission and compliance year columns
allAllowancePrograms["emissionYears"] <- NA

# getting compliance years and merging table
applicableCompliance <- get_allow_comp_applicable_data()

applicableProgramCompYears <- unique(applicableCompliance[,c("year","programCode")])

applicableProgramListYears <- as.data.frame(do.call(rbind, unique(applicableProgramCompYears[,c("programCode")]) %>% map(function(prg)
  c(programCode = prg,
       complianceYears = paste(applicableProgramCompYears[applicableProgramCompYears$programCode == prg,c("year")], collapse=',')))
))

allAllowancePrograms <- merge(allAllowancePrograms, applicableProgramListYears, by=c("programCode"), all.x = TRUE)

# emissions data
latestEmissionYear <- get_latest_emission_valid_vear()
# adding ARP years
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode == "ARP")] <- paste(
  append(c(1980,1985,1990),seq(1995,latestEmissionYear)), collapse=',')

# adding CAIR years
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CAIROS","CAIRNOX"))] <- paste(seq(2008, 2014), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CAIRSO2"))] <- paste(seq(2009, 2014), collapse=',')

# adding CSAPR (retired)
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CSNOXOS"))] <- paste(seq(2015, 2016), collapse=',')

# adding NBP and OTC
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("NBP"))] <- paste(seq(2003, 2008), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("OTC"))] <- paste(seq(1999, 2002), collapse=',')

allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CSNOX","CSSO2G1","CSSO2G2"))] <- paste(seq(2015, latestEmissionYear), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CSOSG1","CSOSG2"))] <- paste(seq(2017, latestEmissionYear), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CSOSG3"))] <- paste(seq(2021, latestEmissionYear), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("CSOSG2E"))] <- paste(seq(2023, latestEmissionYear), collapse=',')
allAllowancePrograms$emissionYears[which(allAllowancePrograms$programCode %in% c("TXSO2"))] <- paste(seq(2019, latestEmissionYear), collapse=',')


currentCompliancePrograms <- allAllowancePrograms[(allAllowancePrograms$retiredIndicator==FALSE & allAllowancePrograms$complianceUIFilter == TRUE),]

write.csv(allAllowancePrograms, file = "data/facility-map/programTable.csv", row.names = FALSE)

# reassign dataframe to avoid conflict in loop
compliancePrograms <- allAllowancePrograms

# loops above store years as strings to be able to save to a file. The is reverting back to interger lists.
for (i in 1:nrow(allAllowancePrograms)){
  if (!is.na(allAllowancePrograms$complianceYears[i])){
    allAllowancePrograms$complianceYears[i] <- list(c(as.integer(unlist(strsplit(compliancePrograms$complianceYears[i], ",")))))
  }
  if (!is.na(allAllowancePrograms$emissionYears[i])){
    allAllowancePrograms$emissionYears[i] <- list(c(as.integer(unlist(strsplit(compliancePrograms$emissionYears[i], ",")))))
  }
}

# Get latest compliance year from applicable compliance table
latestYearsCurrentPrograms <- applicableProgramCompYears[applicableProgramCompYears$programCode %in% currentCompliancePrograms$programCode,] %>% aggregate(year ~ programCode, max)

latestComplianceYear <- min(latestYearsCurrentPrograms$year) # get min of max to ensure all compliance data is in for a given year

#### Unit data for latest compliance year ###
unitData <- get_facility_data(latestComplianceYear)

res = GET(statesMdmUrl)
states <- fromJSON(rawToChar(res$content))

unitData <- merge(unitData,states[,c("stateCode","stateName")],all.x=TRUE)

facilitiesForLatestYear <- unique(unitData[,c("facilityId","facilityName")])

### Collect compliance data for all years ###
complianceYears <- unique(na.omit(unlist(allAllowancePrograms$complianceYears)))

# getting all compliance data for all applicable compliance years
allYearComplianceFacilityData <- get_allow_comp_data(complianceYears)

# trim to latest compliance year
complianceFacilityDataLatestYear <- allYearComplianceFacilityData[allYearComplianceFacilityData$year == latestComplianceYear,]

# account info for current facilities
allAccountInfo <- get_account_info_data()

accountInfo <- allAccountInfo[allAccountInfo$facilityId %in% facilitiesForLatestYear$facilityId,]

accountInfoFac <- unique(accountInfo[accountInfo$accountNumber %like% 'FACLTY',c("accountNumber","facilityId")])

# left join the valid facility accounts with facilities in account info
accountInfoFac <- merge(facilitiesForLatestYear,accountInfoFac,by=c("facilityId"),all.x = TRUE)

# Find facility account info (or blank if doesn't exist) of those not in the compliance data
leftOutFacAccts <- anti_join(accountInfoFac,unique(complianceFacilityDataLatestYear[,c("facilityId","facilityName","accountNumber")]), by = c("facilityId", "facilityName", "accountNumber"))

# bind for full set of accounts/facilities
complianceFacilityDataLatestYearFull <- bind_rows(complianceFacilityDataLatestYear,leftOutFacAccts)

# bundle for displaying - latest year
complianceFacilityDataLatestFormat <- bind_rows(lapply(1:nrow(complianceFacilityDataLatestYearFull), function(row){
  if (is.na(complianceFacilityDataLatestYearFull$programCodeInfo[row])){
    compStr <- NA
  }
  else if (is.na(complianceFacilityDataLatestYearFull$excessEmissions[row])){
    compStr <- "Yes"
  }
  else{compStr <- "No"}
  c("facilityName"=complianceFacilityDataLatestYearFull$facilityName[row],
    "facilityId"=complianceFacilityDataLatestYearFull$facilityId[row],
    "accountNumber"=complianceFacilityDataLatestYearFull$accountNumber[row],
    "programCode"=complianceFacilityDataLatestYearFull$programCodeInfo[row],
    "year"=complianceFacilityDataLatestYearFull$year[row],
    "inCompliance?"=compStr)
}))

# bundle for displaying - all years
allYearComplianceFacilityDataFormat <- bind_rows(lapply(1:nrow(allYearComplianceFacilityData), function(row){
  if (!is.na(allYearComplianceFacilityData[row,"excessEmissions"])){
    c("facilityName"=allYearComplianceFacilityData$facilityName[row],
      "facilityId"=allYearComplianceFacilityData$facilityId[row],
      "accountNumber"=allYearComplianceFacilityData$accountNumber[row],
      "programCode"=allYearComplianceFacilityData$programCodeInfo[row],
      "year"=allYearComplianceFacilityData$year[row],
      "inCompliance?"="No")
  }
}))

complianceDataTableForDownload <- rbind(complianceFacilityDataLatestFormat,allYearComplianceFacilityDataFormat)

write.csv(complianceDataTableForDownload, file = "data/facility-map/complianceDataTableForDownload.csv", row.names = FALSE)

### Collect facility data for all years ###
facilityDataTableForDownload <- store_facility_data(unitData)
write.csv(facilityDataTableForDownload, file = "data/facility-map/facilityDataTableForDownload.csv", row.names = FALSE)


