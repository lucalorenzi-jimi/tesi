// EX VISIT.GO
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package codelab

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*Info)(nil)))
	beam.RegisterFunction(CreateInfosFn)
}

// Info represents a Info from a user to the restaurant.
type Info struct {
	
	SNo	int
	Date	time.Time
	Country	string
	RegionCode	int
	RegionName	string
	Latitude	string
	Longitude	string
	HospitalizedPatients	int
	IntensiveCarePatients	int
	TotalHospitalizedPatients	int
	HomeConfinement	int
	CurrentPositiveCases	int
	NewPositiveCases	int
	Recovered	int
	Deaths	int
	TotalPositiveCases int
	TestsPerformed	float


	//InfoorID    string
	//TimeEntered  time.Time
	//MinutesSpent int
	//MoneySpent   int
}

// CreateInfosFn creates and emits a Info struct from a line that holds Info information.
func CreateInfosFn(line string, emit func(Info)) error {
	// Skip the column headers line
	notHeader, err := regexp.MatchString("[0-9]", line)
	if err != nil {
		return err
	}
	if !notHeader {
		return nil
	}

	cols := strings.Split(line, ",")
	if len(cols) != 17 {
		return fmt.Errorf("got %d number of columns in line %q, expected 17", len(cols), line)
	}
	sNo, err := strconv.Atoi(cols[0])
	if err != nil {
		return err
	}
	date, err := time.Parse(time.RFC3339, cols[1] + "Z07:00")
	if err != nil {
		return err
	}
	country := cols[2]
	regionCode, err := strconv.Atoi(cols[3])
	if err != nil {
		return err
	}
	regionName := cols[4]
	latitude := cols[5]
	longitude := cols[6]
	hospitalizedPatients, err := strconv.Atoi(cols[7])
	if err != nil {
		return err
	}
	intensiveCarePatients, err := strconv.Atoi(cols[8])
	if err != nil {
		return err
	}
	totalHospitalizedPatients, err := strconv.Atoi(cols[9])
	if err != nil {
		return err
	}
	homeConfinement, err := strconv.Atoi(cols[10])
	if err != nil {
		return err
	}
	currentPositiveCases, err := strconv.Atoi(cols[11])
	if err != nil {
		return err
	}
	newPositiveCases, err := strconv.Atoi(cols[12])
	if err != nil {
		return err
	}
	recovered, err := strconv.Atoi(cols[13])
	if err != nil {
		return err
	}
	deaths, err := strconv.Atoi(cols[14])
	if err != nil {
		return err
	}
	totalPositiveCases, err := strconv.Atoi(cols[15])
	if err != nil {
		return err
	}
	testsPerformed, err := strconv.ParseFloat(cols[16])
	if err != nil {
		return err
	}
	
	
	emit(Info{
		
		SNo:	sNo
		Date:	date
		Country:	country
		RegionCode:	regionCode
		RegionName:	regionName
		Latitude:	latitude
		Longitude:	longitude
		HospitalizedPatients:	hospitalizedPatients
		IntensiveCarePatients:	intensiveCarePatients
		TotalHospitalizedPatients:	totalHospitalizedPatients
		HomeConfinement:	homeConfinement
		CurrentPositiveCases:	currentPositiveCases
		NewPositiveCases:	newPositiveCases
		Recovered:	recovered
		Deaths:	deaths
		TotalPositiveCases:	totalPositiveCases
		TestsPerformed:	testsPerformed
		
		//InfoorID:    InfoorID,
		//TimeEntered:  timeEntered,
		//MinutesSpent: timeSpent,
		//MoneySpent:   moneySpent,
	})
	return nil
}
