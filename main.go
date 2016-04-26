package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Verbs struct {
	Verbs []string
}

type AggregatedValues struct {
	Values Float32Slice
	Counts map[string]int
	Accum  float32
}

type PercentileValues struct {
	Percentiles map[int]float32
	Count       int
	Average     float32
	Min         float32
	Max         float32
}

var PERCENTILES = [...]int{0, 10, 50, 90, 99, 100}

func main() {
	arg := os.Args[1:]

	f := func(c rune) bool {
		return c == ','
	}

	verbs := Verbs{
		Verbs: strings.FieldsFunc(arg[0], f),
	}

	log.Printf("%s, looking for verbs:%v", arg[1], verbs.Verbs)
	values := filterValues(arg[1], verbs)
	percentiles := computePercentiles(values, PERCENTILES[:])

	printPercentiles(percentiles)
}

func filterValues(filename string, verbs Verbs) AggregatedValues {

	f, _ := os.Open(filename)
	defer f.Close()
	scanner := bufio.NewScanner(f)

	values := AggregatedValues{
		Values: make([]float32, 0),
		Counts: make(map[string]int),
	}
	for scanner.Scan() {
		line := scanner.Text()
		for _, verb := range verbs.Verbs {
			if strings.Contains(line, verb) {
				processLine(line, verb, &values)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("error reading file: %s, err:%v", filename, err)
	}

	return values
}

// extract a float from the last field in this line
func processLine(line string, verb string, values *AggregatedValues) {
	// TODO: allow for regexp to find the float
	lastSpace := strings.LastIndexByte(line, ' ')
	floatStr := line[lastSpace+1:]
	f, err := strconv.ParseFloat(floatStr, 32)
	if err != nil {
		log.Printf("no float:%s, err: %v", floatStr, err)
		return
	}
	val := float32(f)
	values.Values = append(values.Values, val)
	values.Accum += val
	_, ok := values.Counts[verb]
	if !ok {
		values.Counts[verb] = 1
	} else {
		values.Counts[verb]++
	}
}

func computePercentiles(values AggregatedValues, percentiles []int) PercentileValues {

	f := func(sortedValues []float32, percentile int) float32 {
		count := len(sortedValues)
		if count == 0 {
			return -1
		} else if count == 1 {
			return sortedValues[0]
		}
		if percentile >= 100 {
			return sortedValues[count-1]
		}

		pos := (percentile * count) / 100
		return sortedValues[pos]
	}

	values.Values.Sort()
	count := len(values.Values)
	result := PercentileValues{
		Percentiles: make(map[int]float32, len(percentiles)),
		Average:     values.Accum / float32(count),
		Min:         values.Values[0],
		Max:         values.Values[count-1],
		Count:       count,
	}

	for _, percent := range percentiles {
		result.Percentiles[percent] = f(values.Values, percent)
	}

	return result
}

func printPercentiles(values PercentileValues) {

	keys := make([]int, 0, len(values.Percentiles))
	for k := range values.Percentiles {
		keys = append(keys, k)
	}

	sort.Ints(keys)
	summary := fmt.Sprintf("count: %d,    min: %.3f,    avg: %.3f,    max: %.3f\n",
		values.Count, values.Min, values.Average, values.Max)
	for _, k := range keys {
		summary += fmt.Sprintf("P%d%%: %.3f,    ", k, values.Percentiles[k])
	}
	log.Print(summary)
}

// Float32Slice attaches the methods of sort.Interface to []float32, sorting in increasing order.
type Float32Slice []float32

func (s Float32Slice) Len() int           { return len(s) }
func (s Float32Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Float32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Sort is a convenience method.
func (s Float32Slice) Sort() {
	sort.Sort(s)
}
