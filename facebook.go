/* 

BATcave Facebook Crawler 

Extracts nonuser specific feed data from Facebook's Graph API and stores results in a MongoDB database. Does not require an OAuth access token.

Steven Wirsz

5/1/2013

*/

package main

import (
	"container/heap"
	"errors"
	"fmt"
	"io/ioutil"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	BASEURL string = "https://graph.facebook.com/"
)

var tempTimestamp uint64

type Brand struct {
	Name           string // name of brand - key
	Aliases        []string
	searchedbefore int   // unix timestamp of latest search
	time           uint64 // priority, last processed time by the crawler
	pass           int   // how many search passes
	index          int   // used by heap
}
type Brands []*Brand
var brand Brands

type Item struct {
	mention string // text of mention - discarded when copied to the database
	id      int64  // Facebook's ID field - discarded when copied to the database
	time    uint64 // Facebook's mention created time
	index   int    // used by heap - discarded when copied to the database
}
type PriorityQueue []*Item

type Mention struct {
	Name      string
	Timestamp uint64
	Source    string
	Text	  string
	SourceLink string
}

func main() {
	pq := make(PriorityQueue, 0, 99999)

	for {
		brand = retrieveBrands() // Retrieve brands from database
		crawler(&pq, &brand) // pop lowest brand name from the database, crawl, push
		time.Sleep(5 * time.Second)
	}
	// end of the infinite loop - the program should never get here
}

func retrieveBrands() Brands {
	session, err := database() // establish session with database
	brandList := []Brand{}
	c := session.DB("batcave2").C("brand")
	defer session.Close()

	err = c.Find(bson.M{}).All(&brandList)
	if err != nil {
		panic(err)
	}

	if len(brand) == len(brandList) { // if nothing is changed, don't modify the value of brand
		return brand
	}

	br := make(Brands, 0, len(brandList))
	for i := 0; i < len(brandList); i++ {
		item := &Brand{
			Name: brandList[i].Name,
			pass: 0,
		}
		heap.Push(&br, item)
	}

	return br
}

func database() (*mgo.Session, error) {
	//session, err := mgo.Dial("localhost")
	session, err := mgo.Dial("localhost:20173")
	if err != nil {
		return nil, err
	}

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)

	err = session.DB("admin").Login("admin", "sneakers")
	if err != nil {
		return nil, err
	}
	return session, nil
}

func crawler(pq *PriorityQueue, brand *Brands) {
	for i := 0; i < len(*brand); i++ {
		currentbrand := new(Brand)
		currentbrand = heap.Pop(brand).(*Brand)
		
		tempTimestamp = currentbrand.time // used to delay update of last timestamp until one full pass of the crawler has finished

		// Crawl facebook - retrieve first search result page
		resp, err1 := http.Get(BASEURL + "/search?fields=message&q=" + fixspace(currentbrand.Name) + "&type=post&limit=100")
		panicIf(err1)
		defer resp.Body.Close()

		currentbrand.pass += 1           // Keep track of number of crawling passes	
		crawl(resp, 0, pq, currentbrand) // loops for each page

		if currentbrand.time < tempTimestamp {
			currentbrand.time = tempTimestamp // update last timestamp with the latest value
		}

		// update brand name with current time, push back into the stack and repeat
		currentbrand.time = uint64(time.Now().Unix())
		heap.Push(brand, currentbrand)
	}
}

func crawl(resp *http.Response, page int, pq *PriorityQueue, cb *Brand) {
	// process one page of search results

	page++ // how many next pages have been transversed
	fmt.Printf("______ %s pass %d page %d_________\n", cb.Name, cb.pass, page)
	mentionList := make([]string, 0) // (initial data)

	body, err2 := ioutil.ReadAll(resp.Body) // process until EOF reached
	panicIf(err2)
	bodyString := string(body)

	nextLink := getnext(bodyString) // retrieve "next" hyperlink
	//println(nextLink)
	if page > 4 || nextLink == "" { // if no more data, quit
		return
	}

	str := getuntil(nextLink)       // retrieve unix search time of hyperlink
	depth, err := strconv.Atoi(str) // convert the string into an integer
	if err != nil {
	}

	if cb.pass > 1 && cb.searchedbefore >= depth {
		return // quit because this next page has been searched this deep before
	}
	cb.searchedbefore = maxint(cb.searchedbefore, depth)
	//fmt.Printf("\nstr: %d %d\n\n", cb.searchedbefore, depth) // debugging

	// else process data

	getdata(bodyString, &mentionList) // retrieve data fields three at a time
	convertdata(mentionList, pq, cb) // convert list to struct & write mentions

	resp, err1 := http.Get(nextLink) // retrieve next search result page
	panicIf(err1)
	crawl(resp, page, pq, cb) // jump to next level
}

func maxint(x int, y int) int {
	// returns the larger integer value
	if x > y {
		return x
	}
	return y
}

func getnext(webpage string) string {
	// parses the webpage string and returns only the "next" hyperlink
	target1 := strings.Index(webpage, "\"next\"") // search for "next"
	if target1 == -1 {
		return ""
	}

	target1 += len("next :   https://graph.facebook.com/  ")
	target2 := strings.Index(webpage[target1:], "\"")
	target2 = target1 + target2 // to repeat same page

	return BASEURL + webpage[target1:target2]
}

func getuntil(next string) string {
	// parses the next hyperlink and only returns the UNIX timestamp value
	target1 := strings.Index(next, "until=") // search for "until str"
	if target1 == -1 {
		return ""
	}
	target1 += len("until=")
	return next[target1:]
}

func convertdata(mentionList []string, pq *PriorityQueue, cb *Brand) {
	// converts the long string data into struct with mention / ID / time 
	// Also converts the ID string to an integer
	// converts the string time to a UNIX time integer value

	session, err := database() // establish session with database
	if err != nil {
		panic(err)
	}
	m := session.DB("batcave2").C("mention")

	for i := 0; i < len(mentionList)-2; i++ { // convert list to struct

		temp := new(Item)
		temp.mention = mentionList[i] // copies the string message into string mention

		i++ // convert the id string into a 64-bit integer
		linkint, err := strconv.ParseInt(mentionList[i], 10, 64)
		if err != nil {
			fmt.Printf("\nid convert failure: %s\n\n", mentionList[i])
		}
		temp.id = linkint

		i++
		temptime, err := time.Parse("2006-01-02T15:04:05+0000", mentionList[i])
		if err != nil {
			fmt.Printf("\ntime convert failure: %s\n\n", mentionList[i])
		}
		temp.time = (uint64(temptime.Unix())) // convert time to uint unix time

		// duplicate checking, if the timestamp isn't newer, abort
		if cb.time >= temp.time {
			return
		// if timestamp of current field is the newest, update temporary timestamp but do not prematurely stop scanning
		} else if temp.time > tempTimestamp { 
			tempTimestamp = temp.time
		}
		
		heap.Push(pq, temp)

		mention := new(Mention)
		mention.Name = cb.Name
		mention.Timestamp = temp.time
		mention.Source = "facebook"
		mention.Text = temp.mention
		mention.SourceLink = "n/a"
		
		fmt.Printf("Mention: %v\n",mention)
		err = m.Insert(&mention)
		if err != nil {
			panic(err)
		}
	}
}

func getdata(webpage string, mentionList *[]string) {
	// capture every field in batches of three.
	// grab message, ID, created time

	gotLink, i, err := getmessage(webpage)
	*mentionList = append(*mentionList, gotLink)
	if err != nil { // if no more data, then return
		return
	}

	gotLink, i, err = getid(webpage)
	*mentionList = append(*mentionList, gotLink)
	if err != nil { // if no more data, then return
		return
	}

	gotLink, i, err = gettime(webpage)
	*mentionList = append(*mentionList, gotLink)
	if err != nil { // if no more data, then return
		return
	}

	getdata(webpage[i:], mentionList) // Loop this function forever until no more data
}

func getmessage(webpage string) (string, int, error) {
	// locates and parses the message value
	target1 := strings.Index(webpage, "\"message\"") // find "message"
	if target1 == -1 {
		return "", -1, errors.New("No more links.")
	}

	target1 += len("message") + 4                     // skip over "message": " before capturing text
	target2 := strings.Index(webpage[target1:], "\"") // length of message to end with next "
	target2 = target1 + target2                       // position of "

	link := webpage[target1:target2]

	return link, target2, nil
}

func getid(webpage string) (string, int, error) {
	// locates and parses the ID value

	target1 := strings.Index(webpage, "\"id\"")
	if target1 == -1 {
		return "", -1, errors.New("No more links.")
	}

	target1 += len("id") + 4
	target2 := strings.Index(webpage[target1:], "\"")
	target2 = target1 + target2

	link := webpage[target1:target2]
	target3 := strings.Index(link, "_") // remove underscore	
	// link2 := link[1:target3] + link[target3+1:] // combo of both
	link2 := link[target3+1:] // only keep 2nd half of ID string

	return link2, target2, nil
}

func gettime(webpage string) (string, int, error) {
	// locates and parses the created time value
	target1 := strings.Index(webpage, "created_time")
	if target1 == -1 {
		return "", -1, errors.New("No more links.")
	}

	target1 += len("created_time") + 3
	target2 := strings.Index(webpage[target1:], "\"")
	target2 = target1 + target2

	link := webpage[target1:target2]

	return link, target2, nil
}

func fixspace(name string) string {
	// if any white space is found in the name, substitutes it for %20
	target1 := strings.Index(name, " ") // search for a space in the name
	if target1 == -1 {
		return name
	}

	fixspace(name[:target1] + "%20" + name[target1+1:])
	return name
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

////////////////////////////////////////////////////////
// Priority Queue
////////////////////////////////////////////////////////

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	// To simplify indexing expressions in these methods, we save a copy of the
	// slice object. We could instead write (*pq)[i]
	a := *pq
	n := len(a)
	a = a[0 : n+1]
	item := x.(*Item)
	item.index = n
	a[n] = item
	*pq = a
}

func (pq *PriorityQueue) Pop() interface{} {
	a := *pq
	n := len(a)
	item := a[n-1]
	item.index = -1 // for safety
	*pq = a[0 : n-1]
	return item
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].time < pq[j].time // sort descending by timestamp
}

/////////////////////////////////////////////////////////////
func (br Brands) Len() int { return len(br) }

func (br Brands) Swap(i, j int) {
	br[i], br[j] = br[j], br[i]
	br[i].index = i
	br[j].index = j
}

func (br *Brands) Push(x interface{}) {
	a := *br
	n := len(a)
	a = a[0 : n+1]
	Brand := x.(*Brand)
	Brand.index = n
	a[n] = Brand
	*br = a
}

func (br *Brands) Pop() interface{} {
	a := *br
	n := len(a)
	Brand := a[n-1]
	Brand.index = -1 // for safety
	*br = a[0 : n-1]
	return Brand
}

func (br Brands) Less(i, j int) bool {
	return br[i].time < br[j].time // sort ascending by timestamp
}
