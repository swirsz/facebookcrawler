/*
	BATcave
	nlp.go

	Author: Steven Wirsz
	Last Updated: 03/12/2013

    http://golang.org/
    http://www.mongodb.org/
    
	Updates sentiment field for each mention
	
	Requires: SentiWordNet_3.0.0.txt

	main()
		- loads sentiword database into memory
		- load all brand names
		- traverse mention list for each brand
		- evaluate each word, create evalation for entire mention
		- write "positive" "neutral" or "negative" back to the database
	
	loadBrands()
		- loads all brands from the database
		- returns a brand list
	  
	traverseMentions(Brand)
		- Retrieve mention list for exactly one brand
		- Traverse mention list 
		- If mention.sentiment=0 then call ParseMention() with mention text

	parseMention()
		- tokenize all text into an array of words
		- evaluate each word 
		- Determine the value of sediment field for the entire raw text
		- write mention back to database
	
	evalWord()
		- returns positive and negative values for each word
		
	readSenti()
		- preloads the sentiment database into memory
		
	getWord()
		- convert a long string into an array of words
	
	database()
		- connect and authenticate with the database
*/

package main

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"strconv"
	"strings"
	"time"
	"encoding/csv"
	"io"
	"os"
	"math"
)

type Mention struct {
	Id        string `bson:"_id"`
	Name      string `bson:"name"`
	Timestamp uint64 `bson:"timestamp"`
	Source    string `bson:"source"`
	Text	  string `bson:"text"`
	SourceLink string `bson:"sourcelink"`
	Sentiment int  `bson:"sentiment"`// -2 to 2 is neutral
}

type SentiWord struct { // Sentiment database is parsed and loaded into this structure
	Pos		float64
	Neg		float64
	Text	string
}

var Senti = make([]SentiWord, 60000, 60000) // only about 52,000 words actually have sentiment values

func main() {
	readSenti() // preload SentiWordNet 3.0
	for {
		traverseMention()
		//time.Sleep(1 * time.Second) // debugging use only - time to read evaluation
	}
}

func traverseMention()  {
	// retrieve one mention, evaluate, and write it back to the database
	session, err := database() // establish session with database
	if err != nil {
		panic(err)
	}
	defer session.Close()
	d := session.DB("batcave2").C("mention")
	
	var mention Mention

	err = d.Find(bson.M{"Sentiment": nil}).One(&mention)
	if err != nil {
		fmt.Println("Find error: Mentions without sentiment",err)
		time.Sleep(10 * time.Second) // wait for new mentions to show up in database
		return
	}
	
	id := bson.ObjectId(mention.Id)
	
	fmt.Println("Mention: ",mention) // debugging
	mention.Sentiment = parseMention(mention.Text)
	fmt.Println("Sentiment: ",mention.Sentiment) // debugging
	
	err = d.UpdateId(id, bson.M{"$addToSet": bson.M{"Sentiment":mention.Sentiment}})

	if err != nil {
		fmt.Println("Update: ",err)
	}
}

func parseMention(s string) int {
// tokenize all text into an array of words and evaluate each word
	var diff, mean, length float64
	var p, n []float64
	
	if s=="" {
		return 0
	}
	
	//fmt.Println("s: ",s) // debugging - shows the initial unparsed string
	word := getWord(s) // convert string into array of words
	//fmt.Println(word) // debugging, display the array of words for this mention
	
	for i:= range word {
		pos, neg := evalWord(word[i])
		mean += pos - neg
		if pos != 0.0 || neg != 0.0 { // only evaluate non-objective words
			p=append(p,pos)
			n=append(n,neg)
		}
		length++
	}
	
	mean = mean/length
	//fmt.Println("additive sentiment: ",mean)  // debugging
	
	for j:= range p {	// add the squares of all differences from the mean
		diff = diff+math.Pow(mean-(p[j]-n[j]),2)
	}
	sentiment := math.Sqrt(diff/length) // variance from standard deviation
	sentiment = sentiment * 16 // tweak for significant values
	if mean < 0 {
		sentiment = -sentiment // represent negative variances
	}
	
	fmt.Println("Sentiment: ",sentiment)  // debugging
	fmt.Println("__________________")  // debugging
	
	return int(sentiment)
}

func evalWord(s string) (float64, float64) {
	// returns positive and negative values for each word
	
	for i := range Senti {
		if s==Senti[i].Text && len(s)==len(Senti[i].Text)  {
			fmt.Println(Senti[i].Pos, Senti[i].Neg, s) // debugging
			return Senti[i].Pos, Senti[i].Neg
		}
	}
	
	//fmt.Println("Cannot find: ",s)  // debugging
	return 0.0, 0.0
}

func readSenti() { // this function preloads the sentiment database into memory
	wordCount := 0 // number of unique words added
	pos := 0.0 // temporary variable for positive sentiment
	neg := 0.0 // temporary variable for negative sentiment
	
	file, err := os.Open("SentiWordNet_3.0.0.txt")
    if err != nil {
        fmt.Println("Error:", err)
        return
    }
    defer file.Close()
    reader := csv.NewReader(file)
    for { // full database contains 117,000 lines
        record, err := reader.Read() // read exactly one line into this string
        if err == io.EOF {
            break
        } else if err != nil {
            fmt.Println("Error:", err)
            return
        }
        // fmt.Println(i,record) // debugging - record has the type []string
		 parseLine := getWord(record[0])
		 for j:= range parseLine {
			if j==0 { // extract positive value
				pos, err = strconv.ParseFloat(parseLine[j], 3)
				if err != nil {
					break
				}
				//fmt.Println("pos: ",pos) // debugging
			} else if j==1 { // extract negative value
				neg, err = strconv.ParseFloat(parseLine[j], 3)
				if err != nil {
					break
				}
				//fmt.Println("neg: ",neg) // debugging
				if pos==0.0 && neg==0.0 { // discard any 0-value sentiment word
					break
				}
			} else {
				wordCount++			
				//fmt.Println(i,wordCount,parseLine[j]) // debugging
				Senti[wordCount].Pos=pos
				Senti[wordCount].Neg=neg
				Senti[wordCount].Text=parseLine[j][:len(parseLine[j])-2] // trim off #
//				fmt.Println(wordCount, Senti[wordCount]) // debugging - show entire database when loading
			}
		}
    }
}

func getWord(fulltext string) []string {
// convert a long string into an array of words
	word := []string{}
	nextword := 0
	fulltext=fulltext+" "
	
	if strings.Contains(fulltext, " ") {
		for i := range fulltext {
			if fulltext[i] == ' ' || fulltext[i] == '\t' {
				//fmt.Println(fulltext[nextword:i]) // debugging
				word = append(word, fulltext[nextword:i])
				nextword=i+1 // start the next word skipping over the space
			}
		}
	}
	return word
}

func database() (*mgo.Session, error) {
//database: establish connection with the MongoDB database (from Arbiter)
	//session, err := mgo.Dial("localhost:27017")
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
