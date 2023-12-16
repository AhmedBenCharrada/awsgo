package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	dy "github.com/AhmedBenCharrada/awsgo/dynamodb"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
)

var conf = dy.DBConfig{
	TableInfo: dy.TableInfo{
		TableName: "User",
		PrimaryKey: dy.DBPrimaryKeyNames{
			PartitionKey: dy.DynamoKeyMetadata{
				Name:    "ID",
				KeyType: dy.String,
			},
		},
	},
}

type User struct {
	ID         string `json:"id"`
	Enabled    bool   `json:"enabled"`
	Name       string `json:"name"`
	FamilyName string `json:"familyName"`
	Age        int    `json:"age"`
}

func (u User) IsEmpty() bool {
	return len(u.ID) == 0 || len(u.Name) == 0
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Using the default configuration.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-west-1"))
	if err != nil {
		log.Fatalf("unable to load config, %v", err)
	}

	// Create the dynamodb client.
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://localhost:8000")
	})

	// Create the dynamodb client wrapper for the User entity.
	svc := dy.NewClient[User](client, conf)

	// Insert 54 user items.
	for i := 0; i < 54; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		_, err := svc.Create(ctx, User{
			ID:         fmt.Sprintf("user-%v", i),
			Enabled:    true,
			Name:       "user",
			FamilyName: "one",
			Age:        i,
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	fCtx, fCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer fCancel()

	// Retrieve a page of max 5 user items where the user's age is greater or equal 18.
	page, err := svc.Find(fCtx, dy.Request{Size: 5, Conditions: []dy.Criteria{
		*dy.NewCriteria().And("Age", 18, dy.GE),
	}})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("last evaluated key", page.LastEvaluatedKey)
	log.Println("Total", len(page.Items))

	out, _ := json.MarshalIndent(page.Items, "", " ")
	log.Println(string(out))
}
