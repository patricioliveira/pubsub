package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/pubsub"
)

func main() {
	projectid := flag.String("projectid", "", "project id")
	topicName := flag.String("topic", "", "topic name")
	subid := flag.String("subid", "", "subscription id")
	flag.Parse()

	if err := pullMsgs(os.Stdout, *projectid, *subid, *topicName); err != nil {
		fmt.Fprintf(os.Stderr, "pullMsgs: %v", err)
	}
}

func pullMsgs(w io.Writer, projectID, subID, topic string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(subID)
	exist, err := sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("Client.Exists got err: %v", err)
	}

	if !exist {
		sub, err = client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{Topic: client.Topic(topic)})
		if err != nil {
			return fmt.Errorf("Client.CreateSubscription got err: %v", err)
		}
	}

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		fmt.Fprintf(w, "Got message: %q\n", string(msg.Data))
		msg.Ack()
	})
	if err != nil {
		return fmt.Errorf("sub.Receive: %v", err)
	}
	return nil
}
