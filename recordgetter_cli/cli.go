package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/brotherlogic/goserver/utils"
	"google.golang.org/grpc"

	pbrg "github.com/brotherlogic/recordgetter/proto"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(&utils.DiscoveryClientResolverBuilder{})
}

func findServer(name string) (string, int) {
	ip, port, _ := utils.Resolve(name, "recordgetter-cli")
	return ip, int(port)
}

func clear() {
	host, port := findServer("recordgetter")
	conn, _ := grpc.Dial(host+":"+strconv.Itoa(port), grpc.WithInsecure())
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)
	r, err := client.Force(context.Background(), &pbrg.Empty{})
	fmt.Printf("%v and %v", r, err)
}

func listened(score int32) {
	host, port := findServer("recordgetter")
	conn, _ := grpc.Dial(host+":"+strconv.Itoa(port), grpc.WithInsecure())
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)
	r, err := client.GetRecord(context.Background(), &pbrg.GetRecordRequest{})
	if err != nil {
		log.Fatalf("%v", err)
	}
	r.GetRecord().GetRelease().Rating = score
	_, err = client.Listened(context.Background(), r.GetRecord())
	fmt.Printf("%v", err)
}

func get(ctx context.Context) {
	conn, err := grpc.Dial("discovery:///recordgetter", grpc.WithInsecure(), grpc.WithBalancerName("my_pick_first"))
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	for i := 0; i < 5; i++ {
		ctx, cancel := utils.ManualContext("RecordGet-Score", "recordgetter", time.Minute, false)
		defer cancel()
		r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Refresh: true})
		if err == nil {
			fmt.Printf("%v and %v", r, err)
			return
		}
		fmt.Printf("%v and %v\n\n", r, err)
	}
}

func score(ctx context.Context, value int32) {
	conn, err := grpc.Dial("discovery:///recordgetter", grpc.WithInsecure(), grpc.WithBalancerName("my_pick_first"))
	if err != nil {
		log.Fatalf("Can't dial getter: %v", err)
	}
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{})
	if err != nil {
		log.Fatalf("Error in scoring: %v", err)
	}
	r.GetRecord().GetMetadata().SetRating = value
	re, err := client.Listened(ctx, r.GetRecord())
	if err != nil {
		fmt.Printf("%v", err)
	}
	fmt.Printf("%v and %v", re, err)
}

func main() {
	ctx, cancel := utils.ManualContext("RecordGet-Score", "recordgetter", time.Minute, false)
	defer cancel()
	if len(os.Args) > 1 {
		val, err := strconv.Atoi(os.Args[1])
		if err != nil {
			log.Fatalf("Error parsing num: %v", err)
		}
		score(ctx, int32(val))
	} else {
		get(ctx)
	}
}
