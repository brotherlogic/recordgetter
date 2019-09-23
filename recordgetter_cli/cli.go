package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/brotherlogic/goserver/utils"
	"google.golang.org/grpc"

	pbrg "github.com/brotherlogic/recordgetter/proto"

	//Needed to pull in gzip encoding init
	_ "google.golang.org/grpc/encoding/gzip"
)

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
	host, port := findServer("recordgetter")
	conn, _ := grpc.Dial(host+":"+strconv.Itoa(port), grpc.WithInsecure())
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)

	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{Refresh: true})
	fmt.Printf("%v and %v", r, err)
}

func score(ctx context.Context, value int32) {
	host, port := findServer("recordgetter")
	conn, _ := grpc.Dial(host+":"+strconv.Itoa(port), grpc.WithInsecure())
	defer conn.Close()
	client := pbrg.NewRecordGetterClient(conn)
	r, err := client.GetRecord(ctx, &pbrg.GetRecordRequest{})
	if err != nil {
		log.Fatalf("Error in scoring: %v", err)
	}
	r.GetRecord().GetMetadata().SetRating = value
	_, err = client.Listened(ctx, r.GetRecord())
	if err != nil {
		fmt.Printf("%v", err)
	}
}

func main() {
	ctx, cancel := utils.BuildContext("RecordGet-Score", "recordgetter")
	defer cancel()
	val, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Error parsing num: %v", err)
	}
	score(ctx, int32(val))
}
