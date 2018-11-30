package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"time"

	pbcdp "github.com/brotherlogic/cdprocessor/proto"
	pb "github.com/brotherlogic/discogssyncer/server"
	pbd "github.com/brotherlogic/godiscogs"
	"github.com/brotherlogic/goserver"
	pbg "github.com/brotherlogic/goserver/proto"
	"github.com/brotherlogic/goserver/utils"
	"github.com/brotherlogic/keystore/client"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbrg "github.com/brotherlogic/recordgetter/proto"
	pbt "github.com/brotherlogic/tracer/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type cdproc interface {
	isRipped(ID int32) bool
}
type cdprocProd struct{}

func (p *cdprocProd) isRipped(ID int32) bool {
	ip, port, err := utils.Resolve("cdprocessor")
	if err != nil {
		return false
	}

	conn, err := grpc.Dial(ip+":"+strconv.Itoa(int(port)), grpc.WithInsecure())
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pbcdp.NewCDProcessorClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	res, err := client.GetRipped(ctx, &pbcdp.GetRippedRequest{})
	if err != nil {
		return false
	}

	for _, r := range res.GetRipped() {
		if r.Id == ID {
			return true
		}
	}

	return false
}

//Server main server type
type Server struct {
	*goserver.GoServer
	serving    bool
	delivering bool
	state      *pbrg.State
	updater    updater
	rGetter    getter
	cdproc     cdproc
}

const (
	wait = 5 * time.Second

	//KEY under which we store the collection
	KEY = "/github.com/brotherlogic/recordgetter/state"
)

type getter interface {
	getRecords(ctx context.Context, folderID int32) (*pbrc.GetRecordsResponse, error)
	getRelease(ctx context.Context, instanceID int32) (*pbrc.GetRecordsResponse, error)
}

type prodGetter struct{}

func (p *prodGetter) getRecords(ctx context.Context, folderID int32) (*pbrc.GetRecordsResponse, error) {
	host, port, _ := utils.Resolve("recordcollection")
	conn, err := grpc.Dial(host+":"+strconv.Itoa(int(port)), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)

	//Only get clean records
	r, err := client.GetRecords(ctx, &pbrc.GetRecordsRequest{Filter: &pbrc.Record{Release: &pbd.Release{FolderId: folderID}}}, grpc.MaxCallRecvMsgSize(1024*1024*1024))
	return r, err
}

func (p *prodGetter) getRelease(ctx context.Context, instance int32) (*pbrc.GetRecordsResponse, error) {
	host, port, _ := utils.Resolve("recordcollection")
	conn, err := grpc.Dial(host+":"+strconv.Itoa(int(port)), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)

	return client.GetRecords(ctx, &pbrc.GetRecordsRequest{Filter: &pbrc.Record{Release: &pbd.Release{InstanceId: instance}}})
}

func (s *Server) saveRelease(ctx context.Context, in *pbd.Release) (*pb.Empty, error) {
	host, port := s.GetIP("discogssyncer")
	conn, err := grpc.Dial(host+":"+strconv.Itoa(port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pb.NewDiscogsServiceClient(conn)

	return client.UpdateRating(ctx, in)
}

func (s *Server) moveReleaseToListeningBox(ctx context.Context, in *pbd.Release) (*pb.Empty, error) {
	host, port := s.GetIP("discogssyncer")
	conn, err := grpc.Dial(host+":"+strconv.Itoa(port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pb.NewDiscogsServiceClient(conn)
	return client.MoveToFolder(ctx, &pb.ReleaseMove{Release: in, NewFolderId: 673768})
}

type updater interface {
	update(ctx context.Context, rec *pbrc.Record) error
}

type prodUpdater struct{}

func (p *prodUpdater) update(ctx context.Context, rec *pbrc.Record) error {
	host, port, _ := utils.Resolve("recordcollection")
	conn, err := grpc.Dial(host+":"+strconv.Itoa(int(port)), grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)
	_, err = client.UpdateRecord(ctx, &pbrc.UpdateRecordRequest{Update: rec})
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) getReleaseFromPile(ctx context.Context, t time.Time) (*pbrc.Record, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	r, err := s.rGetter.getRecords(ctx, 812802)
	if err != nil {
		return nil, err
	}

	ctx = s.LogTrace(ctx, "getReleaseFromPile", time.Now(), pbt.Milestone_MARKER)

	if len(r.GetRecords()) == 0 {
		return nil, fmt.Errorf("No records found")
	}

	var newRec *pbrc.Record
	newRec = nil

	//Look for a record staged to sell
	for _, rc := range r.GetRecords() {
		if rc.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_STAGED_TO_SELL && rc.GetMetadata().SetRating == 0 && rc.GetRelease().Rating == 0 {
			if !s.needsRip(rc) {
				newRec = rc
				break
			}
		}
	}

	ctx = s.LogTrace(ctx, "PostStage", time.Now(), pbt.Milestone_MARKER)

	// If the time is between 1800 and 1900 - only reveal PRE_FRESHMAN records
	if t.Hour() >= 18 && t.Hour() <= 19 {
		pDate := int64(0)
		for _, rc := range r.GetRecords() {
			if rc.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_PRE_FRESHMAN {
				if (pDate == 0 || rc.GetMetadata().DateAdded < pDate) && rc.GetRelease().Rating == 0 && !rc.GetMetadata().GetDirty() {
					// Check on the data
					dateFine := true
					for _, score := range s.state.Scores {
						if score.InstanceId == rc.GetRelease().InstanceId {
							if t.AddDate(0, 0, -7).Unix() <= score.ScoreDate {
								dateFine = false
							}
						}
					}

					if dateFine && !s.needsRip(rc) {
						pDate = rc.GetMetadata().DateAdded
						newRec = rc
					}
				}
			}
		}
	}

	//Look for the oldest new rec
	if newRec == nil {
		pDate := int64(0)
		for _, rc := range r.GetRecords() {
			if rc.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_UNLISTENED {
				if (pDate == 0 || rc.GetMetadata().DateAdded < pDate) && rc.GetRelease().Rating == 0 && !rc.GetMetadata().GetDirty() {
					// Check on the data
					dateFine := true
					for _, score := range s.state.Scores {
						if score.InstanceId == rc.GetRelease().InstanceId {
							if t.AddDate(0, 0, -7).Unix() <= score.ScoreDate {
								dateFine = false
							}
						}
					}

					if dateFine && !s.needsRip(rc) {
						pDate = rc.GetMetadata().DateAdded
						newRec = rc
					}
				}
			}
		}
	}

	ctx = s.LogTrace(ctx, "PostOldestNew", time.Now(), pbt.Milestone_MARKER)

	//Look for the oldest new rec
	if newRec == nil {
		pDate := int64(0)
		for _, rc := range r.GetRecords() {
			if rc.GetMetadata().GetCategory() == pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL {
				if (pDate == 0 || rc.GetMetadata().DateAdded < pDate) && rc.GetRelease().Rating == 0 && !rc.GetMetadata().GetDirty() {
					// Check on the data
					dateFine := true
					for _, score := range s.state.Scores {
						if score.InstanceId == rc.GetRelease().InstanceId {
							if t.AddDate(0, 0, -7).Unix() <= score.ScoreDate {
								dateFine = false
							}
						}
					}

					if dateFine && !s.needsRip(rc) {
						pDate = rc.GetMetadata().DateAdded
						newRec = rc
					}
				}
			}
		}
	}

	ctx = s.LogTrace(ctx, "PostOldestNew2", time.Now(), pbt.Milestone_MARKER)

	if newRec == nil {
		//Get the youngest record in the to listen to that isn't pre-freshman
		pDate := int64(0)
		for _, rc := range r.GetRecords() {
			if rc.GetMetadata().DateAdded > pDate && rc.GetRelease().Rating == 0 && !rc.GetMetadata().GetDirty() {
				if rc.GetMetadata().GetCategory() != pbrc.ReleaseMetadata_PRE_FRESHMAN {
					// Check on the data
					dateFine := true
					for _, score := range s.state.Scores {
						if score.InstanceId == rc.GetRelease().InstanceId {
							if t.AddDate(0, 0, -7).Unix() <= score.ScoreDate {
								dateFine = false
							}
						}
					}

					if dateFine && !s.needsRip(rc) {
						pDate = rc.GetMetadata().DateAdded
						newRec = rc
					}
				}
			}
		}
	}
	ctx = s.LogTrace(ctx, "Youngest", time.Now(), pbt.Milestone_MARKER)

	if newRec == nil {
		recs, err := s.rGetter.getRecords(ctx, 242017)
		if err == nil {
			for _, r := range recs.GetRecords() {
				if r.GetRelease().Rating == 0 {
					newRec = r
					break
				}
			}
		}
	}

	return newRec, nil
}

//Init a record getter
func Init() *Server {
	s := &Server{GoServer: &goserver.GoServer{}, serving: true, delivering: true, state: &pbrg.State{}}
	s.updater = &prodUpdater{}
	s.rGetter = &prodGetter{}
	s.cdproc = &cdprocProd{}
	s.Register = s
	s.PrepServer()
	return s
}

// DoRegister does RPC registration
func (s *Server) DoRegister(server *grpc.Server) {
	pbrg.RegisterRecordGetterServer(server, s)
}

// ReportHealth alerts if we're not healthy
func (s Server) ReportHealth() bool {
	return true
}

// Mote promotes/demotes this server
func (s *Server) Mote(ctx context.Context, master bool) error {
	s.delivering = master

	if master {
		return s.readState(ctx)
	}

	return nil
}

// GetState gets the state of the server
func (s Server) GetState() []*pbg.State {
	text := "No record chosen"
	if s.state.CurrentPick != nil {
		text = s.state.CurrentPick.GetRelease().Title
	}
	return []*pbg.State{
		&pbg.State{Key: "Current", Text: text},
	}
}

// This is the only method that interacts with disk
func (s *Server) readState(ctx context.Context) error {
	state := &pbrg.State{}
	data, _, err := s.KSclient.Read(ctx, KEY, state)

	if err != nil {
		return err
	}

	if data != nil {
		s.state = data.(*pbrg.State)
	}

	return nil
}

func (s *Server) saveState(ctx context.Context) {
	s.KSclient.Save(ctx, KEY, s.state)
}

func main() {
	var quiet = flag.Bool("quiet", false, "Show all output")
	flag.Parse()

	server := Init()

	//Turn off logging
	if *quiet {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	server.GoServer.KSclient = *keystoreclient.GetClient(server.GetIP)
	server.RegisterServer("recordgetter", false)
	//server.RegisterServingTask(server.GetRecords)
	server.Log("Starting!")
	err := server.Serve()
	if err != nil {
		log.Fatalf("Error running getter: %v", err)
	}
}
