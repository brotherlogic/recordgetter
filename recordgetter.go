package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"time"

	"github.com/brotherlogic/goserver"
	"github.com/brotherlogic/keystore/client"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	pbd "github.com/brotherlogic/godiscogs"
	pbg "github.com/brotherlogic/goserver/proto"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbrg "github.com/brotherlogic/recordgetter/proto"
	pbro "github.com/brotherlogic/recordsorganiser/proto"
)

//Server main server type
type Server struct {
	*goserver.GoServer
	serving    bool
	delivering bool
	state      *pbrg.State
	updater    updater
	rGetter    getter
	rd         *rand.Rand
	requests   int64
	lastPre    time.Time
	org        org
}

const (
	wait = 5 * time.Second

	//KEY under which we store the collection
	KEY = "/github.com/brotherlogic/recordgetter/state"
)

type org interface {
	getLocations(ctx context.Context) ([]*pbro.Location, error)
}

type prodOrg struct {
	dial func(server string) (*grpc.ClientConn, error)
}

func (p *prodOrg) getLocations(ctx context.Context) ([]*pbro.Location, error) {
	conn, err := p.dial("recordsorganiser")
	if err != nil {
		return []*pbro.Location{}, err
	}
	defer conn.Close()
	client := pbro.NewOrganiserServiceClient(conn)
	locations, err := client.GetOrganisation(ctx, &pbro.GetOrganisationRequest{})
	if err != nil {
		return []*pbro.Location{}, err
	}

	return locations.Locations, err
}

type getter interface {
	getRecords(ctx context.Context, folderID int32) (*pbrc.GetRecordsResponse, error)
	getRelease(ctx context.Context, instanceID int32) (*pbrc.Record, error)
	getRecordsInCategory(ctx context.Context, category pbrc.ReleaseMetadata_Category) ([]int32, error)
	getRecordsInFolder(ctx context.Context, folder int32) ([]int32, error)
}

type prodGetter struct {
	dial func(server string) (*grpc.ClientConn, error)
}

func (p *prodGetter) getRecords(ctx context.Context, folderID int32) (*pbrc.GetRecordsResponse, error) {
	conn, err := p.dial("recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)

	//Only get clean records
	r, err := client.GetRecords(ctx, &pbrc.GetRecordsRequest{Caller: "recordgetter", Filter: &pbrc.Record{Release: &pbd.Release{FolderId: folderID}}}, grpc.MaxCallRecvMsgSize(1024*1024*1024))
	return r, err
}

func (p *prodGetter) getRecordsInCategory(ctx context.Context, category pbrc.ReleaseMetadata_Category) ([]int32, error) {
	conn, err := p.dial("recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)

	r, err := client.QueryRecords(ctx, &pbrc.QueryRecordsRequest{Query: &pbrc.QueryRecordsRequest_Category{category}})
	if err == nil {
		return r.GetInstanceIds(), err
	}
	return []int32{}, err
}

func (p *prodGetter) getRecordsInFolder(ctx context.Context, folder int32) ([]int32, error) {
	conn, err := p.dial("recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := pbrc.NewRecordCollectionServiceClient(conn)

	r, err := client.QueryRecords(ctx, &pbrc.QueryRecordsRequest{Query: &pbrc.QueryRecordsRequest_FolderId{folder}})
	if err == nil {
		return r.GetInstanceIds(), err
	}
	return []int32{}, err
}

func (p *prodGetter) getRelease(ctx context.Context, instance int32) (*pbrc.Record, error) {
	conn, err := p.dial("recordcollection")
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pbrc.NewRecordCollectionServiceClient(conn)
	r, err := client.GetRecord(ctx, &pbrc.GetRecordRequest{InstanceId: instance})
	if err != nil {
		return nil, err
	}
	return r.GetRecord(), err
}

type updater interface {
	update(ctx context.Context, rec *pbrc.Record) error
}

type prodUpdater struct {
	dial func(server string) (*grpc.ClientConn, error)
}

func (p *prodUpdater) update(ctx context.Context, rec *pbrc.Record) error {
	conn, err := p.dial("recordcollection")
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

func (s *Server) dateFine(rc *pbrc.Record, t time.Time) bool {
	for _, score := range s.state.Scores {
		if score.InstanceId == rc.GetRelease().InstanceId {
			if t.AddDate(0, 0, -7).Unix() <= score.ScoreDate {
				return false
			}
		}
	}
	return true
}

func (s *Server) getReleaseFromPile(ctx context.Context, t time.Time) (*pbrc.Record, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	//Look for a record staged to sell
	rec, err := s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_STAGED_TO_SELL)
	if err != nil || rec != nil {
		return rec, err
	}

	if t.Sub(s.lastPre) > time.Hour*3 {
		rec, err = s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_PRE_FRESHMAN)
		if err != nil || rec != nil {
			s.lastPre = time.Now()
			return rec, err
		}
	}

	rec, err = s.getCategoryRecord(ctx, t, pbrc.ReleaseMetadata_UNLISTENED)
	if err != nil || rec != nil {
		return rec, err
	}

	// Look for pre high school records
	rec, err = s.getInFolderWithCategory(ctx, t, int32(673768), pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL)
	if err != nil || rec != nil {
		return rec, err
	}

	rec, err = s.getInFolders(ctx, t, s.state.ActiveFolders)
	if err != nil || rec != nil {
		return rec, err
	}
	return nil, fmt.Errorf("Unable to locate record to listen to")
}

//Init a record getter
func Init() *Server {
	s := &Server{
		GoServer: &goserver.GoServer{},
		serving:  true, delivering: true,
		state: &pbrg.State{},
		rd:    rand.New(rand.NewSource(time.Now().Unix())),
	}
	s.updater = &prodUpdater{s.DialMaster}
	s.rGetter = &prodGetter{s.DialMaster}
	s.org = &prodOrg{s.DialMaster}
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

// Shutdown the server
func (s *Server) Shutdown(ctx context.Context) error {
	return nil
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
func (s *Server) GetState() []*pbg.State {
	text := "No record chosen"
	state := ""
	match := ""
	goal := ""
	price := ""
	formats := ""
	if s.state.CurrentPick != nil {
		text = s.state.CurrentPick.GetRelease().Title
		state = fmt.Sprintf("%v", s.state.CurrentPick.GetMetadata().Category)
		match = fmt.Sprintf("%v", s.state.CurrentPick.GetMetadata().Match)
		goal = fmt.Sprintf("%v", s.state.CurrentPick.GetMetadata().GoalFolder)
		price = fmt.Sprintf("%v", s.state.CurrentPick.GetMetadata().CurrentSalePrice)
		formats = fmt.Sprintf("%v", s.state.CurrentPick.GetRelease().Formats)
	}

	val := int64(0)
	val2 := int64(0)
	disk := int32(1)
	if s.state != nil && s.state.CurrentPick != nil {
		val = int64(s.state.CurrentPick.GetRelease().Id)
		val2 = int64(s.state.CurrentPick.GetRelease().InstanceId)
		for _, score := range s.state.Scores {
			if score.InstanceId == s.state.CurrentPick.GetRelease().InstanceId && score.DiskNumber > disk {
				disk = score.DiskNumber + 1
			}
		}
	}

	output := ""
	for _, v := range s.state.Scores {
		if s.state != nil && s.state.CurrentPick != nil {
			if v.InstanceId == s.state.CurrentPick.GetRelease().InstanceId {
				output += fmt.Sprintf("%v - %v,", v.DiskNumber, v.Score)
			}
		}
	}

	return []*pbg.State{
		&pbg.State{Key: "folders", Text: fmt.Sprintf("%v", s.state.ActiveFolders)},
		&pbg.State{Key: "current", Text: text},
		&pbg.State{Key: "format", Text: formats},
		&pbg.State{Key: "disk", Value: int64(disk)},
		&pbg.State{Key: "current_state", Text: state},
		&pbg.State{Key: "match", Text: match},
		&pbg.State{Key: "goal", Text: goal},
		&pbg.State{Key: "price", Text: price},
		&pbg.State{Key: "current_id", Value: val},
		&pbg.State{Key: "current_iid", Value: val2},
		&pbg.State{Key: "requests", Value: s.requests},
		&pbg.State{Key: "tracking", Text: output},
		&pbg.State{Key: "scores", Value: int64(len(s.state.Scores))},
	}
}

// This is the only method that interacts with disk
func (s *Server) readState(ctx context.Context) error {
	state := &pbrg.State{}
	data, _, err := s.KSclient.Read(ctx, KEY, state)

	s.Log(fmt.Sprintf("Read state: %v -> %v", proto.Size(data), err))

	if err != nil {
		return err
	}

	if data != nil {
		s.state = data.(*pbrg.State)
	}

	if len(s.state.ActiveFolders) == 0 {
		s.state.ActiveFolders = []int32{242017}
	}

	return s.readLocations(ctx)
}

func (s *Server) saveState(ctx context.Context) {
	err := s.KSclient.Save(ctx, KEY, s.state)
	s.Log(fmt.Sprintf("Save %v -> %v", proto.Size(s.state), err))
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

	server.GoServer.KSclient = *keystoreclient.GetClient(server.DialMaster)
	server.RPCTracing = true
	server.RegisterServer("recordgetter", false)

	//server.RegisterServingTask(server.GetRecords)
	err := server.Serve()
	if err != nil {
		log.Fatalf("Error running getter: %v", err)
	}
}
