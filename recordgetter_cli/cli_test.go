package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	pbgd "github.com/brotherlogic/godiscogs/proto"
	pbrc "github.com/brotherlogic/recordcollection/proto"
	pbrg "github.com/brotherlogic/recordgetter/proto"
	pbscores "github.com/brotherlogic/recordscores/proto"
	"google.golang.org/grpc"
)


func TestFormatRipStatus(t *testing.T) {
	tests := []struct {
		name     string
		metadata *pbrc.ReleaseMetadata
		expected string
	}{
		{
			name:     "nil metadata",
			metadata: nil,
			expected: "Ripped: Not ripped",
		},
		{
			name: "unset rip date (zero)",
			metadata: &pbrc.ReleaseMetadata{
				LastRipDate:   0,
				RippedQuality: 0,
			},
			expected: "Ripped: Not ripped",
		},
		{
			name: "negative rip date",
			metadata: &pbrc.ReleaseMetadata{
				LastRipDate:   -1,
				RippedQuality: 80,
			},
			expected: "Ripped: Not ripped",
		},
		{
			name: "valid rip date and quality",
			metadata: &pbrc.ReleaseMetadata{
				LastRipDate:   1700000000, // 2023-11-14 22:13:20 UTC
				RippedQuality: 95,
			},
			expected: "Ripped: 2023-11-14 (Quality: 95/100)",
		},
		{
			name: "another valid date and quality 100",
			metadata: &pbrc.ReleaseMetadata{
				LastRipDate:   1609459200, // 2021-01-01 00:00:00 UTC
				RippedQuality: 100,
			},
			expected: "Ripped: 2021-01-01 (Quality: 100/100)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatRipStatus(tc.metadata)
			if got != tc.expected {
				t.Errorf("formatRipStatus() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestFormatSalePrice(t *testing.T) {
	tests := []struct {
		name     string
		metadata *pbrc.ReleaseMetadata
		expected string
	}{
		{
			name:     "nil metadata",
			metadata: nil,
			expected: "Sale Price: Not set",
		},
		{
			name: "zero prices",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: 0,
				SalePrice:        0,
			},
			expected: "Sale Price: Not set",
		},
		{
			name: "negative prices",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: -100,
				SalePrice:        -50,
			},
			expected: "Sale Price: Not set",
		},
		{
			name: "current sale price $15.00",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: 1500,
			},
			expected: "Sale Price: $15.00",
		},
		{
			name: "current sale price $2.05",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: 205,
			},
			expected: "Sale Price: $2.05",
		},
		{
			name: "current sale price $0.50",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: 50,
			},
			expected: "Sale Price: $0.50",
		},
		{
			name: "current sale price $0.05",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: 5,
			},
			expected: "Sale Price: $0.05",
		},
		{
			name: "fallback to sale_price when current_sale_price is 0",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: 0,
				SalePrice:        1250,
			},
			expected: "Sale Price: $12.50",
		},
		{
			name: "fallback to sale_price when current_sale_price is negative",
			metadata: &pbrc.ReleaseMetadata{
				CurrentSalePrice: -1,
				SalePrice:        300,
			},
			expected: "Sale Price: $3.00",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatSalePrice(tc.metadata)
			if got != tc.expected {
				t.Errorf("formatSalePrice() = %q, want %q", got, tc.expected)
			}
		})
	}
}

type mockRecordCollectionClient struct {
	records map[int32][]int64
	err     error
}

func (m *mockRecordCollectionClient) QueryRecords(ctx context.Context, in *pbrc.QueryRecordsRequest, opts ...grpc.CallOption) (*pbrc.QueryRecordsResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	relID := in.GetReleaseId()
	instances := m.records[relID]
	return &pbrc.QueryRecordsResponse{InstanceIds: instances}, nil
}

func TestFetchDigitalVersionsStatus(t *testing.T) {
	tests := []struct {
		name       string
		release    *pbgd.Release
		mockClient *mockRecordCollectionClient
		dialErr    error
		expected   string
	}{
		{
			name:     "nil release",
			release:  nil,
			expected: "Digital Versions: None available",
		},
		{
			name: "empty digital versions",
			release: &pbgd.Release{
				DigitalVersions: []int32{},
			},
			expected: "Digital Versions: None available",
		},
		{
			name: "dial error",
			release: &pbgd.Release{
				DigitalVersions: []int32{101, 102},
			},
			dialErr:  fmt.Errorf("connection refused"),
			expected: "Digital Versions: [Unavailable - error querying recordcollection]",
		},
		{
			name: "query error",
			release: &pbgd.Release{
				DigitalVersions: []int32{101, 102},
			},
			mockClient: &mockRecordCollectionClient{
				err: fmt.Errorf("rpc failure"),
			},
			expected: "Digital Versions: [Unavailable - error querying recordcollection]",
		},
		{
			name: "none owned (0 of 2)",
			release: &pbgd.Release{
				DigitalVersions: []int32{101, 102},
			},
			mockClient: &mockRecordCollectionClient{
				records: map[int32][]int64{},
			},
			expected: "Digital Versions: None owned (0 of 2)",
		},
		{
			name: "partial owned (1 of 2)",
			release: &pbgd.Release{
				DigitalVersions: []int32{12345, 67890},
			},
			mockClient: &mockRecordCollectionClient{
				records: map[int32][]int64{
					12345: {999},
				},
			},
			expected: "Digital Versions: Owned 1 of 2 (Owned: [12345])",
		},
		{
			name: "full owned (2 of 2)",
			release: &pbgd.Release{
				DigitalVersions: []int32{12345, 67890},
			},
			mockClient: &mockRecordCollectionClient{
				records: map[int32][]int64{
					12345: {999},
					67890: {1000},
				},
			},
			expected: "Digital Versions: Owned 2 of 2 (Owned: [12345, 67890])",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldDial := dialRecordCollection
			defer func() { dialRecordCollection = oldDial }()

			dialRecordCollection = func(ctx context.Context) (rcClient, func(), error) {
				if tc.dialErr != nil {
					return nil, nil, tc.dialErr
				}
				return tc.mockClient, func() {}, nil
			}

			got := fetchDigitalVersionsStatus(context.Background(), tc.release)
			if got != tc.expected {
				t.Errorf("fetchDigitalVersionsStatus() = %q, want %q", got, tc.expected)
			}
		})
	}
}

type mockRecordScoreServiceClient struct {
	pbscores.UnimplementedRecordScoreServiceServer
	resp *pbscores.GetScoreResponse
	err  error
}

func (m *mockRecordScoreServiceClient) GetScore(ctx context.Context, in *pbscores.GetScoreRequest, opts ...grpc.CallOption) (*pbscores.GetScoreResponse, error) {
	return m.resp, m.err
}

func TestFetchScoreHistoryStatus(t *testing.T) {
	origDial := dialRecordscores
	defer func() { dialRecordscores = origDial }()

	tests := []struct {
		name       string
		dialErr    error
		clientResp *pbscores.GetScoreResponse
		clientErr  error
		expected   string
	}{
		{
			name:     "connection dial failure returns unavailable",
			dialErr:  errors.New("connection failed"),
			expected: "Score History: [Unavailable - error querying recordscores]",
		},
		{
			name:      "rpc failure returns unavailable",
			clientErr: errors.New("rpc failed"),
			expected:  "Score History: [Unavailable - error querying recordscores]",
		},
		{
			name: "zero scores returns no history",
			clientResp: &pbscores.GetScoreResponse{
				Scores: []*pbscores.Score{},
			},
			expected: "Score History: No history",
		},
		{
			name: "single score formatted correctly",
			clientResp: &pbscores.GetScoreResponse{
				Scores: []*pbscores.Score{
					{Rating: 4, ScoreTime: 100},
				},
			},
			expected: "Score History: 1 listens [Ratings: 4] (Avg: 4.0)",
		},
		{
			name: "multiple scores sorted chronologically",
			clientResp: &pbscores.GetScoreResponse{
				Scores: []*pbscores.Score{
					{Rating: 3, ScoreTime: 300},
					{Rating: 4, ScoreTime: 100},
					{Rating: 5, ScoreTime: 200},
				},
			},
			expected: "Score History: 3 listens [Ratings: 4, 5, 3] (Avg: 4.0)",
		},
		{
			name: "caps at last 5 chronological ratings with full average calculation",
			clientResp: &pbscores.GetScoreResponse{
				Scores: []*pbscores.Score{
					{Rating: 2, ScoreTime: 100}, // oldest, should not appear in ratings list
					{Rating: 3, ScoreTime: 200}, // second oldest, should not appear in ratings list
					{Rating: 4, ScoreTime: 300},
					{Rating: 5, ScoreTime: 400},
					{Rating: 4, ScoreTime: 500},
					{Rating: 5, ScoreTime: 600},
					{Rating: 5, ScoreTime: 700},
				},
			},
			// Sum: 2+3+4+5+4+5+5 = 28. Count = 7. Avg: 28/7 = 4.0.
			// Last 5 ratings: 4, 5, 4, 5, 5
			expected: "Score History: 7 listens [Ratings: 4, 5, 4, 5, 5] (Avg: 4.0)",
		},
		{
			name: "average rounded to one decimal place",
			clientResp: &pbscores.GetScoreResponse{
				Scores: []*pbscores.Score{
					{Rating: 4, ScoreTime: 100},
					{Rating: 5, ScoreTime: 200},
					{Rating: 4, ScoreTime: 300},
				},
			},
			// Sum: 13 / 3 = 4.3333... -> 4.3
			expected: "Score History: 3 listens [Ratings: 4, 5, 4] (Avg: 4.3)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.dialErr != nil {
				dialRecordscores = func(ctx context.Context) (pbscores.RecordScoreServiceClient, func(), error) {
					return nil, nil, tc.dialErr
				}
			} else {
				client := &mockRecordScoreServiceClient{
					resp: tc.clientResp,
					err:  tc.clientErr,
				}
				dialRecordscores = func(ctx context.Context) (pbscores.RecordScoreServiceClient, func(), error) {
					return client, func() {}, nil
				}
			}

			got := fetchScoreHistoryStatus(context.Background(), 12345)
			if got != tc.expected {
				t.Errorf("fetchScoreHistoryStatus() = %q, want %q", got, tc.expected)
			}
		})
	}
}

type mockRecordGetterClient struct {
	resp *pbrg.GetRecordResponse
	err  error
}

func (m *mockRecordGetterClient) GetRecord(ctx context.Context, in *pbrg.GetRecordRequest, opts ...grpc.CallOption) (*pbrg.GetRecordResponse, error) {
	return m.resp, m.err
}

func TestGet_StagedToSellEnrichment(t *testing.T) {
	origDialRG := dialRecordGetter
	origDialRC := dialRecordCollection
	origDialRS := dialRecordscores
	origStdout := stdout
	defer func() {
		dialRecordGetter = origDialRG
		dialRecordCollection = origDialRC
		dialRecordscores = origDialRS
		stdout = origStdout
	}()

	mockRG := &mockRecordGetterClient{
		resp: &pbrg.GetRecordResponse{
			Record: &pbrc.Record{
				Release: &pbgd.Release{
					Id:             12345,
					InstanceId:     9999,
					Title:          "Blue Train",
					FormatQuantity: 1,
					Artists: []*pbgd.Artist{
						{Name: "John Coltrane"},
					},
					DigitalVersions: []int32{555},
				},
				Metadata: &pbrc.ReleaseMetadata{
					Category:         pbrc.ReleaseMetadata_STAGED_TO_SELL,
					LastRipDate:      1700000000,
					RippedQuality:    95,
					CurrentSalePrice: 1500,
				},
			},
			Disk: 1,
		},
	}
	dialRecordGetter = func(ctx context.Context) (rgClient, func(), error) {
		return mockRG, func() {}, nil
	}

	mockRC := &mockRecordCollectionClient{
		records: map[int32][]int64{
			555: {8888},
		},
	}
	dialRecordCollection = func(ctx context.Context) (rcClient, func(), error) {
		return mockRC, func() {}, nil
	}

	mockRS := &mockRecordScoreServiceClient{
		resp: &pbscores.GetScoreResponse{
			Scores: []*pbscores.Score{
				{Rating: 5, ScoreTime: 100},
			},
		},
	}
	dialRecordscores = func(ctx context.Context) (pbscores.RecordScoreServiceClient, func(), error) {
		return mockRS, func() {}, nil
	}

	var buf bytes.Buffer
	stdout = &buf

	get(context.Background(), false)

	expectedOutput := strings.Join([]string{
		"John Coltrane - Blue Train [STAGED_TO_SELL] (1/1) {12345,9999}",
		"  Ripped: 2023-11-14 (Quality: 95/100)",
		"  Digital Versions: Owned 1 of 1 (Owned: [555])",
		"  Score History: 1 listens [Ratings: 5] (Avg: 5.0)",
		"  Sale Price: $15.00",
		"",
	}, "\n")

	if buf.String() != expectedOutput {
		t.Errorf("get(ctx, false) output = \n%q\nwant:\n%q", buf.String(), expectedOutput)
	}
}

func TestGet_CleanTrue(t *testing.T) {
	origDialRG := dialRecordGetter
	origStdout := stdout
	defer func() {
		dialRecordGetter = origDialRG
		stdout = origStdout
	}()

	mockRG := &mockRecordGetterClient{
		resp: &pbrg.GetRecordResponse{
			Record: &pbrc.Record{
				Release: &pbgd.Release{
					Id:         12345,
					InstanceId: 9999,
					Title:      "Blue Train",
				},
				Metadata: &pbrc.ReleaseMetadata{
					Category: pbrc.ReleaseMetadata_STAGED_TO_SELL,
				},
			},
		},
	}
	dialRecordGetter = func(ctx context.Context) (rgClient, func(), error) {
		return mockRG, func() {}, nil
	}

	var buf bytes.Buffer
	stdout = &buf

	get(context.Background(), true)

	expectedOutput := "12345\n"
	if buf.String() != expectedOutput {
		t.Errorf("get(ctx, true) output = %q, want %q", buf.String(), expectedOutput)
	}
}

func TestGet_NonStagedToSell(t *testing.T) {
	origDialRG := dialRecordGetter
	origStdout := stdout
	defer func() {
		dialRecordGetter = origDialRG
		stdout = origStdout
	}()

	mockRG := &mockRecordGetterClient{
		resp: &pbrg.GetRecordResponse{
			Record: &pbrc.Record{
				Release: &pbgd.Release{
					Id:             12345,
					InstanceId:     9999,
					Title:          "Blue Train",
					FormatQuantity: 1,
					Artists: []*pbgd.Artist{
						{Name: "John Coltrane"},
					},
				},
				Metadata: &pbrc.ReleaseMetadata{
					Category: pbrc.ReleaseMetadata_PRE_HIGH_SCHOOL,
				},
			},
			Disk: 1,
		},
	}
	dialRecordGetter = func(ctx context.Context) (rgClient, func(), error) {
		return mockRG, func() {}, nil
	}

	var buf bytes.Buffer
	stdout = &buf

	get(context.Background(), false)

	expectedOutput := "John Coltrane - Blue Train [PRE_HIGH_SCHOOL] (1/1) {12345,9999}\n"
	if buf.String() != expectedOutput {
		t.Errorf("get(ctx, false) for non-staged output = %q, want %q", buf.String(), expectedOutput)
	}
}
