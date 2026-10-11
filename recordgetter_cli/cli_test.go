package main

import (
	"testing"

	pbrc "github.com/brotherlogic/recordcollection/proto"
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
