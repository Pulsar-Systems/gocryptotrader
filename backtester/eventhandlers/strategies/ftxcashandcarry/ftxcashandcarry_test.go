package ftxcashandcarry

import (
	"errors"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/thrasher-corp/gocryptotrader/backtester/common"
	"github.com/thrasher-corp/gocryptotrader/backtester/data"
	datakline "github.com/thrasher-corp/gocryptotrader/backtester/data/kline"
	"github.com/thrasher-corp/gocryptotrader/backtester/eventhandlers/portfolio"
	"github.com/thrasher-corp/gocryptotrader/backtester/eventhandlers/strategies/base"
	"github.com/thrasher-corp/gocryptotrader/backtester/eventtypes/event"
	eventkline "github.com/thrasher-corp/gocryptotrader/backtester/eventtypes/kline"
	"github.com/thrasher-corp/gocryptotrader/backtester/eventtypes/signal"
	"github.com/thrasher-corp/gocryptotrader/backtester/funding"
	"github.com/thrasher-corp/gocryptotrader/currency"
	"github.com/thrasher-corp/gocryptotrader/exchanges/asset"
	gctkline "github.com/thrasher-corp/gocryptotrader/exchanges/kline"
	gctorder "github.com/thrasher-corp/gocryptotrader/exchanges/order"
)

func TestName(t *testing.T) {
	t.Parallel()
	d := Strategy{}
	if n := d.Name(); n != Name {
		t.Errorf("expected %v", Name)
	}
}

func TestDescription(t *testing.T) {
	t.Parallel()
	d := Strategy{}
	if n := d.Description(); n != description {
		t.Errorf("expected %v", description)
	}
}

func TestSupportsSimultaneousProcessing(t *testing.T) {
	t.Parallel()
	s := Strategy{}
	if !s.SupportsSimultaneousProcessing() {
		t.Error("expected true")
	}
}

func TestSetCustomSettings(t *testing.T) {
	t.Parallel()
	s := Strategy{}
	err := s.SetCustomSettings(nil)
	if err != nil {
		t.Error(err)
	}
	float14 := float64(14)
	mappalopalous := make(map[string]interface{})
	mappalopalous[openShortDistancePercentageString] = float14
	mappalopalous[closeShortDistancePercentageString] = float14

	err = s.SetCustomSettings(mappalopalous)
	if err != nil {
		t.Error(err)
	}

	mappalopalous[openShortDistancePercentageString] = "14"
	err = s.SetCustomSettings(mappalopalous)
	if !errors.Is(err, base.ErrInvalidCustomSettings) {
		t.Errorf("received: %v, expected: %v", err, base.ErrInvalidCustomSettings)
	}

	mappalopalous[closeShortDistancePercentageString] = float14
	mappalopalous[openShortDistancePercentageString] = "14"
	err = s.SetCustomSettings(mappalopalous)
	if !errors.Is(err, base.ErrInvalidCustomSettings) {
		t.Errorf("received: %v, expected: %v", err, base.ErrInvalidCustomSettings)
	}

	mappalopalous[closeShortDistancePercentageString] = float14
	mappalopalous["lol"] = float14
	err = s.SetCustomSettings(mappalopalous)
	if !errors.Is(err, base.ErrInvalidCustomSettings) {
		t.Errorf("received: %v, expected: %v", err, base.ErrInvalidCustomSettings)
	}
}

func TestOnSignal(t *testing.T) {
	t.Parallel()
	s := Strategy{
		openShortDistancePercentage: decimal.NewFromInt(14),
	}
	_, err := s.OnSignal(nil, nil, nil)
	if !errors.Is(err, base.ErrSimultaneousProcessingOnly) {
		t.Errorf("received: %v, expected: %v", err, base.ErrSimultaneousProcessingOnly)
	}
}

func TestSetDefaults(t *testing.T) {
	t.Parallel()
	s := Strategy{}
	s.SetDefaults()
	if !s.openShortDistancePercentage.Equal(decimal.NewFromInt(0)) {
		t.Errorf("expected 5, received %v", s.openShortDistancePercentage)
	}
	if !s.closeShortDistancePercentage.Equal(decimal.NewFromInt(0)) {
		t.Errorf("expected 5, received %v", s.closeShortDistancePercentage)
	}
}

func TestSortSignals(t *testing.T) {
	t.Parallel()
	dInsert := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	exch := "ftx"
	a := asset.Spot
	p := currency.NewPair(currency.BTC, currency.USDT)
	d := data.Base{}
	d.SetStream([]common.DataEventHandler{&eventkline.Kline{
		Base: &event.Base{
			Exchange:     exch,
			Time:         dInsert,
			Interval:     gctkline.OneDay,
			CurrencyPair: p,
			AssetType:    a,
		},
		Open:   decimal.NewFromInt(1337),
		Close:  decimal.NewFromInt(1337),
		Low:    decimal.NewFromInt(1337),
		High:   decimal.NewFromInt(1337),
		Volume: decimal.NewFromInt(1337),
	}})
	d.Next()
	da := &datakline.DataFromKline{
		Item:        gctkline.Item{},
		Base:        d,
		RangeHolder: &gctkline.IntervalRangeHolder{},
	}
	_, err := sortSignals([]data.Handler{da})
	if !errors.Is(err, errNotSetup) {
		t.Errorf("received: %v, expected: %v", err, errNotSetup)
	}

	d2 := data.Base{}
	d2.SetStream([]common.DataEventHandler{&eventkline.Kline{
		Base: &event.Base{
			Exchange:       exch,
			Time:           dInsert,
			Interval:       gctkline.OneDay,
			CurrencyPair:   currency.NewPair(currency.DOGE, currency.XRP),
			AssetType:      asset.Futures,
			UnderlyingPair: p,
		},
		Open:   decimal.NewFromInt(1337),
		Close:  decimal.NewFromInt(1337),
		Low:    decimal.NewFromInt(1337),
		High:   decimal.NewFromInt(1337),
		Volume: decimal.NewFromInt(1337),
	}})
	d2.Next()
	da2 := &datakline.DataFromKline{
		Item:        gctkline.Item{},
		Base:        d2,
		RangeHolder: &gctkline.IntervalRangeHolder{},
	}
	_, err = sortSignals([]data.Handler{da, da2})
	if !errors.Is(err, nil) {
		t.Errorf("received: %v, expected: %v", err, nil)
	}
}

func TestCreateSignals(t *testing.T) {
	t.Parallel()
	s := Strategy{}
	var expectedError = common.ErrNilArguments
	_, err := s.createSignals(nil, nil, nil, decimal.Zero, false)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}

	spotSignal := &signal.Signal{
		Base: &event.Base{AssetType: asset.Spot},
	}
	_, err = s.createSignals(nil, spotSignal, nil, decimal.Zero, false)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}

	// targeting first case
	expectedError = nil
	futuresSignal := &signal.Signal{
		Base: &event.Base{AssetType: asset.Futures},
	}
	resp, err := s.createSignals(nil, spotSignal, futuresSignal, decimal.Zero, false)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}
	if len(resp) != 1 {
		t.Errorf("received '%v' expected '%v", len(resp), 1)
	}
	if resp[0].GetAssetType() != asset.Spot {
		t.Errorf("received '%v' expected '%v", resp[0].GetAssetType(), asset.Spot)
	}

	// targeting second case:
	pos := []gctorder.Position{
		{
			Status: gctorder.Open,
		},
	}
	resp, err = s.createSignals(pos, spotSignal, futuresSignal, decimal.Zero, false)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}
	if len(resp) != 2 {
		t.Errorf("received '%v' expected '%v", len(resp), 2)
	}
	caseTested := false
	for i := range resp {
		if resp[i].GetAssetType().IsFutures() {
			if resp[i].GetDirection() != gctorder.ClosePosition {
				t.Errorf("received '%v' expected '%v", resp[i].GetDirection(), gctorder.ClosePosition)
			}
			caseTested = true
		}
	}
	if !caseTested {
		t.Fatal("unhandled issue in test scenario")
	}

	// targeting third case
	resp, err = s.createSignals(pos, spotSignal, futuresSignal, decimal.Zero, true)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}
	if len(resp) != 2 {
		t.Errorf("received '%v' expected '%v", len(resp), 2)
	}
	caseTested = false
	for i := range resp {
		if resp[i].GetAssetType().IsFutures() {
			if resp[i].GetDirection() != gctorder.ClosePosition {
				t.Errorf("received '%v' expected '%v", resp[i].GetDirection(), gctorder.ClosePosition)
			}
			caseTested = true
		}
	}
	if !caseTested {
		t.Fatal("unhandled issue in test scenario")
	}

	// targeting first case after a cash and carry is completed, have a new one opened
	pos[0].Status = gctorder.Closed
	resp, err = s.createSignals(pos, spotSignal, futuresSignal, decimal.NewFromInt(1337), true)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}
	if len(resp) != 1 {
		t.Errorf("received '%v' expected '%v", len(resp), 1)
	}
	caseTested = false
	for i := range resp {
		if resp[i].GetAssetType() == asset.Spot {
			if resp[i].GetDirection() != gctorder.Buy {
				t.Errorf("received '%v' expected '%v", resp[i].GetDirection(), gctorder.Buy)
			}
			if resp[i].GetFillDependentEvent() == nil {
				t.Errorf("received '%v' expected '%v'", nil, "fill dependent event")
			}
			caseTested = true
		}
	}
	if !caseTested {
		t.Fatal("unhandled issue in test scenario")
	}

	// targeting default case
	pos[0].Status = gctorder.UnknownStatus
	resp, err = s.createSignals(pos, spotSignal, futuresSignal, decimal.NewFromInt(1337), true)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}
	if len(resp) != 2 {
		t.Errorf("received '%v' expected '%v", len(resp), 2)
	}
}

// funderino overrides default implementation
type funderino struct {
	funding.FundManager
	hasBeenLiquidated bool
}

// HasExchangeBeenLiquidated overrides default implementation
func (f funderino) HasExchangeBeenLiquidated(_ common.EventHandler) bool {
	return f.hasBeenLiquidated
}

// portfolerino overrides default implementation
type portfolerino struct {
	portfolio.Portfolio
}

// GetPositions overrides default implementation
func (p portfolerino) GetPositions(common.EventHandler) ([]gctorder.Position, error) {
	return []gctorder.Position{
		{
			Exchange:           exchangeName,
			Asset:              asset.Spot,
			Pair:               currency.NewPair(currency.BTC, currency.USD),
			Underlying:         currency.BTC,
			CollateralCurrency: currency.USD,
		},
	}, nil
}

func TestOnSimultaneousSignals(t *testing.T) {
	t.Parallel()
	s := Strategy{}
	var expectedError = errNoSignals
	_, err := s.OnSimultaneousSignals(nil, nil, nil)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}

	expectedError = common.ErrNilArguments
	cp := currency.NewPair(currency.BTC, currency.USD)
	d := &datakline.DataFromKline{
		Base: data.Base{},
		Item: gctkline.Item{
			Exchange:       exchangeName,
			Asset:          asset.Spot,
			Pair:           cp,
			UnderlyingPair: currency.NewPair(currency.BTC, currency.USD),
		},
	}
	tt := time.Now()
	d.SetStream([]common.DataEventHandler{&eventkline.Kline{
		Base: &event.Base{
			Exchange:     exchangeName,
			Time:         tt,
			Interval:     gctkline.OneDay,
			CurrencyPair: cp,
			AssetType:    asset.Spot,
		},
		Open:   decimal.NewFromInt(1337),
		Close:  decimal.NewFromInt(1337),
		Low:    decimal.NewFromInt(1337),
		High:   decimal.NewFromInt(1337),
		Volume: decimal.NewFromInt(1337),
	}})

	d.Next()
	signals := []data.Handler{
		d,
	}
	f := &funderino{}
	_, err = s.OnSimultaneousSignals(signals, f, nil)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}

	p := &portfolerino{}
	expectedError = errNotSetup
	_, err = s.OnSimultaneousSignals(signals, f, p)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}

	expectedError = nil
	d2 := &datakline.DataFromKline{
		Base: data.Base{},
		Item: gctkline.Item{
			Exchange:       exchangeName,
			Asset:          asset.Futures,
			Pair:           cp,
			UnderlyingPair: cp,
		},
	}
	d2.SetStream([]common.DataEventHandler{&eventkline.Kline{
		Base: &event.Base{
			Exchange:       exchangeName,
			Time:           tt,
			Interval:       gctkline.OneDay,
			CurrencyPair:   cp,
			AssetType:      asset.Futures,
			UnderlyingPair: cp,
		},
		Open:   decimal.NewFromInt(1337),
		Close:  decimal.NewFromInt(1337),
		Low:    decimal.NewFromInt(1337),
		High:   decimal.NewFromInt(1337),
		Volume: decimal.NewFromInt(1337),
	}})
	d2.Next()
	signals = []data.Handler{
		d,
		d2,
	}
	resp, err := s.OnSimultaneousSignals(signals, f, p)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}
	if len(resp) != 2 {
		t.Errorf("received '%v' expected '%v", len(resp), 2)
	}

	f.hasBeenLiquidated = true
	resp, err = s.OnSimultaneousSignals(signals, f, p)
	if !errors.Is(err, expectedError) {
		t.Errorf("received '%v' expected '%v", err, expectedError)
	}
	if len(resp) != 2 {
		t.Fatalf("received '%v' expected '%v", len(resp), 2)
	}
	if resp[0].GetDirection() != gctorder.DoNothing {
		t.Errorf("received '%v' expected '%v", resp[0].GetDirection(), gctorder.DoNothing)
	}
}
