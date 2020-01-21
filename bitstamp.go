package bitstamp

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/avdva/go-pusher"
	"github.com/pkg/errors"
)

const (
	// API_URL is a url of the api v2 endpoint
	API_URL = "https://www.bitstamp.net/api/v2"
	// APP_KEY is a pusher app key.
	APP_KEY = "de504dc5763aeef9ff52"
)

// Ticker is a tick representation.
type Ticker struct {
	Last float64 `json:",string"`
	High float64 `json:",string"`
	Low  float64 `json:",string"`
	Ask  float64 `json:",string"`
	Bid  float64 `json:",string"`
}

// OrderBook is a standart order book.
type OrderBook struct {
	Time time.Time
	Asks []Order
	Bids []Order
}

// Order is a (price, amount) pair
type Order struct {
	Price  float64
	Amount float64
}

// Trade is a trade representation.
type Trade struct {
	Time   time.Time
	ID     string
	Price  float64
	Amount float64
}

// Api is a Bitstamp client.
type Api struct {
	User     string
	Password string
}

// NewFromConfig creates a new api object given a config file. The config file must
// be json formated to inlude User and Password
func NewFromConfig(cfgfile string) (api *Api, err error) {
	file, err := ioutil.ReadFile(cfgfile)
	if err != nil {
		return nil, err
	}

	api = new(Api)
	err = json.Unmarshal(file, api)
	if err != nil {
		return nil, err
	}
	return api, nil
}

// New creates a new api object given a user and a password.
func New(user, password string) *Api {
	api := &Api{
		User:     user,
		Password: password,
	}
	return api
}

func (api *Api) get(url string) (body []byte, err error) {
	resp, err := http.Get(fmt.Sprint(API_URL, url))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	return
}

// GetTicker returns a ticker for the goven symbol.
func (api *Api) GetTicker(symbol string) (ticker *Ticker, err error) {
	body, err := api.get("/ticker/" + symbol)
	if err != nil {
		return
	}
	ticker = new(Ticker)
	err = json.Unmarshal(body, ticker)
	if err != nil {
		return
	}
	return
}

// GetOrderBook returns order book for the given symbol.
func (api *Api) GetOrderBook(symbol string) (orderbook *OrderBook, err error) {
	body, err := api.get("/order_book/" + symbol)
	if err != nil {
		return
	}
	return api.parseOrderBook(body)
}

func (api *Api) parseOrderBook(data []byte) (*OrderBook, error) {
	defaultstruct := make(map[string]interface{})
	err := json.Unmarshal(data, &defaultstruct)
	if err != nil {
		return nil, err
	}

	timestamp := time.Now().Unix()

	if timestampObj, found := defaultstruct["timestamp"]; found {
		timestampStr, ok := timestampObj.(string)
		if !ok {
			return nil, errors.New("invalid timestamp")
		}
		timestamp, err = strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	result := &OrderBook{Time: time.Unix(timestamp, 0)}

	bids := defaultstruct["bids"].([]interface{})
	asks := defaultstruct["asks"].([]interface{})

	parse := func(arr []interface{}) ([]Order, error) {
		var result []Order
		for _, elem := range arr {
			ord := elem.([]interface{})
			price, err := strconv.ParseFloat(ord[0].(string), 64)
			if err != nil {
				return nil, err
			}
			amount, err := strconv.ParseFloat(ord[1].(string), 64)
			if err != nil {
				return nil, err
			}
			result = append(result, Order{Price: price, Amount: amount})
		}
		return result, nil
	}

	if parsedBids, err := parse(bids); err == nil {
		result.Bids = parsedBids
	} else {
		return nil, errors.Wrap(err, "bids parsing error")
	}

	if parsedAsks, err := parse(asks); err == nil {
		result.Asks = parsedAsks
	} else {
		return nil, errors.Wrap(err, "asks parsing error")
	}

	return result, nil
}

// GetTrades returns the list of last trades with default parameters.
func (api *Api) GetTrades(symbol string) (trades []Trade, err error) {
	body, err := api.get("/transactions/" + symbol)
	return formatTrades(body)
}

// GetTradesParams returns the list of last trades.
//	interval - The time interval from which we want the transactions to be returned.
//		Possible values are minute, hour (default) or day.
func (api *Api) GetTradesParams(symbol string, interval string) (trades []Trade, err error) {
	values := url.Values{}
	values.Add("time", interval)
	body, err := api.get("/transactions/" + symbol + "/?" + values.Encode())
	if err != nil {
		return
	}
	return formatTrades(body)
}

// SubscribeOrderBook subscribes for websocket events and sends order book updates
// into dataChan. To stop processing, sent to, or close stopChan.
func (api *Api) SubscribeOrderBook(symb string, dataChan chan<- OrderBook, stopChan <-chan struct{}) error {
	pusherClient, err := pusher.NewClient(APP_KEY)
	if err != nil {
		return errors.Wrap(err, "client creation error")
	}
	channel := "order_book"
	if symb != "btcusd" {
		channel += "_" + symb
	}
	err = pusherClient.Subscribe(channel)
	if err != nil {
		return errors.Wrap(err, "subscription error")
	}
	dataChannelTrade, err := pusherClient.Bind("data")
	if err != nil {
		return errors.Wrap(err, "bind error")
	}
	for {
		select {
		case dataEvt, ok := <-dataChannelTrade:
			if !ok {
				return errors.New("websocket has been closed")
			}
			if ob, err := api.parseOrderBook([]byte(dataEvt.Data)); err == nil {
				dataChan <- *ob
			}
		case <-stopChan:
			pusherClient.Unbind("data")
			pusherClient.Unsubscribe("order_book")
			pusherClient.Close()
			return nil
		}
	}
}

func formatTrades(body []byte) (trades []Trade, err error) {
	var defaultstruct []interface{}
	err = json.Unmarshal(body, &defaultstruct)
	if err != nil {
		return
	}
	trades = make([]Trade, len(defaultstruct))
	for i, _trade := range defaultstruct {
		_t := _trade.(map[string]interface{})
		price, err := strconv.ParseFloat(_t["price"].(string), 64)
		if err != nil {
			return trades, err
		}
		amount, err := strconv.ParseFloat(_t["amount"].(string), 64)
		if err != nil {
			return trades, err
		}
		timestampStr := _t["date"].(string)
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			return trades, err
		}
		time := time.Unix(timestamp, 0)
		trade := Trade{
			Time:   time,
			ID:     _t["tid"].(string),
			Price:  price,
			Amount: amount,
		}
		trades[i] = trade
	}
	return
}
