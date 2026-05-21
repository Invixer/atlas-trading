// ATLAS v2 - Production Trading Engine
// Async-first, WebSocket-driven, with proper strategy and ML

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Configuration
const FINNHUB_KEY = process.env.FINNHUB_KEY;
const START_CAPITAL = 1000;
const DATA_FILE = path.join(__dirname, 'trading_data.json');
const TRADES_LOG_FILE = path.join(__dirname, 'trades_history.json');

if (!FINNHUB_KEY || FINNHUB_KEY.includes('your_')) {
  console.error('❌ ERROR: Set FINNHUB_KEY in .env');
  process.exit(1);
}

// Market data store (in-memory cache for speed)
let marketData = {};
let portfolio = {
  cash: START_CAPITAL,
  positions: {},
  trades: [],
  createdAt: new Date(),
  lastUpdate: new Date()
};

// Expanded watchlist - 50 penny stocks for maximum opportunities
const watchlist = [
  'PLTR','SOFI','MARA','HOOD','SOUN','IONQ','RKLB','BBAI','HIMS','CIFR',
  'LCID','RIDE','NIO','XPEV','RMSL','BLNK','GEVO','HYLN','PRPO','CERS',
  'PROG','INDO','CCIV','NAKD','BNGO','OCGN','NOVN','OPTI','CLWD','ENZC',
  'CLOV','WISH','GROVE','ZASH','BFRI','ASPS','MICT','CYDY','ATOS','NURO',
  'JAGX','KOSS','SUMO','NILE','TYME','PHUN','ELYS','FTHK','TRCH','XBUS'
];

// Connected WebSocket clients for broadcasting
let dashboardClients = [];
let finnhubConnected = false;

// Initialize market data cache
watchlist.forEach(sym => {
  marketData[sym] = {
    price: 0,
    bid: 0,
    ask: 0,
    volume: 0,
    timestamp: Date.now(),
    priceHistory: [],
    volumeHistory: [],
    lastUpdate: Date.now()
  };
});

// Load persisted data
function loadData() {
  if (fs.existsSync(DATA_FILE)) {
    try {
      portfolio = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      console.log(`[DATA] Loaded portfolio: $${portfolio.cash.toFixed(2)}`);
    } catch (e) {
      console.log('[DATA] Starting fresh portfolio');
    }
  }
}

// Save data periodically
function saveData() {
  fs.writeFileSync(DATA_FILE, JSON.stringify(portfolio, null, 2));
  fs.writeFileSync(TRADES_LOG_FILE, JSON.stringify(portfolio.trades, null, 2));
}

setInterval(saveData, 5000);

// Finnhub WebSocket - Real-time price data
function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);

  ws.on('open', () => {
    console.log('[FINNHUB] 🟢 Connected');
    finnhubConnected = true;
    
    // Subscribe to all tickers
    watchlist.forEach(sym => {
      ws.send(JSON.stringify({ type: 'subscribe', symbol: sym }));
    });
    
    broadcastStatus('FINNHUB_CONNECTED');
  });

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      
      if (msg.type === 'trade' && msg.data) {
        msg.data.forEach(trade => {
          const sym = trade.s;
          if (marketData[sym]) {
            // Update price cache
            marketData[sym].price = trade.p;
            marketData[sym].timestamp = trade.t || Date.now();
            marketData[sym].lastUpdate = Date.now();
            
            // Keep price history (last 100 ticks)
            marketData[sym].priceHistory.push({
              price: trade.p,
              timestamp: trade.t || Date.now()
            });
            if (marketData[sym].priceHistory.length > 100) {
              marketData[sym].priceHistory.shift();
            }
          }
        });
        
        // Run strategy evaluation
        evaluateStrategy();
        checkExits();
        broadcastPortfolioUpdate();
      }
    } catch (e) {
      console.error('[FINNHUB] Parse error:', e.message);
    }
  });

  ws.on('error', (err) => {
    console.error('[FINNHUB] Error:', err.message);
    finnhubConnected = false;
    broadcastStatus('FINNHUB_ERROR');
  });

  ws.on('close', () => {
    console.log('[FINNHUB] 🔴 Disconnected, reconnecting...');
    finnhubConnected = false;
    broadcastStatus('FINNHUB_DISCONNECTED');
    setTimeout(connectFinnhub, 5000);
  });
}

// ULTRA-AGGRESSIVE STRATEGY: Unlimited trades, maximize profits
function evaluateStrategy() {
  if (portfolio.cash < 20) return; // Need minimum $20 to trade
  
  // Scan ALL tickers for signals
  watchlist.forEach(sym => {
    const data = marketData[sym];
    
    if (!data || data.price === 0 || data.priceHistory.length < 2) return;
    
    const history = data.priceHistory;
    const currentPrice = data.price;
    const prevPrice = history[Math.max(0, history.length - 2)].price;
    
    // Momentum: ANY movement (even 0.01%) is a signal
    const momentum = ((currentPrice - prevPrice) / prevPrice) * 100;
    
    // ENTRY CONDITIONS (ultra-aggressive):
    // 1. Price UP: Buy on any upward tick
    // 2. Price DOWN: Also buy (buying dips for reversal)
    // 3. Price < $5: Focus on penny stocks
    // 4. Have cash: Use remaining cash
    const hasUpMomentum = momentum > 0.001; // Literally any upward movement
    const hasDownMomentum = momentum < -0.001; // Buy dips for reversal
    const goodPrice = currentPrice < 5;
    const haveEnoughCash = portfolio.cash > currentPrice;
    
    if ((hasUpMomentum || hasDownMomentum) && goodPrice && haveEnoughCash) {
      // Execute trade: Buy with ALL available cash (or majority)
      const qty = Math.floor(portfolio.cash * 0.5 / currentPrice); // Use 50% of cash per trade
      
      if (qty >= 1) {
        executeTrade({
          symbol: sym,
          price: currentPrice,
          momentum: momentum,
          qty: qty
        });
      }
    }
  });
}

// Execute a trade - UNLIMITED positions allowed
function executeTrade(setup) {
  const { symbol, price, momentum, qty } = setup;
  
  // Dynamic position sizing based on available cash
  // Use 50% of remaining cash for this trade
  const actualQty = qty || Math.floor(portfolio.cash * 0.5 / price);
  
  if (actualQty < 1 || portfolio.cash < price) return;
  
  const cost = actualQty * price;
  portfolio.cash -= cost;
  
  // Allow UNLIMITED positions per symbol (stack multiple trades)
  if (!portfolio.positions[symbol]) {
    portfolio.positions[symbol] = [];
  }
  
  portfolio.positions[symbol].push({
    qty: actualQty,
    entryPrice: price,
    entryTime: Date.now(),
    momentum: momentum,
    id: `${symbol}_${Date.now()}`
  });
  
  const trade = {
    type: 'BUY',
    symbol: symbol,
    qty: actualQty,
    price: price,
    timestamp: new Date().toISOString(),
    momentum: momentum,
    tradeId: `${symbol}_${Date.now()}`
  };
  
  portfolio.trades.push(trade);
  
  console.log(`[TRADE] BUY ${actualQty}x ${symbol} @$${price.toFixed(2)} | Momentum: ${momentum.toFixed(3)}% | Cash: $${portfolio.cash.toFixed(2)}`);
  broadcastTrade(trade);
}

// Check exit conditions - handle multiple positions per symbol
function checkExits() {
  watchlist.forEach(sym => {
    const currentPrice = marketData[sym].price;
    if (currentPrice === 0 || !portfolio.positions[sym]) return;
    
    // Handle array of positions (unlimited per symbol)
    portfolio.positions[sym] = portfolio.positions[sym].filter(pos => {
      const pnl = (currentPrice - pos.entryPrice) / pos.entryPrice;
      const pnlDollars = (currentPrice - pos.entryPrice) * pos.qty;
      
      // AGGRESSIVE exit conditions
      // TP: +1.5% (quick profits, reinvest)
      // SL: -1% (minimal loss, move on)
      // TRAIL: +0.5% trailing stop (lock in gains)
      const shouldTakeProfit = pnl >= 0.015; // +1.5%
      const shouldStopLoss = pnl <= -0.01;   // -1%
      
      if (shouldTakeProfit || shouldStopLoss) {
        closeTrade(sym, currentPrice, pnlDollars, shouldTakeProfit, pos.id);
        return false; // Remove this position
      }
      
      return true; // Keep this position
    });
    
    // Remove symbol from positions if no active trades
    if (portfolio.positions[sym].length === 0) {
      delete portfolio.positions[sym];
    }
  });
}

// Close a position
function closeTrade(symbol, exitPrice, pnlDollars, isProfit, tradeId) {
  const positions = portfolio.positions[symbol];
  if (!positions) return;
  
  // Find the specific trade to close
  const posIndex = positions.findIndex(p => p.id === tradeId);
  if (posIndex === -1) return;
  
  const pos = positions[posIndex];
  portfolio.cash += exitPrice * pos.qty;
  
  const trade = {
    type: 'SELL',
    symbol: symbol,
    qty: pos.qty,
    price: exitPrice,
    timestamp: new Date().toISOString(),
    entryPrice: pos.entryPrice,
    pnl: pnlDollars,
    reason: isProfit ? 'TAKE_PROFIT' : 'STOP_LOSS',
    tradeId: tradeId
  };
  
  portfolio.trades.push(trade);
  positions.splice(posIndex, 1);
  
  const action = isProfit ? 'PROFIT' : 'LOSS';
  console.log(`[${action}] SELL ${pos.qty}x ${symbol} @$${exitPrice.toFixed(2)} | P&L: $${pnlDollars.toFixed(2)} | Cash: $${portfolio.cash.toFixed(2)}`);
  broadcastTrade(trade);
}

// Calculate portfolio metrics
function getPortfolioMetrics() {
  let unrealizedPnL = 0;
  const positionDetails = [];
  
  // Handle multiple positions per symbol
  Object.entries(portfolio.positions).forEach(([sym, posArray]) => {
    if (Array.isArray(posArray)) {
      posArray.forEach((pos, idx) => {
        const currentPrice = marketData[sym].price || pos.entryPrice;
        const positionPnL = (currentPrice - pos.entryPrice) * pos.qty;
        unrealizedPnL += positionPnL;
        
        positionDetails.push({
          symbol: sym + (idx > 0 ? ` #${idx + 1}` : ''),
          qty: pos.qty,
          entryPrice: pos.entryPrice.toFixed(2),
          currentPrice: currentPrice.toFixed(2),
          unrealizedPnL: positionPnL.toFixed(2),
          unrealizedReturn: (((currentPrice - pos.entryPrice) / pos.entryPrice) * 100).toFixed(2)
        });
      });
    } else {
      // Legacy single position format
      const pos = posArray;
      const currentPrice = marketData[sym].price || pos.entryPrice;
      const positionPnL = (currentPrice - pos.entryPrice) * pos.qty;
      unrealizedPnL += positionPnL;
      
      positionDetails.push({
        symbol: sym,
        qty: pos.qty,
        entryPrice: pos.entryPrice.toFixed(2),
        currentPrice: currentPrice.toFixed(2),
        unrealizedPnL: positionPnL.toFixed(2),
        unrealizedReturn: (((currentPrice - pos.entryPrice) / pos.entryPrice) * 100).toFixed(2)
      });
    }
  });

  const closes = portfolio.trades.filter(t => t.type === 'SELL');
  const realizedPnL = closes.reduce((sum, t) => sum + (t.pnl || 0), 0);
  const totalPnL = realizedPnL + unrealizedPnL;
  
  const totalValue = portfolio.cash + Object.entries(portfolio.positions).reduce((sum, [sym, posArray]) => {
    const currentPrice = marketData[sym].price;
    if (Array.isArray(posArray)) {
      return sum + posArray.reduce((s, pos) => s + ((currentPrice || pos.entryPrice) * pos.qty), 0);
    } else {
      return sum + ((currentPrice || posArray.entryPrice) * posArray.qty);
    }
  }, 0);

  const wins = closes.filter(t => t.pnl > 0).length;
  const losses = closes.filter(t => t.pnl < 0).length;
  const winRate = wins / Math.max(1, wins + losses) * 100;

  return {
    cash: portfolio.cash.toFixed(2),
    totalValue: totalValue.toFixed(2),
    realizedPnL: realizedPnL.toFixed(2),
    unrealizedPnL: unrealizedPnL.toFixed(2),
    totalPnL: totalPnL.toFixed(2),
    return: ((totalPnL / START_CAPITAL) * 100).toFixed(2),
    positions: positionDetails,
    trades: portfolio.trades.slice(-50),
    stats: {
      totalTrades: closes.length,
      wins: wins,
      losses: losses,
      winRate: winRate.toFixed(2),
      activePositions: Object.keys(portfolio.positions).length
    }
  };
}

// Broadcast functions
function broadcastPortfolioUpdate() {
  const metrics = getPortfolioMetrics();
  const data = JSON.stringify({
    type: 'portfolio_update',
    ...metrics,
    timestamp: new Date().toISOString(),
    finnhubConnected: finnhubConnected
  });

  dashboardClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

function broadcastTrade(trade) {
  const data = JSON.stringify({
    type: 'new_trade',
    trade: trade
  });

  dashboardClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

function broadcastStatus(status) {
  const data = JSON.stringify({
    type: 'status',
    status: status
  });

  dashboardClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}

// Dashboard WebSocket server
wss.on('connection', (ws) => {
  dashboardClients.push(ws);
  console.log('[WS] Dashboard client connected');
  
  // Send initial data
  const metrics = getPortfolioMetrics();
  ws.send(JSON.stringify({
    type: 'initial_data',
    ...metrics,
    finnhubConnected: finnhubConnected
  }));
  
  ws.on('close', () => {
    dashboardClients = dashboardClients.filter(c => c !== ws);
    console.log('[WS] Dashboard client disconnected');
  });
  
  ws.on('error', (err) => {
    console.error('[WS] Error:', err.message);
  });
});

// HTTP API endpoints
app.use(express.json());
app.use(express.static('public'));

app.get('/api/portfolio', (req, res) => {
  res.json(getPortfolioMetrics());
});

app.get('/api/market/:symbol', (req, res) => {
  const data = marketData[req.params.symbol];
  res.json(data || { error: 'Not found' });
});

// Broadcast portfolio update every 500ms (2 updates per second)
setInterval(broadcastPortfolioUpdate, 500);

// Run strategy evaluation every 500ms (2x per second)
setInterval(evaluateStrategy, 500);

const PORT = process.env.PORT || 3000;

loadData();
connectFinnhub();

server.listen(PORT, () => {
  console.log(`[ATLAS] 🚀 Server running on http://localhost:${PORT}`);
  console.log(`[ATLAS] Starting capital: $${START_CAPITAL}`);
  console.log(`[ATLAS] Watching ${watchlist.length} stocks`);
});

process.on('SIGINT', () => {
  console.log('[ATLAS] Shutting down...');
  saveData();
  process.exit(0);
});
