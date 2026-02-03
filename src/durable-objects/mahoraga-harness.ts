/**
 * MahoragaHarness - Autonomous Trading Agent Durable Object
 * 
 * A fully autonomous trading agent that runs 24/7 on Cloudflare Workers.
 * This is the "harness" - customize it to match your trading strategy.
 * 
 * ═══════════════════════════════════════════════════════════════════════════
 * HOW TO CUSTOMIZE THIS AGENT
 * ═══════════════════════════════════════════════════════════════════════════
 * 
 * 1. CONFIGURATION (AgentConfig & DEFAULT_CONFIG)
 *    - Tune risk parameters, position sizes, thresholds
 *    - Enable/disable features (options, crypto, staleness)
 *    - Set LLM models and token limits
 * 
 * 2. DATA SOURCES (runDataGatherers, gatherStockTwits, gatherReddit, etc.)
 *    - Add new data sources (news APIs, alternative data)
 *    - Modify scraping logic and sentiment analysis
 *    - Adjust source weights in SOURCE_CONFIG
 * 
 * 3. TRADING LOGIC (runAnalyst, executeBuy, executeSell)
 *    - Change entry/exit rules
 *    - Modify position sizing formulas
 *    - Add custom indicators
 * 
 * 4. LLM PROMPTS (researchSignal, runPreMarketAnalysis)
 *    - Customize how the AI analyzes signals
 *    - Change research criteria and output format
 * 
 * 5. NOTIFICATIONS (sendDiscordNotification)
 *    - Set DISCORD_WEBHOOK_URL secret to enable
 *    - Modify what triggers notifications
 * 
 * Deploy with: wrangler deploy -c wrangler.v2.toml
 * ═══════════════════════════════════════════════════════════════════════════
 */

import { DurableObject } from "cloudflare:workers";
import OpenAI from "openai";
import type { Env } from "../env.d";
import { createAlpacaProviders } from "../providers/alpaca";
import type { Account, Position, MarketClock, Asset, Snapshot, Bar } from "../providers/types";
import {
  buildCryptoSymbolMap,
  cryptoSymbolKey,
  normalizeCryptoSymbol,
  normalizeSymbol,
  toSlashUsdSymbol,
} from "../utils/symbols";

// ============================================================================
// SECTION 1: TYPES & CONFIGURATION
// ============================================================================
// [CUSTOMIZABLE] Modify these interfaces to add new fields for custom data sources.
// [CUSTOMIZABLE] AgentConfig contains ALL tunable parameters - start here!
// ============================================================================

interface AgentConfig {
  // Polling intervals - how often the agent checks for new data
  data_poll_interval_ms: number;   // [TUNE] Default: 30s. Lower = more API calls
  analyst_interval_ms: number;     // [TUNE] Default: 120s. How often to run trading logic
  
  // Position limits - risk management basics
  max_position_value: number;      // [TUNE] Max $ per position
  max_positions: number;           // [TUNE] Max concurrent positions
  min_sentiment_score: number;     // [TUNE] Min sentiment to consider buying (0-1)
  min_analyst_confidence: number;  // [TUNE] Min LLM confidence to execute (0-1)
  sell_sentiment_threshold: number; // [TUNE] Sentiment below this triggers sell review
  allowed_exchanges: string[] | null; // [TUNE] Optional allowlist for equity exchanges (null = allow all)
  
  // Risk management - take profit and stop loss
  take_profit_pct: number;         // [TUNE] Take profit at this % gain
  stop_loss_pct: number;           // [TUNE] Stop loss at this % loss
  position_size_pct_of_cash: number; // [TUNE] % of cash per trade
  
  // Stale position management - exit positions that have lost momentum
  stale_position_enabled: boolean;
  stale_min_hold_hours: number;    // [TUNE] Min hours before checking staleness
  stale_max_hold_days: number;     // [TUNE] Force exit after this many days
  stale_min_gain_pct: number;      // [TUNE] Required gain % to hold past max days
  stale_mid_hold_days: number;
  stale_mid_min_gain_pct: number;
  stale_social_volume_decay: number; // [TUNE] Exit if volume drops to this % of entry
  stale_no_mentions_hours: number;   // [TUNE] Exit if no mentions for N hours
  
  // LLM configuration
  llm_model: string;               // [TUNE] Model for quick research (gpt-4o-mini)
  llm_analyst_model: string;       // [TUNE] Model for deep analysis (gpt-4o)
  llm_max_tokens: number;
  
  // Options trading - trade options instead of shares for high-conviction plays
  options_enabled: boolean;        // [TOGGLE] Enable/disable options trading
  options_min_confidence: number;  // [TUNE] Higher threshold for options (riskier)
  options_max_pct_per_trade: number;
  options_max_total_exposure: number;
  options_min_dte: number;         // [TUNE] Minimum days to expiration
  options_max_dte: number;         // [TUNE] Maximum days to expiration
  options_target_delta: number;    // [TUNE] Target delta (0.3-0.5 typical)
  options_min_delta: number;
  options_max_delta: number;
  options_stop_loss_pct: number;   // [TUNE] Options stop loss (wider than stocks)
  options_take_profit_pct: number; // [TUNE] Options take profit (higher targets)
  options_max_positions: number;
  
  // Crypto trading - 24/7 momentum-based crypto trading
  crypto_enabled: boolean;         // [TOGGLE] Enable/disable crypto trading
  crypto_symbols: string[];        // [TUNE] Which cryptos to trade (BTC/USD, etc.)
  crypto_momentum_threshold: number; // [TUNE] Min % move to trigger signal
  crypto_max_position_value: number;
  crypto_take_profit_pct: number;
  crypto_stop_loss_pct: number;
  crypto_min_analyst_confidence: number; // [TUNE] Lower threshold for crypto-only trades
  crypto_universe_top_n: number;   // [TUNE] Use CoinMarketCap top-N (0 disables)
  crypto_universe_refresh_ms: number; // [TUNE] Refresh cadence for top-N universe

  // Manual watchlist - user-specified stock tickers added to signal cache
  stock_watchlist_symbols: string[];

  // Alpha scan - quantitative filters + edge scoring
  alpha_scan_enabled: boolean;
  alpha_scan_interval_ms: number;
  alpha_scan_max_markets: number;
  alpha_min_notional_volume: number;
  alpha_max_spread_pct: number;
  alpha_min_edge: number;
  alpha_edge_threshold: number;
  alpha_bars_lookback: number;
}

// [CUSTOMIZABLE] Add fields here when you add new data sources
interface Signal {
  symbol: string;
  source: string;           // e.g., "stocktwits", "reddit", "crypto", "your_source"
  source_detail: string;    // e.g., "reddit_wallstreetbets"
  sentiment: number;        // Weighted sentiment (-1 to 1)
  raw_sentiment: number;    // Raw sentiment before weighting
  volume: number;           // Number of mentions/messages
  freshness: number;        // Time decay factor (0-1)
  source_weight: number;    // How much to trust this source
  reason: string;           // Human-readable reason
  upvotes?: number;
  comments?: number;
  quality_score?: number;
  subreddits?: string[];
  best_flair?: string | null;
  bullish?: number;
  bearish?: number;
  isCrypto?: boolean;
  momentum?: number;
  price?: number;
}

interface PositionEntry {
  symbol: string;
  entry_time: number;
  entry_price: number;
  entry_sentiment: number;
  entry_social_volume: number;
  entry_sources: string[];
  entry_reason: string;
  peak_price: number;
  peak_sentiment: number;
}

interface SocialHistoryEntry {
  timestamp: number;
  volume: number;
  sentiment: number;
}

interface LogEntry {
  timestamp: string;
  agent: string;
  action: string;
  [key: string]: unknown;
}

interface CostTracker {
  total_usd: number;
  calls: number;
  tokens_in: number;
  tokens_out: number;
}

interface ResearchResult {
  symbol: string;
  verdict: "BUY" | "SKIP" | "WAIT";
  confidence: number;
  entry_quality: "excellent" | "good" | "fair" | "poor";
  reasoning: string;
  red_flags: string[];
  catalysts: string[];
  timestamp: number;
}

interface TwitterConfirmation {
  symbol: string;
  tweet_count: number;
  sentiment: number;
  confirms_existing: boolean;
  highlights: Array<{ author: string; text: string; likes: number }>;
  timestamp: number;
}

interface PremarketPlan {
  timestamp: number;
  recommendations: Array<{
    action: "BUY" | "SELL" | "HOLD";
    symbol: string;
    confidence: number;
    reasoning: string;
    suggested_size_pct?: number;
  }>;
  market_summary: string;
  high_conviction: string[];
  researched_buys: ResearchResult[];
}

interface AlphaMarket {
  symbol: string;
  isCrypto: boolean;
  notional_volume: number;
  spread_pct: number | null;
  implied_prob: number;
  calculated_prob: number;
  alpha: number;
}

interface AlphaScanState {
  updated_at: number;
  total: number;
  volume_pass: number;
  liquidity_pass: number;
  edge_pass: number;
  edge_candidates: AlphaMarket[];
  top_alpha: AlphaMarket[];
}

interface AgentState {
  config: AgentConfig;
  signalCache: Signal[];
  positionEntries: Record<string, PositionEntry>;
  socialHistory: Record<string, SocialHistoryEntry[]>;
  logs: LogEntry[];
  costTracker: CostTracker;
  lastDataGatherRun: number;
  lastAnalystRun: number;
  lastResearchRun: number;
  signalResearch: Record<string, ResearchResult>;
  positionResearch: Record<string, unknown>;
  stalenessAnalysis: Record<string, unknown>;
  twitterConfirmations: Record<string, TwitterConfirmation>;
  twitterDailyReads: number;
  twitterDailyReadReset: number;
  premarketPlan: PremarketPlan | null;
  cryptoUniverseSymbols: string[];
  cryptoUniverseUpdatedAt: number;
  alphaScan: AlphaScanState;
  enabled: boolean;
}

// ============================================================================
// [CUSTOMIZABLE] SOURCE_CONFIG - How much to trust each data source
// ============================================================================
const SOURCE_CONFIG = {
  // [TUNE] Weight each source by reliability (0-1). Higher = more trusted.
  weights: {
    stocktwits: 0.85,           // Decent signal, some noise
    reddit_wallstreetbets: 0.6, // High volume, lots of memes - lower trust
    reddit_stocks: 0.9,         // Higher quality discussions
    reddit_investing: 0.8,      // Long-term focused
    reddit_options: 0.85,       // Options-specific alpha
    twitter_fintwit: 0.95,      // FinTwit has real traders
    twitter_news: 0.9,          // Breaking news accounts
  },
  // [TUNE] Reddit flair multipliers - boost/penalize based on post type
  flairMultipliers: {
    "DD": 1.5,                  // Due Diligence - high value
    "Technical Analysis": 1.3,
    "Fundamentals": 1.3,
    "News": 1.2,
    "Discussion": 1.0,
    "Chart": 1.1,
    "Daily Discussion": 0.7,   // Low signal
    "Weekend Discussion": 0.6,
    "YOLO": 0.6,               // Entertainment, not alpha
    "Gain": 0.5,               // Loss porn - inverse signal?
    "Loss": 0.5,
    "Meme": 0.4,
    "Shitpost": 0.3,
  } as Record<string, number>,
  // [TUNE] Engagement multipliers - more engagement = more trusted
  engagement: {
    upvotes: { 1000: 1.5, 500: 1.3, 200: 1.2, 100: 1.1, 50: 1.0, 0: 0.8 } as Record<number, number>,
    comments: { 200: 1.4, 100: 1.25, 50: 1.15, 20: 1.05, 0: 0.9 } as Record<number, number>,
  },
  // [TUNE] How fast old posts lose weight (minutes). Lower = faster decay.
  decayHalfLifeMinutes: 120,
};

const DEFAULT_CONFIG: AgentConfig = {
  data_poll_interval_ms: 30_000,
  analyst_interval_ms: 120_000,
  max_position_value: 5000,
  max_positions: 5,
  min_sentiment_score: 0.3,
  min_analyst_confidence: 0.6,
  sell_sentiment_threshold: -0.2,
  allowed_exchanges: null,
  take_profit_pct: 10,
  stop_loss_pct: 5,
  position_size_pct_of_cash: 25,
  stale_position_enabled: true,
  stale_min_hold_hours: 24,
  stale_max_hold_days: 3,
  stale_min_gain_pct: 5,
  stale_mid_hold_days: 2,
  stale_mid_min_gain_pct: 3,
  stale_social_volume_decay: 0.3,
  stale_no_mentions_hours: 24,
  llm_model: "gpt-4o-mini",
  llm_analyst_model: "gpt-4o",
  llm_max_tokens: 500,
  options_enabled: false,
  options_min_confidence: 0.8,
  options_max_pct_per_trade: 0.02,
  options_max_total_exposure: 0.10,
  options_min_dte: 30,
  options_max_dte: 60,
  options_target_delta: 0.45,
  options_min_delta: 0.30,
  options_max_delta: 0.70,
  options_stop_loss_pct: 50,
  options_take_profit_pct: 100,
  options_max_positions: 3,
  crypto_enabled: false,
  crypto_symbols: ["BTC/USD", "ETH/USD", "SOL/USD"],
  crypto_momentum_threshold: 2.0,
  crypto_max_position_value: 1000,
  crypto_take_profit_pct: 10,
  crypto_stop_loss_pct: 5,
  crypto_min_analyst_confidence: 0.5,
  crypto_universe_top_n: 100,
  crypto_universe_refresh_ms: 300_000,
  stock_watchlist_symbols: [],
  alpha_scan_enabled: true,
  alpha_scan_interval_ms: 300_000,
  alpha_scan_max_markets: 1000,
  alpha_min_notional_volume: 1_000_000,
  alpha_max_spread_pct: 0.01,
  alpha_min_edge: 0.08,
  alpha_edge_threshold: 0.2,
  alpha_bars_lookback: 60,
};

const DEFAULT_STATE: AgentState = {
  config: DEFAULT_CONFIG,
  signalCache: [],
  positionEntries: {},
  socialHistory: {},
  logs: [],
  costTracker: { total_usd: 0, calls: 0, tokens_in: 0, tokens_out: 0 },
  lastDataGatherRun: 0,
  lastAnalystRun: 0,
  lastResearchRun: 0,
  signalResearch: {},
  positionResearch: {},
  stalenessAnalysis: {},
  twitterConfirmations: {},
  twitterDailyReads: 0,
  twitterDailyReadReset: 0,
  premarketPlan: null,
  cryptoUniverseSymbols: [],
  cryptoUniverseUpdatedAt: 0,
  alphaScan: {
    updated_at: 0,
    total: 0,
    volume_pass: 0,
    liquidity_pass: 0,
    edge_pass: 0,
    edge_candidates: [],
    top_alpha: [],
  },
  enabled: false,
};

// Blacklist for ticker extraction
const TICKER_BLACKLIST = new Set([
  "CEO", "CFO", "IPO", "EPS", "GDP", "SEC", "FDA", "USA", "USD", "ETF",
  "ATH", "ATL", "IMO", "FOMO", "YOLO", "DD", "TA", "THE", "AND", "FOR",
  "ARE", "BUT", "NOT", "YOU", "ALL", "CAN", "HER", "WAS", "ONE", "OUR",
  "WSB", "RIP", "LOL", "OMG", "WTF", "FUD", "HODL", "APE", "GME", "AMC",
]);

// ============================================================================
// SECTION 2: HELPER FUNCTIONS
// ============================================================================
// [CUSTOMIZABLE] These utilities calculate sentiment weights and extract tickers.
// Modify these to change how posts are scored and filtered.
// ============================================================================

/**
 * [TUNE] Time decay - how quickly old posts lose weight
 * Uses exponential decay with half-life from SOURCE_CONFIG.decayHalfLifeMinutes
 * Modify the min/max clamp values (0.2-1.0) to change bounds
 */
function calculateTimeDecay(postTimestamp: number): number {
  const ageMinutes = (Date.now() - postTimestamp * 1000) / 60000;
  const halfLife = SOURCE_CONFIG.decayHalfLifeMinutes;
  const decay = Math.pow(0.5, ageMinutes / halfLife);
  return Math.max(0.2, Math.min(1.0, decay));
}

function getEngagementMultiplier(upvotes: number, comments: number): number {
  let upvoteMultiplier = 0.8;
  const upvoteThresholds = Object.entries(SOURCE_CONFIG.engagement.upvotes)
    .sort(([a], [b]) => Number(b) - Number(a));
  for (const [threshold, mult] of upvoteThresholds) {
    if (upvotes >= parseInt(threshold)) {
      upvoteMultiplier = mult;
      break;
    }
  }
  
  let commentMultiplier = 0.9;
  const commentThresholds = Object.entries(SOURCE_CONFIG.engagement.comments)
    .sort(([a], [b]) => Number(b) - Number(a));
  for (const [threshold, mult] of commentThresholds) {
    if (comments >= parseInt(threshold)) {
      commentMultiplier = mult;
      break;
    }
  }
  
  return (upvoteMultiplier + commentMultiplier) / 2;
}

/** [TUNE] Flair multiplier - boost/penalize based on Reddit post flair */
function getFlairMultiplier(flair: string | null | undefined): number {
  if (!flair) return 1.0;
  return SOURCE_CONFIG.flairMultipliers[flair.trim()] || 1.0;
}

/**
 * [CUSTOMIZABLE] Ticker extraction - modify regex to change what counts as a ticker
 * Current: $SYMBOL or SYMBOL followed by trading keywords
 * Add patterns for your data sources (e.g., cashtags, mentions)
 */
function extractTickers(text: string): string[] {
  const matches = new Set<string>();
  const regex = /\$([A-Z]{1,5})\b|\b([A-Z]{2,5})\b(?=\s+(?:calls?|puts?|stock|shares?|moon|rocket|yolo|buy|sell|long|short))/gi;
  let match;
  while ((match = regex.exec(text)) !== null) {
    const ticker = (match[1] || match[2] || "").toUpperCase();
    if (ticker.length >= 2 && ticker.length <= 5 && !TICKER_BLACKLIST.has(ticker)) {
      matches.add(ticker);
    }
  }
  return Array.from(matches);
}

/**
 * [CUSTOMIZABLE] Sentiment detection - keyword-based bullish/bearish scoring
 * Add/remove words to match your trading style
 * Returns -1 (bearish) to +1 (bullish)
 */
function detectSentiment(text: string): number {
  const lower = text.toLowerCase();
  const bullish = ["moon", "rocket", "buy", "calls", "long", "bullish", "yolo", "tendies", "gains", "diamond", "squeeze", "pump", "green", "up", "breakout", "undervalued", "accumulate"];
  const bearish = ["puts", "short", "sell", "bearish", "crash", "dump", "drill", "tank", "rip", "red", "down", "bag", "overvalued", "bubble", "avoid"];
  
  let bull = 0, bear = 0;
  for (const w of bullish) if (lower.includes(w)) bull++;
  for (const w of bearish) if (lower.includes(w)) bear++;
  
  const total = bull + bear;
  if (total === 0) return 0;
  return (bull - bear) / total;
}

// ============================================================================
// SECTION 3: DURABLE OBJECT CLASS
// ============================================================================
// The main agent class. Modify alarm() to change the core loop.
// Add new HTTP endpoints in fetch() for custom dashboard controls.
// ============================================================================

export class MahoragaHarness extends DurableObject<Env> {
  private state: AgentState = { ...DEFAULT_STATE };
  private _openai: OpenAI | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    
    if (env.OPENAI_API_KEY) {
      const openaiConfig: { apiKey: string; baseURL?: string } = { apiKey: env.OPENAI_API_KEY };
      if (env.OPENAI_BASE_URL) {
        openaiConfig.baseURL = env.OPENAI_BASE_URL;
      }
      this._openai = new OpenAI(openaiConfig);
      console.log("[MahoragaHarness] OpenAI initialized");
    } else {
      console.log("[MahoragaHarness] WARNING: OPENAI_API_KEY not found - research disabled");
    }
    
    this.ctx.blockConcurrencyWhile(async () => {
      const stored = await this.ctx.storage.get<AgentState>("state");
      if (stored) {
        this.state = {
          ...DEFAULT_STATE,
          ...stored,
          config: { ...DEFAULT_CONFIG, ...stored.config },
        };
        const storedAlpha = stored.alphaScan as unknown as Record<string, unknown> | undefined;
        if (storedAlpha && !("edge_candidates" in storedAlpha)) {
          this.state.alphaScan.edge_candidates = [];
        }
      }
    });
  }

  // ============================================================================
  // [CUSTOMIZABLE] ALARM HANDLER - Main entry point for scheduled work
  // ============================================================================
  // This runs every 30 seconds. Modify to change:
  // - What runs and when (intervals, market hours checks)
  // - Order of operations (data → research → trading)
  // - Add new features (e.g., portfolio rebalancing, alerts)
  // ============================================================================

  async alarm(): Promise<void> {
    if (!this.state.enabled) {
      this.log("System", "alarm_skipped", { reason: "Agent not enabled" });
      return;
    }

    const now = Date.now();
    const RESEARCH_INTERVAL_MS = 120_000;
    const POSITION_RESEARCH_INTERVAL_MS = 300_000;
    
    try {
      const alpaca = createAlpacaProviders(this.env);
      const clock = await alpaca.trading.getClock();
      const isPremarket = this.isPreMarketWindow();
      const shouldScanEquities = clock.is_open || isPremarket;
      const shouldScanCrypto = this.state.config.crypto_enabled;
      
      if (shouldScanCrypto) {
        await this.refreshCryptoUniverse(alpaca);
      }

      if (now - this.state.lastDataGatherRun >= this.state.config.data_poll_interval_ms) {
        await this.runDataGatherers({
          scanEquities: shouldScanEquities,
          scanCrypto: shouldScanCrypto,
        });
        this.state.lastDataGatherRun = now;
      }
      
      if (now - this.state.lastResearchRun >= RESEARCH_INTERVAL_MS) {
        if (shouldScanEquities) {
          await this.researchTopSignals(5);
        } else {
          this.log("SignalResearch", "skipped_market_closed", { reason: "Equities market closed" });
        }
        this.state.lastResearchRun = now;
      }
      
      if (isPremarket && !this.state.premarketPlan) {
        await this.runPreMarketAnalysis();
      }
      
      const positions = await alpaca.trading.getPositions();
      
      if (this.state.config.crypto_enabled) {
        await this.runCryptoTrading(alpaca, positions);
      }
      
      if (clock.is_open) {
        if (this.isMarketJustOpened() && this.state.premarketPlan) {
          await this.executePremarketPlan();
        }
        
        if (now - this.state.lastAnalystRun >= this.state.config.analyst_interval_ms) {
          await this.runAnalyst();
          this.state.lastAnalystRun = now;
        }

        if (positions.length > 0 && now - this.state.lastResearchRun >= POSITION_RESEARCH_INTERVAL_MS) {
          for (const pos of positions) {
            if (pos.asset_class !== "us_option") {
              await this.researchPosition(pos.symbol, pos);
            }
          }
        }

        if (this.isOptionsEnabled()) {
          const optionsExits = await this.checkOptionsExits(positions);
          for (const exit of optionsExits) {
            await this.executeSell(alpaca, exit.symbol, exit.reason);
          }
        }

        if (this.isTwitterEnabled()) {
          const heldSymbols = positions.map(p => p.symbol);
          const breakingNews = await this.checkTwitterBreakingNews(heldSymbols);
          for (const news of breakingNews) {
            if (news.is_breaking) {
              this.log("System", "twitter_breaking_news", {
                symbol: news.symbol,
                headline: news.headline.slice(0, 100),
              });
            }
          }
        }
      }
      
      await this.persist();
    } catch (error) {
      this.log("System", "alarm_error", { error: String(error) });
    }
    
    await this.scheduleNextAlarm();
  }

  private async scheduleNextAlarm(): Promise<void> {
    const nextRun = Date.now() + 30_000;  // 30 seconds
    await this.ctx.storage.setAlarm(nextRun);
  }

  // ============================================================================
  // HTTP HANDLER (for dashboard/control)
  // ============================================================================
  // Add new endpoints here for custom dashboard controls.
  // Example: /webhook for external alerts, /backtest for simulation
  // ============================================================================

  private constantTimeCompare(a: string, b: string): boolean {
    if (a.length !== b.length) return false;
    let mismatch = 0;
    for (let i = 0; i < a.length; i++) {
      mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
    }
    return mismatch === 0;
  }

  private isAuthorized(request: Request): boolean {
    const token = this.env.MAHORAGA_API_TOKEN;
    if (!token) {
      console.warn("[MahoragaHarness] MAHORAGA_API_TOKEN not set - denying request");
      return false;
    }
    const authHeader = request.headers.get("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return false;
    }
    return this.constantTimeCompare(authHeader.slice(7), token);
  }

  private isKillSwitchAuthorized(request: Request): boolean {
    const secret = this.env.KILL_SWITCH_SECRET;
    if (!secret) {
      return false;
    }
    const authHeader = request.headers.get("Authorization");
    if (!authHeader?.startsWith("Bearer ")) {
      return false;
    }
    return this.constantTimeCompare(authHeader.slice(7), secret);
  }

  private unauthorizedResponse(): Response {
    return new Response(
      JSON.stringify({ error: "Unauthorized. Requires: Authorization: Bearer <MAHORAGA_API_TOKEN>" }),
      { status: 401, headers: { "Content-Type": "application/json" } }
    );
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const action = url.pathname.slice(1);

    const protectedActions = ["enable", "disable", "config", "trigger", "status", "logs", "costs", "signals", "setup/status", "positions/close"];
    if (protectedActions.includes(action)) {
      if (!this.isAuthorized(request)) {
        return this.unauthorizedResponse();
      }
    }

    try {
      switch (action) {
        case "status":
          return this.handleStatus();
        
        case "setup/status":
          return this.jsonResponse({ ok: true, data: { configured: true } });
        
        case "config":
          if (request.method === "POST") {
            return this.handleUpdateConfig(request);
          }
          return this.jsonResponse({ ok: true, data: this.state.config });
        
        case "enable":
          return this.handleEnable();
        
        case "disable":
          return this.handleDisable();
        
        case "logs":
          return this.handleGetLogs(url);
        
        case "costs":
          return this.jsonResponse({ costs: this.state.costTracker });
        
        case "signals":
          return this.jsonResponse({ signals: this.state.signalCache });
        
        case "trigger":
          await this.alarm();
          return this.jsonResponse({ ok: true, message: "Alarm triggered" });

        case "positions/close":
          if (request.method !== "POST") {
            return new Response("Method not allowed", { status: 405 });
          }
          return this.handleClosePosition(request);
        
        case "kill":
          if (!this.isKillSwitchAuthorized(request)) {
            return new Response(
              JSON.stringify({ error: "Forbidden. Requires: Authorization: Bearer <KILL_SWITCH_SECRET>" }),
              { status: 403, headers: { "Content-Type": "application/json" } }
            );
          }
          return this.handleKillSwitch();
        
        default:
          return new Response("Not found", { status: 404 });
      }
    } catch (error) {
      return new Response(
        JSON.stringify({ error: String(error) }),
        { status: 500, headers: { "Content-Type": "application/json" } }
      );
    }
  }

  private async handleStatus(): Promise<Response> {
    const alpaca = createAlpacaProviders(this.env);
    
    let account: Account | null = null;
    let positions: Position[] = [];
    let clock: MarketClock | null = null;
    
    try {
      [account, positions, clock] = await Promise.all([
        alpaca.trading.getAccount(),
        alpaca.trading.getPositions(),
        alpaca.trading.getClock(),
      ]);
    } catch (e) {
      // Ignore - will return null
    }
    
    return this.jsonResponse({
      ok: true,
      data: {
        enabled: this.state.enabled,
        account,
        positions,
        clock,
        config: this.state.config,
        signals: this.state.signalCache,
        logs: this.state.logs.slice(-100),
        costs: this.state.costTracker,
        lastAnalystRun: this.state.lastAnalystRun,
        lastResearchRun: this.state.lastResearchRun,
        signalResearch: this.state.signalResearch,
        positionResearch: this.state.positionResearch,
        positionEntries: this.state.positionEntries,
        twitterConfirmations: this.state.twitterConfirmations,
        premarketPlan: this.state.premarketPlan,
        stalenessAnalysis: this.state.stalenessAnalysis,
        alphaScan: this.state.alphaScan,
        cryptoUniverse: {
          provider: this.env.COINMARKETCAP_API_KEY ? "coinmarketcap" : "coinpaprika",
          enabled: this.state.config.crypto_enabled && (this.state.config.crypto_universe_top_n ?? 0) > 0,
          top_n: this.state.config.crypto_universe_top_n ?? 0,
          refresh_ms: this.state.config.crypto_universe_refresh_ms ?? 0,
          last_updated_at: this.state.cryptoUniverseUpdatedAt,
          size: this.state.cryptoUniverseSymbols.length,
        },
      },
    });
  }

  private async handleUpdateConfig(request: Request): Promise<Response> {
    const body = await request.json() as Partial<AgentConfig>;
    this.state.config = { ...this.state.config, ...body };
    await this.persist();
    return this.jsonResponse({ ok: true, config: this.state.config });
  }

  private async handleEnable(): Promise<Response> {
    this.state.enabled = true;
    await this.persist();
    await this.scheduleNextAlarm();
    this.log("System", "agent_enabled", {});
    return this.jsonResponse({ ok: true, enabled: true });
  }

  private async handleDisable(): Promise<Response> {
    this.state.enabled = false;
    await this.ctx.storage.deleteAlarm();
    await this.persist();
    this.log("System", "agent_disabled", {});
    return this.jsonResponse({ ok: true, enabled: false });
  }

  private handleGetLogs(url: URL): Response {
    const limit = parseInt(url.searchParams.get("limit") || "100");
    const logs = this.state.logs.slice(-limit);
    return this.jsonResponse({ logs });
  }

  private async handleClosePosition(request: Request): Promise<Response> {
    const body = await request.json() as { symbol?: string; reason?: string };
    const symbol = body.symbol ? normalizeSymbol(body.symbol) : null;

    if (!symbol) {
      return new Response(
        JSON.stringify({ ok: false, error: "symbol is required" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    const alpaca = createAlpacaProviders(this.env);
    const reason = body.reason ? `Manual close: ${body.reason}` : "Manual close";

    const resolved = await this.resolveAssetClass(alpaca, symbol);
    if (!resolved.asset) {
      return this.jsonResponse({ ok: false, error: "Asset not found", symbol });
    }

    if (!resolved.isCrypto) {
      const clock = await alpaca.trading.getClock().catch(() => null);
      if (clock && !clock.is_open) {
        return this.jsonResponse({ ok: false, error: "Market closed", symbol });
      }
    }

    const candidateSymbols = new Set<string>([
      symbol,
      resolved.symbol,
      normalizeSymbol(resolved.asset.symbol),
    ]);

    const result = await this.closePositionWithFallback(alpaca, Array.from(candidateSymbols), reason);
    await this.persist();

    return this.jsonResponse({ ok: result.ok, symbol: result.symbol ?? symbol, error: result.error });
  }

  private async handleKillSwitch(): Promise<Response> {
    this.state.enabled = false;
    await this.ctx.storage.deleteAlarm();
    this.state.signalCache = [];
    this.state.signalResearch = {};
    this.state.premarketPlan = null;
    await this.persist();
    this.log("System", "kill_switch_activated", { timestamp: new Date().toISOString() });
    return this.jsonResponse({ 
      ok: true, 
      message: "KILL SWITCH ACTIVATED. Agent disabled, alarms cancelled, signal cache cleared.",
      note: "Existing positions are NOT automatically closed. Review and close manually if needed."
    });
  }

  // ============================================================================
  // SECTION 4: DATA GATHERING
  // ============================================================================
  // [CUSTOMIZABLE] This is where you add NEW DATA SOURCES.
  // 
  // To add a new source:
  // 1. Create a new gather method (e.g., gatherNewsAPI)
  // 2. Add it to runDataGatherers() Promise.all
  // 3. Add source weight to SOURCE_CONFIG.weights
  // 4. Return Signal[] with your source name
  //
  // Each gatherer returns Signal[] which get merged into signalCache.
  // ============================================================================

  private async runDataGatherers(options: { scanEquities: boolean; scanCrypto: boolean }): Promise<void> {
    this.log("System", "gathering_data", {
      scan_equities: options.scanEquities,
      scan_crypto: options.scanCrypto,
    });

    let stocktwitsSignals: Signal[] = [];
    let redditSignals: Signal[] = [];
    let cryptoSignals: Signal[] = [];
    const manualSignals = this.buildManualWatchlistSignals();

    if (options.scanEquities) {
      [stocktwitsSignals, redditSignals] = await Promise.all([
        this.gatherStockTwits(),
        this.gatherReddit(),
      ]);
    }

    if (options.scanCrypto) {
      cryptoSignals = await this.gatherCrypto();
    }

    const mergedSignals = [...stocktwitsSignals, ...redditSignals, ...cryptoSignals, ...manualSignals];
    const filteredSignals = await this.prefilterSignals(mergedSignals);
    this.state.signalCache = filteredSignals;
    await this.runAlphaScan();
    
    this.log("System", "data_gathered", {
      stocktwits: stocktwitsSignals.length,
      reddit: redditSignals.length,
      crypto: cryptoSignals.length,
      manual: manualSignals.length,
      total: this.state.signalCache.length,
      filtered_out: mergedSignals.length - filteredSignals.length,
    });
  }

  private getExchangeAllowlist(): Set<string> | null {
    const allowlist = (this.state.config.allowed_exchanges ?? [])
      .map(entry => entry.trim().toUpperCase())
      .filter(Boolean);
    if (allowlist.length === 0) return null;
    return new Set(allowlist);
  }

  private async prefilterSignals(signals: Signal[]): Promise<Signal[]> {
    if (signals.length === 0) return signals;

    const alpaca = createAlpacaProviders(this.env);
    const allowlist = this.getExchangeAllowlist();
    const activeCryptoSymbols = this.getActiveCryptoSymbols();
    const cryptoAllowlist = new Set<string>();
    for (const symbol of activeCryptoSymbols) {
      const normalized = normalizeSymbol(symbol);
      cryptoAllowlist.add(cryptoSymbolKey(normalized));
      cryptoAllowlist.add(this.getCryptoBaseSymbol(normalized));
    }
    const decisions = new Map<string, { allowed: boolean; symbol: string; isCrypto: boolean }>();
    const filtered: Signal[] = [];

    for (const signal of signals) {
      const cacheKey = cryptoSymbolKey(signal.symbol);
      const baseKey = this.getCryptoBaseSymbol(signal.symbol);
      let decision = decisions.get(cacheKey);

      if (cryptoAllowlist.has(cacheKey) || cryptoAllowlist.has(baseKey)) {
        filtered.push({
          ...signal,
          symbol: normalizeCryptoSymbol(signal.symbol, activeCryptoSymbols),
          isCrypto: true,
        });
        continue;
      }

      if (signal.isCrypto) {
        this.log("SignalFilter", "crypto_not_allowed", { symbol: signal.symbol });
        continue;
      }

      if (!decision) {
        const resolved = await this.resolveAssetClass(alpaca, signal.symbol);

        if (resolved.isCrypto) {
          const resolvedKey = cryptoSymbolKey(normalizeCryptoSymbol(resolved.symbol, activeCryptoSymbols));
          const resolvedBase = this.getCryptoBaseSymbol(resolved.symbol);
          if (!cryptoAllowlist.has(resolvedKey) && !cryptoAllowlist.has(resolvedBase)) {
            this.log("SignalFilter", "crypto_not_allowed", { symbol: resolved.symbol });
            decision = { allowed: false, symbol: resolved.symbol, isCrypto: true };
            decisions.set(cacheKey, decision);
            continue;
          }
        }

        if (!resolved.asset) {
          this.log("SignalFilter", "asset_unavailable", { symbol: resolved.symbol });
          decision = { allowed: false, symbol: resolved.symbol, isCrypto: resolved.isCrypto };
        } else if (!resolved.asset.tradable) {
          this.log("SignalFilter", "asset_not_tradable", {
            symbol: resolved.symbol,
            tradable: resolved.asset.tradable,
            status: resolved.asset.status,
          });
          decision = { allowed: false, symbol: resolved.symbol, isCrypto: resolved.isCrypto };
        } else if (allowlist && !resolved.isCrypto && !allowlist.has(resolved.asset.exchange.toUpperCase())) {
          this.log("SignalFilter", "asset_exchange_blocked", {
            symbol: resolved.symbol,
            exchange: resolved.asset.exchange,
            allowlist: Array.from(allowlist),
          });
          decision = { allowed: false, symbol: resolved.symbol, isCrypto: resolved.isCrypto };
        } else {
          decision = {
            allowed: true,
            symbol: normalizeSymbol(resolved.asset.symbol),
            isCrypto: resolved.isCrypto,
          };
        }

        decisions.set(cacheKey, decision);
      }

      if (decision.allowed) {
        filtered.push({
          ...signal,
          symbol: decision.symbol,
          isCrypto: signal.isCrypto ?? decision.isCrypto,
        });
      }
    }

    return filtered;
  }

  private getManualWatchlistSymbols(): string[] {
    const symbols = this.state.config.stock_watchlist_symbols ?? [];
    const unique = new Set(
      symbols
        .map((symbol) => normalizeSymbol(symbol))
        .filter(Boolean)
    );
    return Array.from(unique);
  }

  private buildManualWatchlistSignals(): Signal[] {
    const symbols = this.getManualWatchlistSymbols();
    if (symbols.length === 0) return [];

    const baseSentiment = Math.max(0.05, this.state.config.min_sentiment_score * 0.75);
    return symbols.map((symbol) => ({
      symbol,
      source: "manual",
      source_detail: "manual_watchlist",
      sentiment: baseSentiment,
      raw_sentiment: baseSentiment,
      volume: 0,
      freshness: 1.0,
      source_weight: 1.0,
      reason: "Manual watchlist entry",
      bullish: 0,
      bearish: 0,
    }));
  }

  private clampProbability(value: number): number {
    if (!Number.isFinite(value)) return 0.5;
    return Math.max(0, Math.min(1, value));
  }

  private computeAvgAbsReturn(bars: Bar[]): number | null {
    if (bars.length < 2) return null;
    const returns: number[] = [];
    for (let i = 1; i < bars.length; i++) {
      const prevClose = bars[i - 1]!.c;
      const close = bars[i]!.c;
      if (!prevClose) continue;
      returns.push(Math.abs((close - prevClose) / prevClose));
    }
    if (returns.length === 0) return null;
    return returns.reduce((sum, value) => sum + value, 0) / returns.length;
  }

  private computeUpDaysProbability(bars: Bar[]): number | null {
    if (bars.length < 2) return null;
    let upDays = 0;
    let total = 0;
    for (let i = 1; i < bars.length; i++) {
      const prevClose = bars[i - 1]!.c;
      const close = bars[i]!.c;
      if (!prevClose) continue;
      total += 1;
      if (close > prevClose) {
        upDays += 1;
      }
    }
    if (total === 0) return null;
    return upDays / total;
  }

  private computeImpliedProbability(dailyReturn: number, avgAbsReturn: number): number {
    if (!Number.isFinite(avgAbsReturn) || avgAbsReturn <= 0) {
      return 0.5;
    }
    const normalized = dailyReturn / (2 * avgAbsReturn);
    const clamped = Math.max(-0.5, Math.min(0.5, normalized));
    return this.clampProbability(0.5 + clamped);
  }

  private computeAlphaConfidence(alpha: number, thresholdOverride?: number): number {
    const threshold = (thresholdOverride ?? this.state.config.alpha_edge_threshold) ?? 0.2;
    const span = Math.max(0.05, 1 - threshold);
    const scaled = (alpha - threshold) / span;
    return this.clampProbability(0.5 + scaled * 0.5);
  }

  private async runAlphaScan(): Promise<void> {
    if (!this.state.config.alpha_scan_enabled) return;

    const now = Date.now();
    const interval = this.state.config.alpha_scan_interval_ms || 300_000;
    if (this.state.alphaScan.updated_at && now - this.state.alphaScan.updated_at < interval) {
      return;
    }

    const signals = this.state.signalCache;
    if (signals.length === 0) {
      this.state.alphaScan = {
        updated_at: now,
        total: 0,
        volume_pass: 0,
        liquidity_pass: 0,
        edge_pass: 0,
        edge_candidates: [],
        top_alpha: [],
      };
      return;
    }

    const aggregated = new Map<string, { symbol: string; isCrypto: boolean; sentiment: number; count: number; momentum: number; momentumCount: number }>();
    for (const sig of signals) {
      const symbol = normalizeSymbol(sig.symbol);
      const existing = aggregated.get(symbol);
      if (existing) {
        existing.sentiment += sig.sentiment;
        existing.count += 1;
        if (sig.momentum !== undefined && Number.isFinite(sig.momentum)) {
          existing.momentum += sig.momentum;
          existing.momentumCount += 1;
        }
      } else {
        aggregated.set(symbol, {
          symbol,
          isCrypto: !!sig.isCrypto,
          sentiment: sig.sentiment,
          count: 1,
          momentum: sig.momentum ?? 0,
          momentumCount: sig.momentum !== undefined ? 1 : 0,
        });
      }
    }

    const maxMarkets = this.state.config.alpha_scan_max_markets || 1000;
    const markets = Array.from(aggregated.values())
      .map((entry) => ({
        symbol: entry.symbol,
        isCrypto: entry.isCrypto,
        sentimentAvg: entry.sentiment / entry.count,
        momentumAvg: entry.momentumCount > 0 ? entry.momentum / entry.momentumCount : null,
      }))
      .sort((a, b) => b.sentimentAvg - a.sentimentAvg)
      .slice(0, maxMarkets);

    if (markets.length === 0) {
      this.state.alphaScan = {
        updated_at: now,
        total: 0,
        volume_pass: 0,
        liquidity_pass: 0,
        edge_pass: 0,
        edge_candidates: [],
        top_alpha: [],
      };
      return;
    }

    const alpaca = createAlpacaProviders(this.env);
    const snapshots = new Map<string, Snapshot>();
    const equitySymbols = markets.filter((m) => !m.isCrypto).map((m) => m.symbol);

    const snapshotChunkSize = 100;
    for (let i = 0; i < equitySymbols.length; i += snapshotChunkSize) {
      const chunk = equitySymbols.slice(i, i + snapshotChunkSize);
      try {
        const batch = await alpaca.marketData.getSnapshots(chunk);
        for (const [symbol, snap] of Object.entries(batch)) {
          snapshots.set(normalizeSymbol(symbol), snap);
        }
      } catch (error) {
        this.log("Alpha", "snapshot_batch_failed", { count: chunk.length, error: String(error) });
      }
    }

    for (const market of markets.filter((m) => m.isCrypto)) {
      try {
        const snap = await alpaca.marketData.getCryptoSnapshot(market.symbol);
        snapshots.set(normalizeSymbol(market.symbol), snap);
      } catch (error) {
        this.log("Alpha", "snapshot_failed", { symbol: market.symbol, error: String(error) });
      }
    }

    const minNotional = this.state.config.alpha_min_notional_volume || 0;
    const maxSpread = this.state.config.alpha_max_spread_pct || 0.02;
    const minEdge = this.state.config.alpha_min_edge || 0.08;
    const edgeThreshold = this.state.config.alpha_edge_threshold || 0.2;
    const lookback = this.state.config.alpha_bars_lookback || 60;

    const volumePass: typeof markets = [];
    const liquidityPass: typeof markets = [];
    const edgePass: AlphaMarket[] = [];

    for (const market of markets) {
      const snapshot = snapshots.get(market.symbol);
      if (!snapshot) continue;

      const price = snapshot.latest_trade?.price || snapshot.daily_bar?.c || 0;
      const volume = snapshot.daily_bar?.v || 0;
      const notional = price * volume;

      if (notional < minNotional) continue;
      volumePass.push(market);

      const bid = snapshot.latest_quote?.bid_price || 0;
      const ask = snapshot.latest_quote?.ask_price || 0;
      let spreadPct: number | null = null;
      if (bid > 0 && ask > 0) {
        const mid = (bid + ask) / 2;
        if (mid > 0) {
          spreadPct = (ask - bid) / mid;
        }
      }

      if (spreadPct === null || spreadPct > maxSpread) continue;
      liquidityPass.push(market);
    }

    for (const market of liquidityPass) {
      const snapshot = snapshots.get(market.symbol);
      if (!snapshot) continue;

      const price = snapshot.latest_trade?.price || snapshot.daily_bar?.c || 0;
      const volume = snapshot.daily_bar?.v || 0;
      const notional = price * volume;
      const bid = snapshot.latest_quote?.bid_price || 0;
      const ask = snapshot.latest_quote?.ask_price || 0;
      const mid = (bid + ask) / 2;
      const spreadPct = mid > 0 ? (ask - bid) / mid : null;

      const prevClose = snapshot.prev_daily_bar?.c || 0;
      const close = snapshot.daily_bar?.c || 0;
      const dailyReturn = prevClose ? (close - prevClose) / prevClose : 0;

      let avgAbsReturn = Math.max(Math.abs(dailyReturn), 0.02);
      let calcProb = this.clampProbability(market.sentimentAvg);

      if (market.isCrypto) {
        const momentumAvg = market.momentumAvg ?? 0;
        const momentumEdge = Math.max(-0.4, Math.min(0.4, momentumAvg / 10));
        const momentumProb = this.clampProbability(0.5 + momentumEdge);
        const sentimentProb = this.clampProbability(market.sentimentAvg);
        calcProb = this.clampProbability(0.7 * momentumProb + 0.3 * sentimentProb);
      } else {
        try {
          const bars = await alpaca.marketData.getBars(market.symbol, "1Day", { limit: lookback });
          const barAvgAbs = this.computeAvgAbsReturn(bars);
          const upDaysProb = this.computeUpDaysProbability(bars);
          if (barAvgAbs !== null) {
            avgAbsReturn = barAvgAbs;
          }
          if (upDaysProb !== null) {
            calcProb = this.clampProbability(0.6 * calcProb + 0.4 * upDaysProb);
          }
        } catch (error) {
          this.log("Alpha", "bars_failed", { symbol: market.symbol, error: String(error) });
        }
      }

      const impliedProb = market.isCrypto
        ? 0.5
        : this.computeImpliedProbability(dailyReturn, avgAbsReturn);
      const alpha = calcProb - impliedProb;

      const minEdgeForMarket = market.isCrypto ? Math.min(minEdge, 0.01) : minEdge;
      if (Math.abs(alpha) < minEdgeForMarket) continue;

      edgePass.push({
        symbol: market.symbol,
        isCrypto: market.isCrypto,
        notional_volume: notional,
        spread_pct: spreadPct,
        implied_prob: impliedProb,
        calculated_prob: calcProb,
        alpha,
      });
    }

    const edgeCandidates = edgePass
      .filter((m) => m.alpha > 0)
      .sort((a, b) => b.alpha - a.alpha)
      .slice(0, 25);

    const topAlpha = edgePass
      .filter((m) => m.alpha >= (m.isCrypto ? Math.min(edgeThreshold, 0.03) : edgeThreshold))
      .sort((a, b) => b.alpha - a.alpha)
      .slice(0, 10);

    for (const market of topAlpha) {
      this.log("Alpha", "top_alpha_detected", {
        symbol: market.symbol,
        alpha: Number(market.alpha.toFixed(4)),
        implied_prob: Number(market.implied_prob.toFixed(3)),
        calculated_prob: Number(market.calculated_prob.toFixed(3)),
      });
    }

    this.state.alphaScan = {
      updated_at: now,
      total: markets.length,
      volume_pass: volumePass.length,
      liquidity_pass: liquidityPass.length,
      edge_pass: edgePass.length,
      edge_candidates: edgeCandidates,
      top_alpha: topAlpha,
    };

    this.log("Alpha", "scan_complete", {
      total: markets.length,
      volume_pass: volumePass.length,
      liquidity_pass: liquidityPass.length,
      edge_pass: edgePass.length,
      edge_candidates: edgeCandidates.length,
      top_alpha: topAlpha.length,
    });
  }

  private async gatherStockTwits(): Promise<Signal[]> {
    const signals: Signal[] = [];
    const sourceWeight = SOURCE_CONFIG.weights.stocktwits;
    
    try {
      // Get trending symbols
      const trendingRes = await fetch("https://api.stocktwits.com/api/2/trending/symbols.json");
      if (!trendingRes.ok) return [];
      const trendingData = await trendingRes.json() as { symbols?: Array<{ symbol: string }> };
      const trending = trendingData.symbols || [];
      
      // Get sentiment for top trending
      for (const sym of trending.slice(0, 15)) {
        try {
          const streamRes = await fetch(`https://api.stocktwits.com/api/2/streams/symbol/${sym.symbol}.json?limit=30`);
          if (!streamRes.ok) continue;
          const streamData = await streamRes.json() as { messages?: Array<{ entities?: { sentiment?: { basic?: string } }; created_at?: string }> };
          const messages = streamData.messages || [];
          
          // Analyze sentiment
          let bullish = 0, bearish = 0, totalTimeDecay = 0;
          for (const msg of messages) {
            const sentiment = msg.entities?.sentiment?.basic;
            const msgTime = new Date(msg.created_at || Date.now()).getTime() / 1000;
            const timeDecay = calculateTimeDecay(msgTime);
            totalTimeDecay += timeDecay;
            
            if (sentiment === "Bullish") bullish += timeDecay;
            else if (sentiment === "Bearish") bearish += timeDecay;
          }
          
          const total = messages.length;
          const effectiveTotal = totalTimeDecay || 1;
          const score = effectiveTotal > 0 ? (bullish - bearish) / effectiveTotal : 0;
          const avgFreshness = total > 0 ? totalTimeDecay / total : 0;
          
          if (total >= 5) {
            const weightedSentiment = score * sourceWeight * avgFreshness;
            
            signals.push({
              symbol: sym.symbol,
              source: "stocktwits",
              source_detail: "stocktwits_trending",
              sentiment: weightedSentiment,
              raw_sentiment: score,
              volume: total,
              bullish: Math.round(bullish),
              bearish: Math.round(bearish),
              freshness: avgFreshness,
              source_weight: sourceWeight,
              reason: `StockTwits: ${Math.round(bullish)}B/${Math.round(bearish)}b (${(score * 100).toFixed(0)}%) [fresh:${(avgFreshness * 100).toFixed(0)}%]`,
            });
          }
          
          await this.sleep(200);
        } catch {
          continue;
        }
      }
    } catch (error) {
      this.log("StockTwits", "error", { message: String(error) });
    }
    
    return signals;
  }

  private async gatherReddit(): Promise<Signal[]> {
    const subreddits = ["wallstreetbets", "stocks", "investing", "options"];
    const tickerData = new Map<string, {
      mentions: number;
      weightedSentiment: number;
      rawSentiment: number;
      totalQuality: number;
      upvotes: number;
      comments: number;
      sources: Set<string>;
      bestFlair: string | null;
      bestFlairMult: number;
      freshestPost: number;
    }>();

    for (const sub of subreddits) {
      const sourceWeight = SOURCE_CONFIG.weights[`reddit_${sub}` as keyof typeof SOURCE_CONFIG.weights] || 0.7;
      
      try {
        const res = await fetch(`https://www.reddit.com/r/${sub}/hot.json?limit=25`, {
          headers: { "User-Agent": "Mahoraga/2.0" },
        });
        if (!res.ok) continue;
        const data = await res.json() as { data?: { children?: Array<{ data: { title?: string; selftext?: string; created_utc?: number; ups?: number; num_comments?: number; link_flair_text?: string } }> } };
        const posts = data.data?.children?.map(c => c.data) || [];
        
        for (const post of posts) {
          const text = `${post.title || ""} ${post.selftext || ""}`;
          const tickers = extractTickers(text);
          const rawSentiment = detectSentiment(text);
          
          const timeDecay = calculateTimeDecay(post.created_utc || Date.now() / 1000);
          const engagementMult = getEngagementMultiplier(post.ups || 0, post.num_comments || 0);
          const flairMult = getFlairMultiplier(post.link_flair_text);
          const qualityScore = timeDecay * engagementMult * flairMult * sourceWeight;
          
          for (const ticker of tickers) {
            if (!tickerData.has(ticker)) {
              tickerData.set(ticker, {
                mentions: 0,
                weightedSentiment: 0,
                rawSentiment: 0,
                totalQuality: 0,
                upvotes: 0,
                comments: 0,
                sources: new Set(),
                bestFlair: null,
                bestFlairMult: 0,
                freshestPost: 0,
              });
            }
            const d = tickerData.get(ticker)!;
            d.mentions++;
            d.rawSentiment += rawSentiment;
            d.weightedSentiment += rawSentiment * qualityScore;
            d.totalQuality += qualityScore;
            d.upvotes += post.ups || 0;
            d.comments += post.num_comments || 0;
            d.sources.add(sub);
            
            if (flairMult > d.bestFlairMult) {
              d.bestFlair = post.link_flair_text || null;
              d.bestFlairMult = flairMult;
            }
            
            if ((post.created_utc || 0) > d.freshestPost) {
              d.freshestPost = post.created_utc || 0;
            }
          }
        }
        
        await this.sleep(1000);
      } catch {
        continue;
      }
    }

    const signals: Signal[] = [];
    for (const [symbol, data] of tickerData) {
      if (data.mentions >= 2) {
        const avgRawSentiment = data.rawSentiment / data.mentions;
        const avgQuality = data.totalQuality / data.mentions;
        const finalSentiment = data.totalQuality > 0 
          ? data.weightedSentiment / data.mentions
          : avgRawSentiment * 0.5;
        const freshness = calculateTimeDecay(data.freshestPost);
        
        signals.push({
          symbol,
          source: "reddit",
          source_detail: `reddit_${Array.from(data.sources).join("+")}`,
          sentiment: finalSentiment,
          raw_sentiment: avgRawSentiment,
          volume: data.mentions,
          upvotes: data.upvotes,
          comments: data.comments,
          quality_score: avgQuality,
          freshness,
          best_flair: data.bestFlair,
          subreddits: Array.from(data.sources),
          source_weight: avgQuality,
          reason: `Reddit(${Array.from(data.sources).join(",")}): ${data.mentions} mentions, ${data.upvotes} upvotes, quality:${(avgQuality * 100).toFixed(0)}%`,
        });
      }
    }

    return signals;
  }

  private async gatherCrypto(): Promise<Signal[]> {
    if (!this.state.config.crypto_enabled) return [];
    
    const signals: Signal[] = [];
    const symbols = this.getActiveCryptoSymbols();
    const alpaca = createAlpacaProviders(this.env);
    let stablecoinExcluded = 0;
    
    for (const symbol of symbols) {
      if (this.isStablecoinSymbol(symbol)) {
        stablecoinExcluded += 1;
        continue;
      }
      try {
        const snapshot = await alpaca.marketData.getCryptoSnapshot(symbol);
        if (!snapshot) continue;
        
        const price = snapshot.latest_trade?.price || 0;
        const prevClose = snapshot.prev_daily_bar?.c || 0;
        
        if (!price || !prevClose) continue;
        
        const momentum = ((price - prevClose) / prevClose) * 100;
        const threshold = this.state.config.crypto_momentum_threshold || 2.0;
        const hasSignificantMove = Math.abs(momentum) >= threshold;
        const isBullish = momentum > 0;
        
        const rawSentiment = hasSignificantMove && isBullish ? Math.min(Math.abs(momentum) / 5, 1) : 0.1;
        
        signals.push({
          symbol,
          source: "crypto",
          source_detail: "crypto_momentum",
          sentiment: rawSentiment,
          raw_sentiment: rawSentiment,
          volume: snapshot.daily_bar?.v || 0,
          freshness: 1.0,
          source_weight: 0.8,
          reason: `Crypto: ${momentum >= 0 ? '+' : ''}${momentum.toFixed(2)}% (24h)`,
          bullish: isBullish ? 1 : 0,
          bearish: isBullish ? 0 : 1,
          isCrypto: true,
          momentum,
          price,
        });
        
        await this.sleep(200);
      } catch (error) {
        this.log("Crypto", "error", { symbol, message: String(error) });
      }
    }
    
    this.log("Crypto", "gathered_signals", { count: signals.length, stablecoin_excluded: stablecoinExcluded });
    return signals;
  }

  private async runCryptoTrading(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    positions: Position[]
  ): Promise<void> {
    if (!this.state.config.crypto_enabled) return;
    
    const activeCryptoSymbols = this.getActiveCryptoSymbols();
    const cryptoSymbolKeys = new Set(activeCryptoSymbols.map(cryptoSymbolKey));
    const cryptoPositions = positions.filter(p =>
      p.asset_class === "crypto" || cryptoSymbolKeys.has(cryptoSymbolKey(p.symbol)) || p.symbol.includes("/")
    );
    const heldCrypto = new Set(cryptoPositions.map(p => cryptoSymbolKey(p.symbol)));
    
    for (const pos of cryptoPositions) {
      const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100;
      
      if (plPct >= this.state.config.crypto_take_profit_pct) {
        this.log("Crypto", "take_profit", { symbol: pos.symbol, pnl: plPct.toFixed(2) });
        await this.executeSell(alpaca, pos.symbol, `Crypto take profit at +${plPct.toFixed(1)}%`);
        continue;
      }
      
      if (plPct <= -this.state.config.crypto_stop_loss_pct) {
        this.log("Crypto", "stop_loss", { symbol: pos.symbol, pnl: plPct.toFixed(2) });
        await this.executeSell(alpaca, pos.symbol, `Crypto stop loss at ${plPct.toFixed(1)}%`);
        continue;
      }
    }
    
    const maxCryptoPositions = Math.min(activeCryptoSymbols.length || 3, 3);
    if (cryptoPositions.length >= maxCryptoPositions) {
      this.log("Crypto", "entry_skipped_max_positions", {
        held: cryptoPositions.length,
        max: maxCryptoPositions,
      });
      return;
    }

    const edgeCandidates = this.state.alphaScan.edge_candidates ?? [];
    const alphaCandidates = (edgeCandidates.length > 0
      ? edgeCandidates
      : this.state.alphaScan.top_alpha
    ).filter((entry) => entry.isCrypto);
    if (alphaCandidates.length === 0) {
      this.log("Crypto", "entry_skipped_no_alpha", {
        reason: "No alpha candidates for crypto",
        alpha_total: this.state.alphaScan.top_alpha.length,
        edge_candidates: edgeCandidates.length,
      });
      return;
    }

    const alphaBySymbol = new Map<string, AlphaMarket>();
    for (const entry of alphaCandidates) {
      alphaBySymbol.set(normalizeSymbol(entry.symbol), entry);
    }

    const primaryMomentum = this.state.config.crypto_momentum_threshold || 2.0;
    const fallbackMomentum = Math.max(0.25, primaryMomentum * 0.5);
    let cryptoSignals = this.state.signalCache
      .filter(s => s.isCrypto)
      .filter(s => !heldCrypto.has(cryptoSymbolKey(s.symbol)))
      .filter(s => !this.isStablecoinSymbol(s.symbol))
      .filter(s => (s.momentum ?? 0) >= primaryMomentum)
      .filter(s => alphaBySymbol.has(normalizeSymbol(s.symbol)))
      .sort((a, b) => {
        const alphaA = alphaBySymbol.get(normalizeSymbol(a.symbol))?.alpha ?? 0;
        const alphaB = alphaBySymbol.get(normalizeSymbol(b.symbol))?.alpha ?? 0;
        return alphaB - alphaA;
      });

    if (cryptoSignals.length === 0 && fallbackMomentum < primaryMomentum) {
      cryptoSignals = this.state.signalCache
        .filter(s => s.isCrypto)
        .filter(s => !heldCrypto.has(cryptoSymbolKey(s.symbol)))
        .filter(s => !this.isStablecoinSymbol(s.symbol))
        .filter(s => (s.momentum ?? 0) >= fallbackMomentum)
        .filter(s => alphaBySymbol.has(normalizeSymbol(s.symbol)))
        .sort((a, b) => {
          const alphaA = alphaBySymbol.get(normalizeSymbol(a.symbol))?.alpha ?? 0;
          const alphaB = alphaBySymbol.get(normalizeSymbol(b.symbol))?.alpha ?? 0;
          return alphaB - alphaA;
        });

      if (cryptoSignals.length > 0) {
        this.log("Crypto", "momentum_fallback", {
          primary: primaryMomentum,
          fallback: fallbackMomentum,
          candidates: cryptoSignals.length,
        });
      }
    }

    if (cryptoSignals.length === 0) {
      this.log("Crypto", "entry_skipped_no_signals", {
        total_signals: this.state.signalCache.length,
        held_crypto: heldCrypto.size,
        alpha_crypto: alphaCandidates.length,
      });
      return;
    }

    for (const signal of cryptoSignals) {
      if (cryptoPositions.length >= maxCryptoPositions) break;

      const alphaEntry = alphaBySymbol.get(normalizeSymbol(signal.symbol));
      if (!alphaEntry) {
        this.log("Crypto", "entry_skipped_no_alpha_match", { symbol: signal.symbol });
        continue;
      }

      const cryptoEdgeThreshold = Math.min(this.state.config.alpha_edge_threshold || 0.2, 0.03);
      const alphaConfidence = this.computeAlphaConfidence(alphaEntry.alpha, cryptoEdgeThreshold);
      const research = await this.researchCryptoAlpha(signal, alphaEntry, alphaConfidence);
      if (research && research.verdict !== "BUY") {
        this.log("Crypto", "research_skip", {
          symbol: signal.symbol,
          reason: "llm_veto",
          verdict: research.verdict,
          confidence: research.confidence,
        });
        continue;
      }

      const confidence = research
        ? this.clampProbability((alphaConfidence * 0.6) + (research.confidence * 0.4))
        : alphaConfidence;

      const cryptoMinConfidence = this.state.config.crypto_min_analyst_confidence ?? this.state.config.min_analyst_confidence;
      if (confidence < cryptoMinConfidence) {
        this.log("Crypto", "research_skip", {
          reason: "below_threshold",
          symbol: signal.symbol,
          confidence,
          required: cryptoMinConfidence,
        });
        continue;
      }

      const account = await alpaca.trading.getAccount();
      const resultSymbol = await this.executeCryptoBuy(alpaca, signal.symbol, confidence, account);

      if (resultSymbol) {
        this.log("Crypto", "alpha_trade", {
          symbol: resultSymbol,
          alpha: Number(alphaEntry.alpha.toFixed(4)),
          confidence: Number(confidence.toFixed(3)),
        });
        heldCrypto.add(cryptoSymbolKey(resultSymbol));
        cryptoPositions.push({ symbol: resultSymbol } as Position);
        break;
      }
    }
  }

  private async researchCryptoAlpha(
    signal: Signal,
    alpha: AlphaMarket,
    alphaConfidence: number
  ): Promise<ResearchResult | null> {
    if (!this._openai) {
      this.log("Crypto", "skipped_no_openai", { symbol: signal.symbol, reason: "OPENAI_API_KEY not configured" });
      return null;
    }

    const prompt = `You are reviewing a crypto trade candidate that already passed strict alpha filters.

SYMBOL: ${signal.symbol}
MOMENTUM (24h): ${(signal.momentum ?? 0).toFixed(2)}%
SENTIMENT: ${(signal.sentiment * 100).toFixed(0)}% bullish
ALPHA SCORE: ${alpha.alpha.toFixed(3)}
IMPLIED PROB: ${alpha.implied_prob.toFixed(3)}
CALCULATED PROB: ${alpha.calculated_prob.toFixed(3)}
NOTIONAL VOLUME: $${alpha.notional_volume.toFixed(0)}
SPREAD %: ${alpha.spread_pct !== null ? (alpha.spread_pct * 100).toFixed(2) : "N/A"}

Guidance:
- This candidate already passed volume + liquidity + edge filters.
- Only return SKIP if you see a *clear* red flag.
- Otherwise return BUY with confidence scaled to alpha strength.

Return JSON only:
{
  "verdict": "BUY|SKIP|WAIT",
  "confidence": 0.0-1.0,
  "entry_quality": "excellent|good|fair|poor",
  "reasoning": "brief reason",
  "red_flags": ["any concerns"],
  "catalysts": ["positive factors"]
}`;

    try {
      const response = await this._openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You are a disciplined crypto analyst. Favor BUY when alpha filters are strong unless clear risks exist. Output valid JSON only." },
          { role: "user", content: prompt },
        ],
        max_tokens: 250,
        temperature: 0.2,
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost("gpt-4o-mini", usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.choices[0]?.message?.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        verdict: "BUY" | "SKIP" | "WAIT";
        confidence: number;
        entry_quality: "excellent" | "good" | "fair" | "poor";
        reasoning: string;
        red_flags: string[];
        catalysts: string[];
      };

      const result: ResearchResult = {
        symbol: signal.symbol,
        verdict: analysis.verdict,
        confidence: this.clampProbability(analysis.confidence),
        entry_quality: analysis.entry_quality,
        reasoning: analysis.reasoning,
        red_flags: analysis.red_flags || [],
        catalysts: analysis.catalysts || [],
        timestamp: Date.now(),
      };

      this.state.signalResearch[signal.symbol] = result;
      this.log("Crypto", "researched", {
        symbol: signal.symbol,
        verdict: result.verdict,
        confidence: result.confidence,
        quality: result.entry_quality,
        alpha: Number(alpha.alpha.toFixed(4)),
        alpha_confidence: Number(alphaConfidence.toFixed(3)),
      });

      return result;
    } catch (error) {
      this.log("Crypto", "research_error", { symbol: signal.symbol, error: String(error) });
      return null;
    }
  }

  private async executeCryptoBuy(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbol: string,
    confidence: number,
    account: Account
  ): Promise<string | null> {
    const sizePct = Math.min(20, this.state.config.position_size_pct_of_cash);
    const positionSize = Math.min(
      account.cash * (sizePct / 100) * confidence,
      this.state.config.crypto_max_position_value
    );
    
    if (positionSize < 10) {
      this.log("Crypto", "buy_skipped", { symbol, reason: "Position too small" });
      return null;
    }
    
    try {
      const resolved = await this.resolveAssetClass(alpaca, symbol);
      const orderSymbol = normalizeCryptoSymbol(resolved.symbol, this.getActiveCryptoSymbols());
      const order = await alpaca.trading.createOrder({
        symbol: orderSymbol,
        notional: Math.round(positionSize * 100) / 100,
        side: "buy",
        type: "market",
        time_in_force: "gtc",
      });
      
      this.log("Crypto", "buy_executed", { symbol: orderSymbol, status: order.status, size: positionSize });
      return orderSymbol;
    } catch (error) {
      this.log("Crypto", "buy_failed", { symbol, error: String(error) });
      return null;
    }
  }

  // ============================================================================
  // SECTION 5: TWITTER INTEGRATION
  // ============================================================================
  // [TOGGLE] Enable with TWITTER_BEARER_TOKEN secret
  // [TUNE] MAX_DAILY_READS controls API budget (default: 200/day)
  // 
  // Twitter is used for CONFIRMATION only - it boosts/reduces confidence
  // on signals from other sources, doesn't generate signals itself.
  // ============================================================================

  private isTwitterEnabled(): boolean {
    return !!this.env.TWITTER_BEARER_TOKEN;
  }

  private canSpendTwitterRead(): boolean {
    const ONE_DAY_MS = 86400_000;
    const MAX_DAILY_READS = 200;
    
    const now = Date.now();
    if (now - this.state.twitterDailyReadReset > ONE_DAY_MS) {
      this.state.twitterDailyReads = 0;
      this.state.twitterDailyReadReset = now;
    }
    return this.state.twitterDailyReads < MAX_DAILY_READS;
  }

  private spendTwitterRead(count = 1): void {
    this.state.twitterDailyReads += count;
    this.log("Twitter", "read_spent", {
      count,
      daily_total: this.state.twitterDailyReads,
      budget_remaining: 200 - this.state.twitterDailyReads,
    });
  }

  private async twitterSearchRecent(query: string, maxResults = 10): Promise<Array<{
    id: string;
    text: string;
    created_at: string;
    author: string;
    author_followers: number;
    retweets: number;
    likes: number;
  }>> {
    if (!this.isTwitterEnabled() || !this.canSpendTwitterRead()) return [];

    try {
      const params = new URLSearchParams({
        query,
        max_results: Math.min(maxResults, 10).toString(),
        "tweet.fields": "created_at,public_metrics,author_id",
        expansions: "author_id",
        "user.fields": "username,public_metrics",
      });

      const res = await fetch(`https://api.twitter.com/2/tweets/search/recent?${params}`, {
        headers: {
          Authorization: `Bearer ${this.env.TWITTER_BEARER_TOKEN}`,
          "Content-Type": "application/json",
        },
      });

      if (!res.ok) {
        this.log("Twitter", "api_error", { status: res.status });
        return [];
      }

      const data = await res.json() as {
        data?: Array<{
          id: string;
          text: string;
          created_at: string;
          author_id: string;
          public_metrics?: { retweet_count?: number; like_count?: number };
        }>;
        includes?: {
          users?: Array<{
            id: string;
            username: string;
            public_metrics?: { followers_count?: number };
          }>;
        };
      };

      this.spendTwitterRead(1);

      return (data.data || []).map(tweet => {
        const user = data.includes?.users?.find(u => u.id === tweet.author_id);
        return {
          id: tweet.id,
          text: tweet.text,
          created_at: tweet.created_at,
          author: user?.username || "unknown",
          author_followers: user?.public_metrics?.followers_count || 0,
          retweets: tweet.public_metrics?.retweet_count || 0,
          likes: tweet.public_metrics?.like_count || 0,
        };
      });
    } catch (error) {
      this.log("Twitter", "error", { message: String(error) });
      return [];
    }
  }

  private async gatherTwitterConfirmation(symbol: string, existingSentiment: number): Promise<TwitterConfirmation | null> {
    const MIN_SENTIMENT_FOR_CONFIRMATION = 0.3;
    const CACHE_TTL_MS = 300_000;
    
    if (!this.isTwitterEnabled() || !this.canSpendTwitterRead()) return null;
    if (Math.abs(existingSentiment) < MIN_SENTIMENT_FOR_CONFIRMATION) return null;

    const cached = this.state.twitterConfirmations[symbol];
    if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
      return cached;
    }

    const actionableKeywords = ["unusual", "flow", "sweep", "block", "whale", "breaking", "alert", "upgrade", "downgrade"];
    const query = `$${symbol} (${actionableKeywords.slice(0, 5).join(" OR ")}) -is:retweet lang:en`;
    const tweets = await this.twitterSearchRecent(query, 10);

    if (tweets.length === 0) return null;

    let bullish = 0, bearish = 0, totalWeight = 0;
    const highlights: Array<{ author: string; text: string; likes: number }> = [];

    const bullWords = ["buy", "call", "long", "bullish", "upgrade", "beat", "squeeze", "moon", "breakout"];
    const bearWords = ["sell", "put", "short", "bearish", "downgrade", "miss", "crash", "dump", "breakdown"];

    for (const tweet of tweets) {
      const text = tweet.text.toLowerCase();
      
      const authorWeight = Math.min(1.5, Math.log10(tweet.author_followers + 1) / 5);
      const engagementWeight = Math.min(1.3, 1 + (tweet.likes + tweet.retweets * 2) / 1000);
      const weight = authorWeight * engagementWeight;
      
      let sentiment = 0;
      for (const w of bullWords) if (text.includes(w)) sentiment += 1;
      for (const w of bearWords) if (text.includes(w)) sentiment -= 1;
      
      if (sentiment > 0) bullish += weight;
      else if (sentiment < 0) bearish += weight;
      totalWeight += weight;

      if (tweet.likes > 50 || tweet.author_followers > 10000) {
        highlights.push({
          author: tweet.author,
          text: tweet.text.slice(0, 150),
          likes: tweet.likes,
        });
      }
    }

    const twitterSentiment = totalWeight > 0 ? (bullish - bearish) / totalWeight : 0;
    const twitterBullish = twitterSentiment > 0.2;
    const twitterBearish = twitterSentiment < -0.2;
    const existingBullish = existingSentiment > 0;

    const result: TwitterConfirmation = {
      symbol,
      tweet_count: tweets.length,
      sentiment: twitterSentiment,
      confirms_existing: (twitterBullish && existingBullish) || (twitterBearish && !existingBullish),
      highlights: highlights.slice(0, 3),
      timestamp: Date.now(),
    };

    this.state.twitterConfirmations[symbol] = result;
    this.log("Twitter", "signal_confirmed", {
      symbol,
      sentiment: twitterSentiment.toFixed(2),
      confirms: result.confirms_existing,
      tweet_count: tweets.length,
    });

    return result;
  }

  private async checkTwitterBreakingNews(symbols: string[]): Promise<Array<{
    symbol: string;
    headline: string;
    author: string;
    age_minutes: number;
    is_breaking: boolean;
  }>> {
    if (!this.isTwitterEnabled() || !this.canSpendTwitterRead() || symbols.length === 0) return [];

    const toCheck = symbols.slice(0, 3);
    const newsQuery = `(from:FirstSquawk OR from:DeItaone OR from:Newsquawk) (${toCheck.map(s => `$${s}`).join(" OR ")}) -is:retweet`;
    const tweets = await this.twitterSearchRecent(newsQuery, 5);

    const results: Array<{
      symbol: string;
      headline: string;
      author: string;
      age_minutes: number;
      is_breaking: boolean;
    }> = [];

    const MAX_NEWS_AGE_MS = 1800_000;
    const BREAKING_THRESHOLD_MS = 600_000;
    
    for (const tweet of tweets) {
      const tweetAge = Date.now() - new Date(tweet.created_at).getTime();
      if (tweetAge > MAX_NEWS_AGE_MS) continue;

      const mentionedSymbol = toCheck.find(s =>
        tweet.text.toUpperCase().includes(`$${s}`) ||
        tweet.text.toUpperCase().includes(` ${s} `)
      );

      if (mentionedSymbol) {
        results.push({
          symbol: mentionedSymbol,
          headline: tweet.text.slice(0, 200),
          author: tweet.author,
          age_minutes: Math.round(tweetAge / 60000),
          is_breaking: tweetAge < BREAKING_THRESHOLD_MS,
        });
      }
    }

    if (results.length > 0) {
      this.log("Twitter", "breaking_news_found", {
        count: results.length,
        symbols: results.map(r => r.symbol),
      });
    }

    return results;
  }

  // ============================================================================
  // SECTION 6: LLM RESEARCH
  // ============================================================================
  // [CUSTOMIZABLE] Modify prompts to change how the AI analyzes signals.
  // 
  // Key methods:
  // - researchSignal(): Evaluates individual symbols (BUY/SKIP/WAIT)
  // - researchPosition(): Analyzes held positions (HOLD/SELL/ADD)
  // - analyzeSignalsWithLLM(): Batch analysis for trading decisions
  //
  // [TUNE] Change llm_model and llm_analyst_model in config for cost/quality
  // ============================================================================

  private async researchSignal(
    symbol: string,
    sentimentScore: number,
    sources: string[],
    priceHint?: number
  ): Promise<ResearchResult | null> {
    if (!this._openai) {
      this.log("SignalResearch", "skipped_no_openai", { symbol, reason: "OPENAI_API_KEY not configured" });
      return null;
    }

    const cacheKey = normalizeCryptoSymbol(symbol, this.getActiveCryptoSymbols());
    const cached = this.state.signalResearch[cacheKey];
    const CACHE_TTL_MS = 180_000;
    if (cached && Date.now() - cached.timestamp < CACHE_TTL_MS) {
      return cached;
    }

    try {
      const alpaca = createAlpacaProviders(this.env);
      const resolved = await this.resolveAssetClass(alpaca, symbol);
      const isCrypto = resolved.isCrypto;
      const promptSymbol = resolved.symbol;

      if (!isCrypto) {
        if (!resolved.asset) {
          this.log("SignalResearch", "asset_unavailable", { symbol: promptSymbol });
          return null;
        }
        if (resolved.asset.status !== "active" || !resolved.asset.tradable) {
          this.log("SignalResearch", "asset_not_tradable", {
            symbol: promptSymbol,
            status: resolved.asset.status,
            tradable: resolved.asset.tradable,
          });
          return null;
        }
      }
      let price = priceHint && priceHint > 0 ? priceHint : 0;

      if (price <= 0) {
        if (isCrypto) {
          const snapshot = await alpaca.marketData.getCryptoSnapshot(promptSymbol).catch(() => null);
          price = snapshot?.latest_trade?.price || 0;
        } else {
          const quote = await alpaca.marketData.getQuote(promptSymbol).catch(() => null);
          price = quote?.ask_price || quote?.bid_price || 0;
        }
      }

      if (price <= 0) {
        this.log("SignalResearch", "price_unavailable", { symbol: promptSymbol, isCrypto });
      }


       const prompt = `Should we BUY this ${isCrypto ? "crypto" : "stock"} based on social sentiment and fundamentals?

SYMBOL: ${promptSymbol}
SENTIMENT: ${(sentimentScore * 100).toFixed(0)}% bullish (sources: ${sources.join(", ")})

CURRENT DATA:
- Price: ${price > 0 ? `$${price}` : "unavailable"}

Evaluate if this is a good entry. Consider: Is the sentiment justified? Is it too late (already pumped)? Any red flags?

JSON response:
{
  "verdict": "BUY|SKIP|WAIT",
  "confidence": 0.0-1.0,
  "entry_quality": "excellent|good|fair|poor",
  "reasoning": "brief reason",
  "red_flags": ["any concerns"],
  "catalysts": ["positive factors"]
}`;

      const response = await this._openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You are a stock research analyst. Be skeptical of hype. Output valid JSON only." },
          { role: "user", content: prompt },
        ],
        max_tokens: 250,
        temperature: 0.3,
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost("gpt-4o-mini", usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.choices[0]?.message?.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        verdict: "BUY" | "SKIP" | "WAIT";
        confidence: number;
        entry_quality: "excellent" | "good" | "fair" | "poor";
        reasoning: string;
        red_flags: string[];
        catalysts: string[];
      };

      const result: ResearchResult = {
        symbol: promptSymbol,
        verdict: analysis.verdict,
        confidence: analysis.confidence,
        entry_quality: analysis.entry_quality,
        reasoning: analysis.reasoning,
        red_flags: analysis.red_flags || [],
        catalysts: analysis.catalysts || [],
        timestamp: Date.now(),
      };

      this.state.signalResearch[cacheKey] = result;
      this.log("SignalResearch", "signal_researched", {
        symbol,
        verdict: result.verdict,
        confidence: result.confidence,
        quality: result.entry_quality,
      });

      if (result.verdict === "BUY") {
        await this.sendDiscordNotification("research", {
          symbol: result.symbol,
          verdict: result.verdict,
          confidence: result.confidence,
          quality: result.entry_quality,
          sentiment: sentimentScore,
          sources,
          reasoning: result.reasoning,
          catalysts: result.catalysts,
          red_flags: result.red_flags,
        });
      }

      return result;
    } catch (error) {
      this.log("SignalResearch", "error", { symbol, message: String(error) });
      return null;
    }
  }

  private async researchTopSignals(limit = 5): Promise<ResearchResult[]> {
    const alpaca = createAlpacaProviders(this.env);
    const positions = await alpaca.trading.getPositions();
    const heldSymbols = new Set<string>();
    for (const position of positions) {
      this.addHeldSymbol(heldSymbols, position.symbol);
    }

    const allSignals = this.state.signalCache;
    const notHeld = allSignals.filter(s => !heldSymbols.has(normalizeSymbol(s.symbol)));
    // Use raw_sentiment for threshold (before weighting), weighted sentiment for sorting
    const aboveThreshold = notHeld.filter(s => s.raw_sentiment >= this.state.config.min_sentiment_score);
    const candidates = aboveThreshold
      .sort((a, b) => b.sentiment - a.sentiment)
      .slice(0, limit);

    if (candidates.length === 0) {
      this.log("SignalResearch", "no_candidates", {
        total_signals: allSignals.length,
        not_held: notHeld.length,
        above_threshold: aboveThreshold.length,
        min_sentiment: this.state.config.min_sentiment_score,
      });
      return [];
    }

    this.log("SignalResearch", "researching_signals", { count: candidates.length });

    const aggregated = new Map<string, { symbol: string; sentiment: number; sources: string[]; price?: number }>();
    for (const sig of candidates) {
      if (!aggregated.has(sig.symbol)) {
        aggregated.set(sig.symbol, {
          symbol: sig.symbol,
          sentiment: sig.sentiment,
          sources: [sig.source],
          price: sig.price,
        });
      } else {
        const entry = aggregated.get(sig.symbol)!;
        entry.sources.push(sig.source);
        if (sig.price && sig.price > 0) {
          entry.price = sig.price;
        }
      }
    }

    const results: ResearchResult[] = [];
    for (const [symbol, data] of aggregated) {
      const analysis = await this.researchSignal(symbol, data.sentiment, data.sources, data.price);
      if (analysis) {
        results.push(analysis);
      }
      await this.sleep(500);
    }

    return results;
  }

  private async researchPosition(symbol: string, position: Position): Promise<{
    recommendation: "HOLD" | "SELL" | "ADD";
    risk_level: "low" | "medium" | "high";
    reasoning: string;
    key_factors: string[];
  } | null> {
    if (!this._openai) return null;

    const plPct = (position.unrealized_pl / (position.market_value - position.unrealized_pl)) * 100;

    const prompt = `Analyze this position for risk and opportunity:

POSITION: ${symbol}
- Shares: ${position.qty}
- Market Value: $${position.market_value.toFixed(2)}
- P&L: $${position.unrealized_pl.toFixed(2)} (${plPct.toFixed(1)}%)
- Current Price: $${position.current_price}

Provide a brief risk assessment and recommendation (HOLD, SELL, or ADD). JSON format:
{
  "recommendation": "HOLD|SELL|ADD",
  "risk_level": "low|medium|high",
  "reasoning": "brief reason",
  "key_factors": ["factor1", "factor2"]
}`;

    try {
      const response = await this._openai.chat.completions.create({
        model: "gpt-4o-mini",
        messages: [
          { role: "system", content: "You are a position risk analyst. Be concise. Output valid JSON only." },
          { role: "user", content: prompt },
        ],
        max_tokens: 200,
        temperature: 0.3,
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost("gpt-4o-mini", usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.choices[0]?.message?.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        recommendation: "HOLD" | "SELL" | "ADD";
        risk_level: "low" | "medium" | "high";
        reasoning: string;
        key_factors: string[];
      };

      this.state.positionResearch[symbol] = { ...analysis, timestamp: Date.now() };
      this.log("PositionResearch", "position_analyzed", {
        symbol,
        recommendation: analysis.recommendation,
        risk: analysis.risk_level,
      });

      return analysis;
    } catch (error) {
      this.log("PositionResearch", "error", { symbol, message: String(error) });
      return null;
    }
  }

  private async analyzeSignalsWithLLM(
    signals: Signal[],
    positions: Position[],
    account: Account
  ): Promise<{
    recommendations: Array<{
      action: "BUY" | "SELL" | "HOLD";
      symbol: string;
      confidence: number;
      reasoning: string;
      suggested_size_pct?: number;
    }>;
    market_summary: string;
    high_conviction: string[];
  }> {
    if (!this._openai || signals.length === 0) {
      return { recommendations: [], market_summary: "No signals to analyze", high_conviction: [] };
    }

    const aggregated = new Map<string, { symbol: string; sources: string[]; totalSentiment: number; count: number }>();
    for (const sig of signals) {
      if (!aggregated.has(sig.symbol)) {
        aggregated.set(sig.symbol, { symbol: sig.symbol, sources: [], totalSentiment: 0, count: 0 });
      }
      const agg = aggregated.get(sig.symbol)!;
      agg.sources.push(sig.source);
      agg.totalSentiment += sig.sentiment;
      agg.count++;
    }

    const candidates = Array.from(aggregated.values())
      .map(a => ({ ...a, avgSentiment: a.totalSentiment / a.count }))
      .filter(a => a.avgSentiment >= this.state.config.min_sentiment_score * 0.5)
      .sort((a, b) => b.avgSentiment - a.avgSentiment)
      .slice(0, 10);

    if (candidates.length === 0) {
      return { recommendations: [], market_summary: "No candidates above threshold", high_conviction: [] };
    }

    const positionSymbols = new Set(positions.map(p => p.symbol));
    const prompt = `Current Time: ${new Date().toISOString()}

ACCOUNT STATUS:
- Equity: $${account.equity.toFixed(2)}
- Cash: $${account.cash.toFixed(2)}
- Current Positions: ${positions.length}/${this.state.config.max_positions}

CURRENT POSITIONS:
${positions.length === 0 ? "None" : positions.map(p =>
  `- ${p.symbol}: ${p.qty} shares, P&L: $${p.unrealized_pl.toFixed(2)} (${((p.unrealized_pl / (p.market_value - p.unrealized_pl)) * 100).toFixed(1)}%)`
).join("\n")}

TOP SENTIMENT CANDIDATES:
${candidates.map(c =>
  `- ${c.symbol}: avg sentiment ${(c.avgSentiment * 100).toFixed(0)}%, sources: ${c.sources.join(", ")}, ${positionSymbols.has(c.symbol) ? "[CURRENTLY HELD]" : "[NOT HELD]"}`
).join("\n")}

RAW SIGNALS (top 20):
${signals.slice(0, 20).map(s =>
  `- ${s.symbol} (${s.source}): ${s.reason}`
).join("\n")}

TRADING RULES:
- Max position size: $${this.state.config.max_position_value}
- Take profit target: ${this.state.config.take_profit_pct}%
- Stop loss: ${this.state.config.stop_loss_pct}%
- Min confidence to trade: ${this.state.config.min_analyst_confidence}

Analyze and provide BUY/SELL/HOLD recommendations:`;

    try {
      const response = await this._openai.chat.completions.create({
        model: this.state.config.llm_analyst_model,
        messages: [
          {
            role: "system",
            content: `You are a senior trading analyst AI. Make the FINAL trading decisions based on social sentiment signals.

Rules:
- Only recommend BUY for symbols with strong conviction from multiple data points
- Recommend SELL for positions with deteriorating sentiment or hitting targets
- Consider the QUALITY of sentiment, not just quantity
- Output valid JSON only

Response format:
{
  "recommendations": [
    { "action": "BUY"|"SELL"|"HOLD", "symbol": "TICKER", "confidence": 0.0-1.0, "reasoning": "detailed reasoning", "suggested_size_pct": 10-30 }
  ],
  "market_summary": "overall market read and sentiment",
  "high_conviction_plays": ["symbols you feel strongest about"]
}`,
          },
          { role: "user", content: prompt },
        ],
        max_tokens: 800,
        temperature: 0.4,
      });

      const usage = response.usage;
      if (usage) {
        this.trackLLMCost(this.state.config.llm_analyst_model, usage.prompt_tokens, usage.completion_tokens);
      }

      const content = response.choices[0]?.message?.content || "{}";
      const analysis = JSON.parse(content.replace(/```json\n?|```/g, "").trim()) as {
        recommendations: Array<{
          action: "BUY" | "SELL" | "HOLD";
          symbol: string;
          confidence: number;
          reasoning: string;
          suggested_size_pct?: number;
        }>;
        market_summary: string;
        high_conviction_plays?: string[];
      };

      this.log("Analyst", "analysis_complete", {
        candidates: candidates.length,
        recommendations: analysis.recommendations?.length || 0,
      });

      return {
        recommendations: analysis.recommendations || [],
        market_summary: analysis.market_summary || "",
        high_conviction: analysis.high_conviction_plays || [],
      };
    } catch (error) {
      this.log("Analyst", "error", { message: String(error) });
      return { recommendations: [], market_summary: `Analysis failed: ${error}`, high_conviction: [] };
    }
  }

  // ============================================================================
  // SECTION 7: ANALYST & TRADING LOGIC
  // ============================================================================
  // [CUSTOMIZABLE] Core trading decision logic lives here.
  //
  // runAnalyst(): Main trading loop - checks exits, then looks for entries
  // executeBuy(): Position sizing and order execution
  // executeSell(): Closes positions with reason logging
  //
  // [TUNE] Position sizing formula in executeBuy()
  // [TUNE] Entry/exit conditions in runAnalyst()
  // ============================================================================

  private async runAnalyst(): Promise<void> {
    const alpaca = createAlpacaProviders(this.env);
    
    const [account, positions, clock] = await Promise.all([
      alpaca.trading.getAccount(),
      alpaca.trading.getPositions(),
      alpaca.trading.getClock(),
    ]);
    
    if (!account || !clock.is_open) {
      this.log("System", "analyst_skipped", { reason: "Account unavailable or market closed" });
      return;
    }
    
    const heldSymbols = new Set<string>();
    for (const position of positions) {
      this.addHeldSymbol(heldSymbols, position.symbol);
    }
    
    // Check position exits
    for (const pos of positions) {
      if (pos.asset_class === "us_option") continue;  // Options handled separately
      
      const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100;
      
      // Take profit
      if (plPct >= this.state.config.take_profit_pct) {
        await this.executeSell(alpaca, pos.symbol, `Take profit at +${plPct.toFixed(1)}%`);
        continue;
      }
      
      // Stop loss
      if (plPct <= -this.state.config.stop_loss_pct) {
        await this.executeSell(alpaca, pos.symbol, `Stop loss at ${plPct.toFixed(1)}%`);
        continue;
      }
      
      // Check staleness
      if (this.state.config.stale_position_enabled) {
        const stalenessResult = this.analyzeStaleness(pos.symbol, pos.current_price, 0);
        this.state.stalenessAnalysis[pos.symbol] = stalenessResult;
        
        if (stalenessResult.isStale) {
          await this.executeSell(alpaca, pos.symbol, `STALE: ${stalenessResult.reason}`);
        }
      }
    }
    
    if (positions.length < this.state.config.max_positions && this.state.signalCache.length > 0) {
      const minConfidence = this.state.config.min_analyst_confidence;
      const allResearch = Object.values(this.state.signalResearch);
      const buyResearch = allResearch.filter(r => r.verdict === "BUY");
      const buyAbove = buyResearch.filter(r => r.confidence >= minConfidence);
      const buyBelow = buyResearch.filter(r => r.confidence < minConfidence);
      const buyHeld = buyAbove.filter(r => heldSymbols.has(normalizeSymbol(r.symbol)));

      this.log("System", "entry_research_summary", {
        total_research: allResearch.length,
        buy_verdict: buyResearch.length,
        buy_above_threshold: buyAbove.length,
        buy_below_threshold: buyBelow.length,
        buy_held: buyHeld.length,
        threshold: minConfidence,
      });

      const researchedBuys = buyAbove
        .filter(r => !heldSymbols.has(normalizeSymbol(r.symbol)))
        .sort((a, b) => b.confidence - a.confidence);

      for (const research of researchedBuys.slice(0, 3)) {
        if (positions.length >= this.state.config.max_positions) break;
        if (heldSymbols.has(normalizeSymbol(research.symbol))) continue;

        const originalSignal = this.state.signalCache.find(s => s.symbol === research.symbol);
        let finalConfidence = research.confidence;

        if (this.isTwitterEnabled() && originalSignal) {
          const twitterConfirm = await this.gatherTwitterConfirmation(research.symbol, originalSignal.sentiment);
          if (twitterConfirm?.confirms_existing) {
            finalConfidence = Math.min(1.0, finalConfidence * 1.15);
            this.log("System", "twitter_boost", { symbol: research.symbol, new_confidence: finalConfidence });
          } else if (twitterConfirm && !twitterConfirm.confirms_existing && twitterConfirm.sentiment !== 0) {
            finalConfidence = finalConfidence * 0.85;
          }
        }

        if (finalConfidence < minConfidence) {
          this.log("System", "entry_skipped_low_confidence", {
            symbol: research.symbol,
            confidence: finalConfidence,
            required: minConfidence,
          });
          continue;
        }

        const shouldUseOptions = this.isOptionsEnabled() &&
          finalConfidence >= this.state.config.options_min_confidence &&
          research.entry_quality === "excellent";

        if (shouldUseOptions) {
          const contract = await this.findBestOptionsContract(research.symbol, "bullish", account.equity);
          if (contract) {
            const optionsResult = await this.executeOptionsOrder(contract, 1, account.equity);
            if (optionsResult) {
              this.log("System", "options_position_opened", { symbol: research.symbol, contract: contract.symbol });
            }
          }
        }

        const resultSymbol = await this.executeBuy(alpaca, research.symbol, finalConfidence, account);
        if (resultSymbol) {
          this.addHeldSymbol(heldSymbols, resultSymbol);
          this.addHeldSymbol(heldSymbols, research.symbol);
          this.state.positionEntries[resultSymbol] = {
            symbol: resultSymbol,
            entry_time: Date.now(),
            entry_price: 0,
            entry_sentiment: originalSignal?.sentiment || finalConfidence,
            entry_social_volume: originalSignal?.volume || 0,
            entry_sources: originalSignal?.subreddits || [originalSignal?.source || "research"],
            entry_reason: research.reasoning,
            peak_price: 0,
            peak_sentiment: originalSignal?.sentiment || finalConfidence,
          };
        }
      }

      if (positions.length < this.state.config.max_positions) {
        const analysis = await this.analyzeSignalsWithLLM(this.state.signalCache, positions, account);
        const researchedSymbols = new Set(researchedBuys.map(r => normalizeSymbol(r.symbol)));

        for (const rec of analysis.recommendations) {
          if (positions.length >= this.state.config.max_positions) break;
          if (rec.action !== "BUY") {
            this.log("System", "analyst_rec_skip", {
              symbol: rec.symbol,
              reason: "action_not_buy",
              action: rec.action,
              confidence: rec.confidence,
            });
            continue;
          }
          if (rec.confidence < minConfidence) {
            this.log("System", "analyst_rec_skip", {
              symbol: rec.symbol,
              reason: "below_threshold",
              confidence: rec.confidence,
              required: minConfidence,
            });
            continue;
          }
          if (heldSymbols.has(normalizeSymbol(rec.symbol))) {
            this.log("System", "analyst_rec_skip", {
              symbol: rec.symbol,
              reason: "already_held",
            });
            continue;
          }
          if (researchedSymbols.has(normalizeSymbol(rec.symbol))) {
            this.log("System", "analyst_rec_skip", {
              symbol: rec.symbol,
              reason: "already_researched",
            });
            continue;
          }

          const resultSymbol = await this.executeBuy(alpaca, rec.symbol, rec.confidence, account);
          if (resultSymbol) {
            const originalSignal = this.state.signalCache.find(s => s.symbol === rec.symbol);
            this.addHeldSymbol(heldSymbols, resultSymbol);
            this.addHeldSymbol(heldSymbols, rec.symbol);
            this.state.positionEntries[resultSymbol] = {
              symbol: resultSymbol,
              entry_time: Date.now(),
              entry_price: 0,
              entry_sentiment: originalSignal?.sentiment || rec.confidence,
              entry_social_volume: originalSignal?.volume || 0,
              entry_sources: originalSignal?.subreddits || [originalSignal?.source || "analyst"],
              entry_reason: rec.reasoning,
              peak_price: 0,
              peak_sentiment: originalSignal?.sentiment || rec.confidence,
            };
          }
        }
      }
    }
    if (positions.length >= this.state.config.max_positions) {
      this.log("System", "entry_skipped_max_positions", {
        held: positions.length,
        max: this.state.config.max_positions,
      });
    } else if (this.state.signalCache.length === 0) {
      this.log("System", "entry_skipped_no_signals", { reason: "Signal cache empty" });
    }
  }

  private buildCryptoSymbolMap(): Map<string, string> {
    return buildCryptoSymbolMap(this.getActiveCryptoSymbols());
  }

  private getActiveCryptoSymbols(): string[] {
    const fallback = this.state.config.crypto_symbols ?? ["BTC/USD", "ETH/USD", "SOL/USD"];
    const topN = this.state.config.crypto_universe_top_n ?? 0;
    if (topN <= 0) return fallback;
    if (this.state.cryptoUniverseSymbols.length > 0) return this.state.cryptoUniverseSymbols;
    return fallback;
  }

  private getCryptoBaseSymbol(symbol: string): string {
    const normalized = normalizeSymbol(symbol);
    if (normalized.includes("/")) {
      return normalized.split("/")[0] ?? normalized;
    }
    if (normalized.endsWith("USD") && normalized.length > 3) {
      return normalized.slice(0, -3);
    }
    return normalized;
  }

  private isStablecoinSymbol(symbol: string): boolean {
    const base = this.getCryptoBaseSymbol(symbol);
    const stablecoins = new Set([
      "USDT",
      "USDC",
      "USDG",
      "DAI",
      "BUSD",
      "TUSD",
      "USDP",
      "GUSD",
      "FDUSD",
      "USDD",
      "USDE",
      "EURC",
      "PYUSD",
    ]);
    return stablecoins.has(base);
  }

  private addHeldSymbol(heldSymbols: Set<string>, symbol: string): void {
    const normalized = normalizeSymbol(symbol);
    heldSymbols.add(normalized);
    const base = this.getCryptoBaseSymbol(normalized);
    if (base !== normalized) {
      heldSymbols.add(base);
    }
  }

  private async refreshCryptoUniverse(
    alpaca: ReturnType<typeof createAlpacaProviders>
  ): Promise<void> {
    const topN = this.state.config.crypto_universe_top_n ?? 0;
    if (!this.state.config.crypto_enabled || topN <= 0) return;

    const now = Date.now();
    const refreshMs = Math.max(120_000, this.state.config.crypto_universe_refresh_ms || 0);
    if (this.state.cryptoUniverseUpdatedAt && now - this.state.cryptoUniverseUpdatedAt < refreshMs) {
      return;
    }

    let marketSymbols: string[] = [];
    const hasCMCKey = !!this.env.COINMARKETCAP_API_KEY;
    const provider = hasCMCKey ? "coinmarketcap" : "coinpaprika";
    try {
      if (hasCMCKey) {
        const url = `https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?sort=market_cap&sort_dir=desc&limit=${topN}&convert=USD`;
        const response = await fetch(url, {
          headers: {
            "accept": "application/json",
            "X-CMC_PRO_API_KEY": this.env.COINMARKETCAP_API_KEY || "",
          },
        });
        if (!response.ok) {
          this.log("Crypto", "universe_fetch_failed", { provider, status: response.status });
          return;
        }
        const payload = await response.json() as { data?: Array<{ symbol?: string }> };
        marketSymbols = (payload.data ?? [])
          .map((entry) => (entry.symbol || "").toUpperCase())
          .filter(Boolean)
          .slice(0, topN);
      } else {
        const url = "https://api.coinpaprika.com/v1/tickers";
        const response = await fetch(url, {
          headers: { "accept": "application/json" },
        });
        if (!response.ok) {
          this.log("Crypto", "universe_fetch_failed", { provider, status: response.status });
          return;
        }
        const data = await response.json() as Array<{ symbol?: string; rank?: number }>;
        const ranked = data
          .filter((entry) => typeof entry.rank === "number" && entry.rank > 0 && entry.symbol)
          .sort((a, b) => (a.rank ?? 0) - (b.rank ?? 0));
        const seen = new Set<string>();
        for (const entry of ranked) {
          if (marketSymbols.length >= topN) break;
          const symbol = (entry.symbol || "").toUpperCase();
          if (!symbol || seen.has(symbol)) continue;
          seen.add(symbol);
          marketSymbols.push(symbol);
        }
      }
    } catch (error) {
      this.log("Crypto", "universe_fetch_error", { provider, error: String(error) });
      return;
    }

    if (marketSymbols.length === 0) {
      this.log("Crypto", "universe_empty", { provider, reason: "No symbols returned" });
      return;
    }

    let assets: Asset[] = [];
    try {
      assets = await alpaca.trading.listAssets({ status: "active", asset_class: "crypto" });
    } catch (error) {
      this.log("Crypto", "universe_alpaca_assets_failed", { error: String(error) });
      return;
    }

    const tradableMap = new Map<string, string>();
    for (const asset of assets) {
      if (!asset.tradable || asset.status !== "active" || asset.class !== "crypto") continue;

      const normalized = normalizeSymbol(asset.symbol);
      if (normalized.includes("/")) {
        const [base, quote] = normalized.split("/");
        if (!base || quote !== "USD") continue;
        if (!tradableMap.has(base)) {
          tradableMap.set(base, normalized);
        }
        continue;
      }

      if (normalized.endsWith("USD") && normalized.length > 3) {
        const base = normalized.slice(0, -3);
        if (!tradableMap.has(base)) {
          tradableMap.set(base, normalized);
        }
      }
    }

    const matched: string[] = [];
    for (const symbol of marketSymbols) {
      const alpacaSymbol = tradableMap.get(symbol);
      if (alpacaSymbol) matched.push(alpacaSymbol);
    }

    const unique = Array.from(new Set(matched));
    if (unique.length === 0) {
      this.log("Crypto", "universe_no_match", { provider, requested: topN, symbols: marketSymbols.length });
      return;
    }

    this.state.cryptoUniverseSymbols = unique;
    this.state.cryptoUniverseUpdatedAt = now;
    this.log("Crypto", "universe_updated", {
      provider,
      requested: topN,
      symbols: marketSymbols.length,
      alpaca: unique.length,
      refresh_ms: refreshMs,
    });
  }

  private async resolveAssetClass(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbol: string
  ): Promise<{ symbol: string; isCrypto: boolean; asset: Asset | null }> {
    const normalized = normalizeSymbol(symbol);
    let asset: Asset | null = null;

    if (normalized.includes("/")) {
      asset = await alpaca.trading.getAsset(normalized).catch(() => null);
      if (!asset) {
        const compact = normalized.replace("/", "");
        asset = await alpaca.trading.getAsset(compact).catch(() => null);
      }
      if (asset?.class === "crypto") {
        return { symbol: normalizeSymbol(asset.symbol), isCrypto: true, asset };
      }
      return { symbol: normalized, isCrypto: true, asset };
    }

    const cryptoMap = this.buildCryptoSymbolMap();
    const mapped = cryptoMap.get(normalized);
    if (mapped) {
      asset = await alpaca.trading.getAsset(mapped).catch(() => null);
      if (!asset) {
        const compact = mapped.replace("/", "");
        asset = await alpaca.trading.getAsset(compact).catch(() => null);
      }
      if (asset?.class === "crypto") {
        return { symbol: normalizeSymbol(asset.symbol), isCrypto: true, asset };
      }
      return { symbol: mapped, isCrypto: true, asset };
    }

    try {
      asset = await alpaca.trading.getAsset(normalized);
      if (asset?.class === "crypto") {
        return { symbol: normalizeSymbol(asset.symbol), isCrypto: true, asset };
      }
      if (asset) {
        return { symbol: normalizeSymbol(asset.symbol), isCrypto: false, asset };
      }
    } catch {
      // Best-effort lookup; fall back to heuristics below.
    }

    const slashUsd = toSlashUsdSymbol(normalized);
    if (slashUsd && slashUsd !== normalized) {
      try {
        asset = await alpaca.trading.getAsset(slashUsd);
        if (asset?.class === "crypto") {
          return { symbol: normalizeSymbol(asset.symbol), isCrypto: true, asset };
        }
      } catch {
        // Ignore secondary lookup failures.
      }
    }

    return { symbol: normalized, isCrypto: false, asset };
  }

  private async executeBuy(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbol: string,
    confidence: number,
    account: Account
  ): Promise<string | null> {
    const sizePct = Math.min(20, this.state.config.position_size_pct_of_cash);
    const positionSize = Math.min(
      account.cash * (sizePct / 100) * confidence,
      this.state.config.max_position_value
    );
    
    if (positionSize < 100) {
      this.log("Executor", "buy_skipped", { symbol, reason: "Position too small" });
      return null;
    }
    
    try {
      const resolved = await this.resolveAssetClass(alpaca, symbol);
      const orderSymbol = resolved.symbol;
      const order = await alpaca.trading.createOrder({
        symbol: orderSymbol,
        notional: Math.round(positionSize * 100) / 100,
        side: "buy",
        type: "market",
        time_in_force: resolved.isCrypto ? "gtc" : "day",
      });
      
      this.log("Executor", "buy_executed", { symbol: orderSymbol, status: order.status, size: positionSize });
      return orderSymbol;
    } catch (error) {
      this.log("Executor", "buy_failed", { symbol, error: String(error) });
      return null;
    }
  }

  private async executeSell(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbol: string,
    reason: string
  ): Promise<boolean> {
    try {
      await alpaca.trading.closePosition(symbol);
      this.log("Executor", "sell_executed", { symbol, reason });
      
      // Clean up tracking
      delete this.state.positionEntries[symbol];
      delete this.state.socialHistory[symbol];
      delete this.state.stalenessAnalysis[symbol];
      
      return true;
    } catch (error) {
      this.log("Executor", "sell_failed", { symbol, error: String(error) });
      return false;
    }
  }

  private async closePositionWithFallback(
    alpaca: ReturnType<typeof createAlpacaProviders>,
    symbols: string[],
    reason: string
  ): Promise<{ ok: boolean; symbol?: string; error?: string }> {
    let lastError: string | undefined;

    for (const candidate of symbols) {
      if (!candidate) continue;
      try {
        await alpaca.trading.closePosition(candidate);
        this.log("Executor", "sell_executed", { symbol: candidate, reason });

        for (const key of symbols) {
          delete this.state.positionEntries[key];
          delete this.state.socialHistory[key];
          delete this.state.stalenessAnalysis[key];
        }

        return { ok: true, symbol: candidate };
      } catch (error) {
        lastError = String(error);
      }
    }

    if (lastError) {
      this.log("Executor", "sell_failed", { symbol: symbols[0] ?? "unknown", error: lastError });
    }
    return { ok: false, error: lastError || "Close failed" };
  }

  // ============================================================================
  // SECTION 8: STALENESS DETECTION
  // ============================================================================
  // [TOGGLE] Enable with stale_position_enabled in config
  // [TUNE] Staleness thresholds (hold time, volume decay, gain requirements)
  //
  // Staleness = positions that lost momentum. Scored 0-100 based on:
  // - Time held (vs max hold days)
  // - Price action (P&L vs targets)
  // - Social volume decay (vs entry volume)
  // ============================================================================

  private analyzeStaleness(symbol: string, currentPrice: number, currentSocialVolume: number): {
    isStale: boolean;
    reason: string;
    staleness_score: number;
  } {
    const entry = this.state.positionEntries[symbol];
    if (!entry) {
      return { isStale: false, reason: "No entry data", staleness_score: 0 };
    }

    const holdHours = (Date.now() - entry.entry_time) / (1000 * 60 * 60);
    const holdDays = holdHours / 24;
    const pnlPct = entry.entry_price > 0 
      ? ((currentPrice - entry.entry_price) / entry.entry_price) * 100 
      : 0;

    if (holdHours < this.state.config.stale_min_hold_hours) {
      return { isStale: false, reason: `Too early (${holdHours.toFixed(1)}h)`, staleness_score: 0 };
    }

    let stalenessScore = 0;

    // Time-based (max 40 points)
    if (holdDays >= this.state.config.stale_max_hold_days) {
      stalenessScore += 40;
    } else if (holdDays >= this.state.config.stale_mid_hold_days) {
      stalenessScore += 20 * (holdDays - this.state.config.stale_mid_hold_days) / 
        (this.state.config.stale_max_hold_days - this.state.config.stale_mid_hold_days);
    }

    // Price action (max 30 points)
    if (pnlPct < 0) {
      stalenessScore += Math.min(30, Math.abs(pnlPct) * 3);
    } else if (pnlPct < this.state.config.stale_mid_min_gain_pct && holdDays >= this.state.config.stale_mid_hold_days) {
      stalenessScore += 15;
    }

    // Social volume decay (max 30 points)
    const volumeRatio = entry.entry_social_volume > 0 
      ? currentSocialVolume / entry.entry_social_volume 
      : 1;
    if (volumeRatio <= this.state.config.stale_social_volume_decay) {
      stalenessScore += 30;
    } else if (volumeRatio <= 0.5) {
      stalenessScore += 15;
    }

    stalenessScore = Math.min(100, stalenessScore);
    
    const isStale = stalenessScore >= 70 || 
      (holdDays >= this.state.config.stale_max_hold_days && pnlPct < this.state.config.stale_min_gain_pct);

    return {
      isStale,
      reason: isStale 
        ? `Staleness score ${stalenessScore}/100, held ${holdDays.toFixed(1)} days`
        : `OK (score ${stalenessScore}/100)`,
      staleness_score: stalenessScore,
    };
  }

  // ============================================================================
  // SECTION 9: OPTIONS TRADING
  // ============================================================================
  // [TOGGLE] Enable with options_enabled in config
  // [TUNE] Delta, DTE, and position size limits in config
  //
  // Options are used for HIGH CONVICTION plays only (confidence >= 0.8).
  // Finds ATM/ITM calls for bullish signals, puts for bearish.
  // Wider stop-loss (50%) and higher take-profit (100%) than stocks.
  // ============================================================================

  private isOptionsEnabled(): boolean {
    return this.state.config.options_enabled === true;
  }

  private async findBestOptionsContract(
    symbol: string,
    direction: "bullish" | "bearish",
    equity: number
  ): Promise<{
    symbol: string;
    strike: number;
    expiration: string;
    delta: number;
    mid_price: number;
    max_contracts: number;
  } | null> {
    if (!this.isOptionsEnabled()) return null;

    try {
      const alpaca = createAlpacaProviders(this.env);
      const expirations = await alpaca.options.getExpirations(symbol);
      
      if (!expirations || expirations.length === 0) {
        this.log("Options", "no_expirations", { symbol });
        return null;
      }

      const today = new Date();
      const validExpirations = expirations.filter(exp => {
        const expDate = new Date(exp);
        const dte = Math.ceil((expDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
        return dte >= this.state.config.options_min_dte && dte <= this.state.config.options_max_dte;
      });

      if (validExpirations.length === 0) {
        this.log("Options", "no_valid_expirations", { symbol });
        return null;
      }

      const targetDTE = (this.state.config.options_min_dte + this.state.config.options_max_dte) / 2;
      const bestExpiration = validExpirations.reduce((best: string, exp: string) => {
        const expDate = new Date(exp);
        const dte = Math.ceil((expDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
        const currentBestDte = Math.ceil((new Date(best).getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
        return Math.abs(dte - targetDTE) < Math.abs(currentBestDte - targetDTE) ? exp : best;
      }, validExpirations[0]!);

      const chain = await alpaca.options.getChain(symbol, bestExpiration);
      if (!chain) {
        this.log("Options", "chain_failed", { symbol, expiration: bestExpiration });
        return null;
      }

      const contracts = direction === "bullish" ? chain.calls : chain.puts;
      if (!contracts || contracts.length === 0) {
        this.log("Options", "no_contracts", { symbol, direction });
        return null;
      }

      const quote = await alpaca.marketData.getQuote(symbol);
      const stockPrice = quote?.ask_price || quote?.bid_price || 0;
      if (stockPrice === 0) return null;

      const targetStrike = direction === "bullish"
        ? stockPrice * (1 - (this.state.config.options_target_delta - 0.5) * 0.2)
        : stockPrice * (1 + (this.state.config.options_target_delta - 0.5) * 0.2);

      const sortedContracts = contracts
        .filter(c => c.strike > 0)
        .sort((a, b) => Math.abs(a.strike - targetStrike) - Math.abs(b.strike - targetStrike));

      for (const contract of sortedContracts.slice(0, 5)) {
        const snapshot = await alpaca.options.getSnapshot(contract.symbol);
        if (!snapshot) continue;

        const delta = snapshot.greeks?.delta;
        const absDelta = delta !== undefined ? Math.abs(delta) : null;

        if (absDelta === null || absDelta < this.state.config.options_min_delta || absDelta > this.state.config.options_max_delta) {
          continue;
        }

        const bid = snapshot.latest_quote?.bid_price || 0;
        const ask = snapshot.latest_quote?.ask_price || 0;
        if (bid === 0 || ask === 0) continue;

        const spread = (ask - bid) / ask;
        if (spread > 0.10) continue;

        const midPrice = (bid + ask) / 2;
        const maxCost = equity * this.state.config.options_max_pct_per_trade;
        const maxContracts = Math.floor(maxCost / (midPrice * 100));

        if (maxContracts < 1) continue;

        this.log("Options", "contract_selected", {
          symbol,
          contract: contract.symbol,
          strike: contract.strike,
          expiration: bestExpiration,
          delta: delta?.toFixed(3),
          mid_price: midPrice.toFixed(2),
        });

        return {
          symbol: contract.symbol,
          strike: contract.strike,
          expiration: bestExpiration,
          delta: delta!,
          mid_price: midPrice,
          max_contracts: maxContracts,
        };
      }

      return null;
    } catch (error) {
      this.log("Options", "error", { symbol, message: String(error) });
      return null;
    }
  }

  private async executeOptionsOrder(
    contract: { symbol: string; mid_price: number },
    quantity: number,
    equity: number
  ): Promise<boolean> {
    if (!this.isOptionsEnabled()) return false;

    const totalCost = contract.mid_price * quantity * 100;
    const maxAllowed = equity * this.state.config.options_max_pct_per_trade;

    if (totalCost > maxAllowed) {
      quantity = Math.floor(maxAllowed / (contract.mid_price * 100));
      if (quantity < 1) {
        this.log("Options", "skipped_size", { contract: contract.symbol, cost: totalCost, max: maxAllowed });
        return false;
      }
    }

    try {
      const alpaca = createAlpacaProviders(this.env);
      const order = await alpaca.trading.createOrder({
        symbol: contract.symbol,
        qty: quantity,
        side: "buy",
        type: "limit",
        limit_price: Math.round(contract.mid_price * 100) / 100,
        time_in_force: "day",
      });

      this.log("Options", "options_buy_executed", {
        contract: contract.symbol,
        qty: quantity,
        status: order.status,
        estimated_cost: (contract.mid_price * quantity * 100).toFixed(2),
      });

      return true;
    } catch (error) {
      this.log("Options", "options_buy_failed", { contract: contract.symbol, error: String(error) });
      return false;
    }
  }

  private async checkOptionsExits(positions: Position[]): Promise<Array<{
    symbol: string;
    reason: string;
    type: string;
    pnl_pct: number;
  }>> {
    if (!this.isOptionsEnabled()) return [];

    const exits: Array<{ symbol: string; reason: string; type: string; pnl_pct: number }> = [];
    const optionsPositions = positions.filter(p => p.asset_class === "us_option");

    for (const pos of optionsPositions) {
      const entryPrice = pos.avg_entry_price || pos.current_price;
      const plPct = entryPrice > 0 ? ((pos.current_price - entryPrice) / entryPrice) * 100 : 0;

      if (plPct <= -this.state.config.options_stop_loss_pct) {
        exits.push({
          symbol: pos.symbol,
          reason: `Options stop loss at ${plPct.toFixed(1)}%`,
          type: "stop_loss",
          pnl_pct: plPct,
        });
        continue;
      }

      if (plPct >= this.state.config.options_take_profit_pct) {
        exits.push({
          symbol: pos.symbol,
          reason: `Options take profit at +${plPct.toFixed(1)}%`,
          type: "take_profit",
          pnl_pct: plPct,
        });
        continue;
      }
    }

    return exits;
  }

  // ============================================================================
  // SECTION 10: PRE-MARKET ANALYSIS
  // ============================================================================
  // Runs 9:25-9:29 AM ET to prepare a trading plan before market open.
  // Executes the plan at 9:30-9:32 AM when market opens.
  //
  // [TUNE] Change time windows in isPreMarketWindow() / isMarketJustOpened()
  // [TUNE] Plan staleness (PLAN_STALE_MS) in executePremarketPlan()
  // ============================================================================

  private isPreMarketWindow(): boolean {
    const now = new Date();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const day = now.getDay();

    if (day >= 1 && day <= 5) {
      if (hour === 9 && minute >= 25 && minute <= 29) {
        return true;
      }
    }
    return false;
  }

  private isMarketJustOpened(): boolean {
    const now = new Date();
    const hour = now.getHours();
    const minute = now.getMinutes();
    const day = now.getDay();

    if (day >= 1 && day <= 5) {
      if (hour === 9 && minute >= 30 && minute <= 32) {
        return true;
      }
    }
    return false;
  }

  private async runPreMarketAnalysis(): Promise<void> {
    const alpaca = createAlpacaProviders(this.env);
    const [account, positions] = await Promise.all([
      alpaca.trading.getAccount(),
      alpaca.trading.getPositions(),
    ]);

    if (!account || this.state.signalCache.length === 0) return;

    this.log("System", "premarket_analysis_starting", {
      signals: this.state.signalCache.length,
      researched: Object.keys(this.state.signalResearch).length,
    });

    const signalResearch = await this.researchTopSignals(10);
    const analysis = await this.analyzeSignalsWithLLM(this.state.signalCache, positions, account);

    this.state.premarketPlan = {
      timestamp: Date.now(),
      recommendations: analysis.recommendations.map(r => ({
        action: r.action,
        symbol: r.symbol,
        confidence: r.confidence,
        reasoning: r.reasoning,
        suggested_size_pct: r.suggested_size_pct,
      })),
      market_summary: analysis.market_summary,
      high_conviction: analysis.high_conviction,
      researched_buys: signalResearch.filter(r => r.verdict === "BUY"),
    };

    const buyRecs = this.state.premarketPlan.recommendations.filter(r => r.action === "BUY").length;
    const sellRecs = this.state.premarketPlan.recommendations.filter(r => r.action === "SELL").length;

    this.log("System", "premarket_analysis_complete", {
      buy_recommendations: buyRecs,
      sell_recommendations: sellRecs,
      high_conviction: this.state.premarketPlan.high_conviction,
    });
  }

  private async executePremarketPlan(): Promise<void> {
    const PLAN_STALE_MS = 600_000;
    
    if (!this.state.premarketPlan || Date.now() - this.state.premarketPlan.timestamp > PLAN_STALE_MS) {
      this.log("System", "no_premarket_plan", { reason: "Plan missing or stale" });
      return;
    }

    const alpaca = createAlpacaProviders(this.env);
    const [account, positions] = await Promise.all([
      alpaca.trading.getAccount(),
      alpaca.trading.getPositions(),
    ]);

    if (!account) return;

    const heldSymbols = new Set<string>();
    for (const position of positions) {
      this.addHeldSymbol(heldSymbols, position.symbol);
    }

    this.log("System", "executing_premarket_plan", {
      recommendations: this.state.premarketPlan.recommendations.length,
    });

    for (const rec of this.state.premarketPlan.recommendations) {
      if (rec.action === "SELL" && rec.confidence >= this.state.config.min_analyst_confidence) {
        await this.executeSell(alpaca, rec.symbol, `Pre-market plan: ${rec.reasoning}`);
      }
    }

    for (const rec of this.state.premarketPlan.recommendations) {
      if (rec.action === "BUY" && rec.confidence >= this.state.config.min_analyst_confidence) {
        if (heldSymbols.has(normalizeSymbol(rec.symbol))) continue;
        if (positions.length >= this.state.config.max_positions) break;

        const resultSymbol = await this.executeBuy(alpaca, rec.symbol, rec.confidence, account);
        if (resultSymbol) {
          this.addHeldSymbol(heldSymbols, resultSymbol);
          this.addHeldSymbol(heldSymbols, rec.symbol);

          const originalSignal = this.state.signalCache.find(s => s.symbol === rec.symbol);
          this.state.positionEntries[resultSymbol] = {
            symbol: resultSymbol,
            entry_time: Date.now(),
            entry_price: 0,
            entry_sentiment: originalSignal?.sentiment || 0,
            entry_social_volume: originalSignal?.volume || 0,
            entry_sources: originalSignal?.subreddits || [originalSignal?.source || "premarket"],
            entry_reason: rec.reasoning,
            peak_price: 0,
            peak_sentiment: originalSignal?.sentiment || 0,
          };
        }
      }
    }

    this.state.premarketPlan = null;
  }

  // ============================================================================
  // SECTION 11: UTILITIES
  // ============================================================================
  // Logging, cost tracking, persistence, and Discord notifications.
  // Generally don't need to modify unless adding new notification channels.
  // ============================================================================

  private log(agent: string, action: string, details: Record<string, unknown>): void {
    const entry: LogEntry = {
      timestamp: new Date().toISOString(),
      agent,
      action,
      ...details,
    };
    this.state.logs.push(entry);
    
    // Keep last 500 logs
    if (this.state.logs.length > 500) {
      this.state.logs = this.state.logs.slice(-500);
    }
    
    // Log to console for wrangler tail
    console.log(`[${entry.timestamp}] [${agent}] ${action}`, JSON.stringify(details));
  }

  public trackLLMCost(model: string, tokensIn: number, tokensOut: number): number {
    const pricing: Record<string, { input: number; output: number }> = {
      "gpt-4o": { input: 2.5, output: 10 },
      "gpt-4o-mini": { input: 0.15, output: 0.6 },
    };
    
    const rates = pricing[model] ?? pricing["gpt-4o"]!;
    const cost = (tokensIn * rates.input + tokensOut * rates.output) / 1_000_000;
    
    this.state.costTracker.total_usd += cost;
    this.state.costTracker.calls++;
    this.state.costTracker.tokens_in += tokensIn;
    this.state.costTracker.tokens_out += tokensOut;
    
    return cost;
  }

  private async persist(): Promise<void> {
    await this.ctx.storage.put("state", this.state);
  }

  private jsonResponse(data: unknown): Response {
    return new Response(JSON.stringify(data, null, 2), {
      headers: { "Content-Type": "application/json" },
    });
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  get openai(): OpenAI | null {
    return this._openai;
  }

  private discordCooldowns: Map<string, number> = new Map();
  private readonly DISCORD_COOLDOWN_MS = 30 * 60 * 1000;

  private async sendDiscordNotification(
    type: "signal" | "research",
    data: {
      symbol: string;
      sentiment?: number;
      sources?: string[];
      verdict?: string;
      confidence?: number;
      quality?: string;
      reasoning?: string;
      catalysts?: string[];
      red_flags?: string[];
    }
  ): Promise<void> {
    if (!this.env.DISCORD_WEBHOOK_URL) return;

    const cacheKey = data.symbol;
    const lastNotification = this.discordCooldowns.get(cacheKey);
    if (lastNotification && Date.now() - lastNotification < this.DISCORD_COOLDOWN_MS) {
      return;
    }

    try {
      let embed: {
        title: string;
        color: number;
        fields: Array<{ name: string; value: string; inline: boolean }>;
        description?: string;
        timestamp: string;
        footer: { text: string };
      };

      if (type === "signal") {
        embed = {
          title: `🔔 SIGNAL: $${data.symbol}`,
          color: 0xfbbf24,
          fields: [
            { name: "Sentiment", value: `${((data.sentiment || 0) * 100).toFixed(0)}% bullish`, inline: true },
            { name: "Sources", value: data.sources?.join(", ") || "StockTwits", inline: true },
          ],
          description: "High sentiment detected, researching...",
          timestamp: new Date().toISOString(),
          footer: { text: "MAHORAGA • Not financial advice • DYOR" },
        };
      } else {
        const verdictEmoji = data.verdict === "BUY" ? "✅" : data.verdict === "SKIP" ? "⏭️" : "⏸️";
        const color = data.verdict === "BUY" ? 0x22c55e : data.verdict === "SKIP" ? 0x6b7280 : 0xfbbf24;

        embed = {
          title: `${verdictEmoji} $${data.symbol} → ${data.verdict}`,
          color,
          fields: [
            { name: "Confidence", value: `${((data.confidence || 0) * 100).toFixed(0)}%`, inline: true },
            { name: "Quality", value: data.quality || "N/A", inline: true },
            { name: "Sentiment", value: `${((data.sentiment || 0) * 100).toFixed(0)}%`, inline: true },
          ],
          timestamp: new Date().toISOString(),
          footer: { text: "MAHORAGA • Not financial advice • DYOR" },
        };

        if (data.reasoning) {
          embed.description = data.reasoning.substring(0, 300) + (data.reasoning.length > 300 ? "..." : "");
        }

        if (data.catalysts && data.catalysts.length > 0) {
          embed.fields.push({ name: "Catalysts", value: data.catalysts.slice(0, 3).join(", "), inline: false });
        }

        if (data.red_flags && data.red_flags.length > 0) {
          embed.fields.push({ name: "⚠️ Red Flags", value: data.red_flags.slice(0, 3).join(", "), inline: false });
        }
      }

      await fetch(this.env.DISCORD_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ embeds: [embed] }),
      });

      this.discordCooldowns.set(cacheKey, Date.now());
      this.log("Discord", "notification_sent", { type, symbol: data.symbol });
    } catch (err) {
      this.log("Discord", "notification_failed", { error: String(err) });
    }
  }
}

// ============================================================================
// SECTION 12: EXPORTS & HELPERS
// ============================================================================
// Helper functions to interact with the DO from your worker.
// ============================================================================

export function getHarnessStub(env: Env): DurableObjectStub {
  if (!env.MAHORAGA_HARNESS) {
    throw new Error("MAHORAGA_HARNESS binding not configured - check wrangler.toml");
  }
  const id = env.MAHORAGA_HARNESS.idFromName("main");
  return env.MAHORAGA_HARNESS.get(id);
}

export async function getHarnessStatus(env: Env): Promise<unknown> {
  const stub = getHarnessStub(env);
  const response = await stub.fetch(new Request("http://harness/status"));
  return response.json();
}

export async function enableHarness(env: Env): Promise<void> {
  const stub = getHarnessStub(env);
  await stub.fetch(new Request("http://harness/enable"));
}

export async function disableHarness(env: Env): Promise<void> {
  const stub = getHarnessStub(env);
  await stub.fetch(new Request("http://harness/disable"));
}
