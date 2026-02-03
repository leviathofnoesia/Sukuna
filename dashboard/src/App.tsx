import { useState, useEffect, useRef, useMemo } from 'react'
import { motion, AnimatePresence } from 'motion/react'
import clsx from 'clsx'
import { Panel } from './components/Panel'
import { Metric, MetricInline } from './components/Metric'
import { StatusIndicator, StatusBar } from './components/StatusIndicator'
import { SettingsModal } from './components/SettingsModal'
import { SetupWizard } from './components/SetupWizard'
import { LineChart, Sparkline } from './components/LineChart'
import { NotificationBell } from './components/NotificationBell'
import { Tooltip, TooltipContent } from './components/Tooltip'
import type { Status, Config, LogEntry, Signal, Position, SignalResearch, PortfolioSnapshot } from './types'

const API_BASE = '/api'

function getApiToken(): string {
  return localStorage.getItem('mahoraga_api_token') || (window as unknown as { VITE_MAHORAGA_API_TOKEN?: string }).VITE_MAHORAGA_API_TOKEN || ''
}

function authFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const token = getApiToken()
  const headers = new Headers(options.headers)
  if (token) {
    headers.set('Authorization', `Bearer ${token}`)
  }
  if (!headers.has('Content-Type') && options.body) {
    headers.set('Content-Type', 'application/json')
  }
  return fetch(url, { ...options, headers })
}

function formatCurrency(amount: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount)
}

function formatPercent(value: number): string {
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

function getAgentColor(agent: string): string {
  const colors: Record<string, string> = {
    'Analyst': 'text-hud-purple',
    'Executor': 'text-hud-cyan',
    'StockTwits': 'text-hud-success',
    'SignalResearch': 'text-hud-cyan',
    'PositionResearch': 'text-hud-purple',
    'Crypto': 'text-hud-warning',
    'System': 'text-hud-text-dim',
  }
  return colors[agent] || 'text-hud-text'
}

function isCryptoSymbol(symbol: string, cryptoSymbols: string[] = []): boolean {
  return cryptoSymbols.includes(symbol) || symbol.includes('/USD') || symbol.includes('BTC') || symbol.includes('ETH') || symbol.includes('SOL')
}

function getVerdictColor(verdict: string): string {
  if (verdict === 'BUY') return 'text-hud-success'
  if (verdict === 'SKIP') return 'text-hud-error'
  return 'text-hud-warning'
}

function getQualityColor(quality: string): string {
  if (quality === 'excellent') return 'text-hud-success'
  if (quality === 'good') return 'text-hud-primary'
  if (quality === 'fair') return 'text-hud-warning'
  return 'text-hud-error'
}

function getSentimentColor(score: number): string {
  if (score >= 0.3) return 'text-hud-success'
  if (score <= -0.2) return 'text-hud-error'
  return 'text-hud-warning'
}

function isOptionsSignal(signal: Signal): boolean {
  if (signal.isCrypto) return false
  const source = (signal.source || '').toLowerCase()
  const detail = (signal.source_detail || '').toLowerCase()
  const reason = (signal.reason || '').toLowerCase()
  const subreddits = signal.subreddits?.map((s) => s.toLowerCase()) ?? []

  if (source.includes('options')) return true
  if (detail.includes('options')) return true
  if (reason.includes('options')) return true
  if (subreddits.some((sub) => sub.includes('options'))) return true
  return false
}

function formatDuration(ms: number): string {
  if (!ms || ms <= 0) return '—'
  const totalSeconds = Math.floor(ms / 1000)
  const hours = Math.floor(totalSeconds / 3600)
  const minutes = Math.floor((totalSeconds % 3600) / 60)
  const seconds = totalSeconds % 60
  if (hours > 0) return `${hours}h ${minutes}m`
  if (minutes > 0) return `${minutes}m ${seconds}s`
  return `${seconds}s`
}

function buildSignalTooltipItems(sig: Signal) {
  return [
    { label: 'Sentiment', value: `${(sig.sentiment * 100).toFixed(0)}%`, color: getSentimentColor(sig.sentiment) },
    { label: 'Volume', value: sig.volume },
    ...(sig.bullish !== undefined ? [{ label: 'Bullish', value: sig.bullish, color: 'text-hud-success' }] : []),
    ...(sig.bearish !== undefined ? [{ label: 'Bearish', value: sig.bearish, color: 'text-hud-error' }] : []),
    ...(sig.score !== undefined ? [{ label: 'Score', value: sig.score }] : []),
    ...(sig.upvotes !== undefined ? [{ label: 'Upvotes', value: sig.upvotes }] : []),
    ...(sig.momentum !== undefined ? [{ label: 'Momentum', value: `${sig.momentum >= 0 ? '+' : ''}${sig.momentum.toFixed(2)}%` }] : []),
    ...(sig.price !== undefined ? [{ label: 'Price', value: formatCurrency(sig.price) }] : []),
  ]
}

type LayoutItem = {
  id: string
  colSpan: number
  rowSpan: number
  minColSpan: number
  maxColSpan: number
  minRowSpan: number
  maxRowSpan: number
}

const LAYOUT_STORAGE_KEY = 'mahoraga_layout_v1'

const DEFAULT_LAYOUT: LayoutItem[] = [
  { id: 'account', colSpan: 3, rowSpan: 3, minColSpan: 2, maxColSpan: 6, minRowSpan: 2, maxRowSpan: 6 },
  { id: 'positions', colSpan: 5, rowSpan: 3, minColSpan: 3, maxColSpan: 8, minRowSpan: 2, maxRowSpan: 6 },
  { id: 'llm_costs', colSpan: 4, rowSpan: 3, minColSpan: 2, maxColSpan: 6, minRowSpan: 2, maxRowSpan: 6 },
  { id: 'portfolio', colSpan: 8, rowSpan: 3, minColSpan: 4, maxColSpan: 12, minRowSpan: 2, maxRowSpan: 6 },
  { id: 'position_perf', colSpan: 4, rowSpan: 3, minColSpan: 3, maxColSpan: 8, minRowSpan: 2, maxRowSpan: 6 },
  { id: 'active_signals', colSpan: 4, rowSpan: 3, minColSpan: 3, maxColSpan: 6, minRowSpan: 2, maxRowSpan: 6 },
  { id: 'activity_feed', colSpan: 4, rowSpan: 3, minColSpan: 3, maxColSpan: 6, minRowSpan: 2, maxRowSpan: 6 },
  { id: 'signal_research', colSpan: 4, rowSpan: 3, minColSpan: 3, maxColSpan: 6, minRowSpan: 2, maxRowSpan: 6 },
]

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value))
}

function normalizeLayout(layout?: LayoutItem[]): LayoutItem[] {
  if (!layout || layout.length === 0) return DEFAULT_LAYOUT
  return DEFAULT_LAYOUT.map((item) => {
    const match = layout.find((entry) => entry.id === item.id)
    if (!match) return item
    const merged = { ...item, ...match }
    return {
      ...merged,
      colSpan: clamp(merged.colSpan, merged.minColSpan, merged.maxColSpan),
      rowSpan: clamp(merged.rowSpan, merged.minRowSpan, merged.maxRowSpan),
    }
  })
}

// Generate mock portfolio history for demo (will be replaced by real data from API)
function generateMockPortfolioHistory(equity: number, points: number = 24): PortfolioSnapshot[] {
  const history: PortfolioSnapshot[] = []
  const now = Date.now()
  const interval = 3600000 // 1 hour in ms
  let value = equity * 0.95 // Start slightly lower
  
  for (let i = points; i >= 0; i--) {
    const change = (Math.random() - 0.45) * equity * 0.005 // Small random walk with slight upward bias
    value = Math.max(value + change, equity * 0.8)
    const pl = value - equity * 0.95
    history.push({
      timestamp: now - i * interval,
      equity: value,
      pl,
      pl_pct: (pl / (equity * 0.95)) * 100,
    })
  }
  // Ensure last point is current equity
  history[history.length - 1] = {
    timestamp: now,
    equity,
    pl: equity - history[0].equity,
    pl_pct: ((equity - history[0].equity) / history[0].equity) * 100,
  }
  return history
}

// Generate mock price history for positions
function generateMockPriceHistory(currentPrice: number, unrealizedPl: number, points: number = 20): number[] {
  const prices: number[] = []
  const isPositive = unrealizedPl >= 0
  const startPrice = currentPrice * (isPositive ? 0.95 : 1.05)
  
  for (let i = 0; i < points; i++) {
    const progress = i / (points - 1)
    const trend = startPrice + (currentPrice - startPrice) * progress
    const noise = trend * (Math.random() - 0.5) * 0.02
    prices.push(trend + noise)
  }
  prices[prices.length - 1] = currentPrice
  return prices
}

export default function App() {
  const [status, setStatus] = useState<Status | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [showSettings, setShowSettings] = useState(false)
  const [showSetup, setShowSetup] = useState(false)
  const [setupChecked, setSetupChecked] = useState(false)
  const [time, setTime] = useState(new Date())
  const [portfolioHistory, setPortfolioHistory] = useState<PortfolioSnapshot[]>([])
  const [closingSymbol, setClosingSymbol] = useState<string | null>(null)
  const [layoutEdit, setLayoutEdit] = useState(false)
  const [logFilter, setLogFilter] = useState<'all' | 'skips' | 'trades' | 'errors'>('all')
  const [logReasonFilter, setLogReasonFilter] = useState<string | null>(null)
  const [layout, setLayout] = useState<LayoutItem[]>(() => {
    if (typeof window === 'undefined') return DEFAULT_LAYOUT
    try {
      const stored = localStorage.getItem(LAYOUT_STORAGE_KEY)
      if (!stored) return DEFAULT_LAYOUT
      const parsed = JSON.parse(stored) as LayoutItem[]
      return normalizeLayout(parsed)
    } catch {
      return DEFAULT_LAYOUT
    }
  })
  const [draggingPanel, setDraggingPanel] = useState<string | null>(null)
  const [dropTarget, setDropTarget] = useState<string | null>(null)
  const [viewportWidth, setViewportWidth] = useState(
    typeof window !== 'undefined' ? window.innerWidth : 1280
  )
  const logsEndRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const checkSetup = async () => {
      try {
        const res = await authFetch(`${API_BASE}/setup/status`)
        const data = await res.json()
        if (data.ok && !data.data.configured) {
          setShowSetup(true)
        }
        setSetupChecked(true)
      } catch {
        setSetupChecked(true)
      }
    }
    checkSetup()
  }, [])

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await authFetch(`${API_BASE}/status`)
        const data = await res.json()
        if (data.ok) {
          setStatus(data.data)
          setError(null)
          
          // Generate mock portfolio history if we have account data but no history
          if (data.data.account && portfolioHistory.length === 0) {
            setPortfolioHistory(generateMockPortfolioHistory(data.data.account.equity))
          } else if (data.data.account) {
            // Append new data point on each fetch
            setPortfolioHistory(prev => {
              const now = Date.now()
              const newSnapshot: PortfolioSnapshot = {
                timestamp: now,
                equity: data.data.account.equity,
                pl: data.data.account.equity - (prev[0]?.equity || data.data.account.equity),
                pl_pct: prev[0] ? ((data.data.account.equity - prev[0].equity) / prev[0].equity) * 100 : 0,
              }
              // Keep last 48 points (4 hours at 5-second intervals, or display fewer if needed)
              const updated = [...prev, newSnapshot].slice(-48)
              return updated
            })
          }
        } else {
          setError(data.error || 'Failed to fetch status')
        }
      } catch (err) {
        setError('Connection failed - is the agent running?')
      }
    }

    if (setupChecked && !showSetup && !showSettings) {
      fetchStatus()
      const interval = setInterval(fetchStatus, 5000)
      const timeInterval = setInterval(() => setTime(new Date()), 1000)

      return () => {
        clearInterval(interval)
        clearInterval(timeInterval)
      }
    }
  }, [setupChecked, showSetup, showSettings])

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [status?.logs])

  useEffect(() => {
    const handleResize = () => setViewportWidth(window.innerWidth)
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') return
    localStorage.setItem(LAYOUT_STORAGE_KEY, JSON.stringify(layout))
  }, [layout])

  useEffect(() => {
    if (!layoutEdit) {
      setDraggingPanel(null)
      setDropTarget(null)
    }
  }, [layoutEdit])

  const resetLayout = () => {
    setLayout(DEFAULT_LAYOUT)
  }

  const movePanel = (sourceId: string, targetId: string) => {
    if (sourceId === targetId) return
    setLayout((prev) => {
      const next = [...prev]
      const sourceIndex = next.findIndex((item) => item.id === sourceId)
      const targetIndex = next.findIndex((item) => item.id === targetId)
      if (sourceIndex === -1 || targetIndex === -1) return prev
      const [moved] = next.splice(sourceIndex, 1)
      next.splice(targetIndex, 0, moved)
      return next
    })
  }

  const resizePanel = (id: string, deltaCols: number, deltaRows: number) => {
    setLayout((prev) =>
      prev.map((item) => {
        if (item.id !== id) return item
        const colSpan = clamp(item.colSpan + deltaCols, item.minColSpan, item.maxColSpan)
        const rowSpan = clamp(item.rowSpan + deltaRows, item.minRowSpan, item.maxRowSpan)
        return { ...item, colSpan, rowSpan }
      })
    )
  }

  const handleSaveConfig = async (config: Config) => {
    const res = await authFetch(`${API_BASE}/config`, {
      method: 'POST',
      body: JSON.stringify(config),
    })
    const data = await res.json()
    if (data.ok && status) {
      setStatus({ ...status, config: data.data })
    }
  }

  const handleClosePosition = async (symbol: string) => {
    if (closingSymbol) return
    if (!confirm(`Close ${symbol} position?`)) return
    setClosingSymbol(symbol)
    try {
      const res = await authFetch(`${API_BASE}/positions/close`, {
        method: 'POST',
        body: JSON.stringify({ symbol, reason: 'manual_close' }),
      })
      const data = await res.json()
      if (!data.ok) {
        alert(data.error || `Failed to close ${symbol}`)
        return
      }
      const statusRes = await authFetch(`${API_BASE}/status`)
      const statusData = await statusRes.json()
      if (statusData.ok) {
        setStatus(statusData.data)
      }
    } catch {
      alert(`Failed to close ${symbol}`)
    } finally {
      setClosingSymbol(null)
    }
  }

  // Derived state (must stay above early returns per React hooks rules)
  const account = status?.account
  const positions = status?.positions || []
  const signals = status?.signals || []
  const logs = status?.logs || []
  const costs = status?.costs || { total_usd: 0, calls: 0, tokens_in: 0, tokens_out: 0 }
  const config = status?.config
  const isMarketOpen = status?.clock?.is_open ?? false

  const logReasonCounts = useMemo(() => {
    const counts = new Map<string, number>()
    logs.slice(-200).forEach((log) => {
      const reason = typeof log.reason === 'string' ? log.reason : ''
      if (!reason) return
      counts.set(reason, (counts.get(reason) || 0) + 1)
    })
    return Array.from(counts.entries()).sort((a, b) => b[1] - a[1])
  }, [logs])

  const filteredLogs = useMemo(() => {
    return logs.filter((log) => {
      const action = (log.action || '').toLowerCase()
      const reason = typeof log.reason === 'string' ? log.reason : ''
      const isSkip = action.includes('skip') || action.includes('skipped')
      const isTrade = action.includes('buy_executed') || action.includes('sell_executed') || action.includes('options_position_opened')
      const isError = action.includes('failed') || action.includes('error')

      if (logFilter === 'skips' && !isSkip) return false
      if (logFilter === 'trades' && !isTrade) return false
      if (logFilter === 'errors' && !isError) return false
      if (logReasonFilter && reason !== logReasonFilter) return false
      return true
    })
  }, [logs, logFilter, logReasonFilter])

  const { stockSignals, optionsSignals, cryptoSignals } = useMemo(() => {
    const crypto = signals.filter((sig) => sig.isCrypto)
    const options = signals.filter((sig) => !sig.isCrypto && isOptionsSignal(sig))
    const stocks = signals.filter((sig) => !sig.isCrypto && !isOptionsSignal(sig))
    return { stockSignals: stocks, optionsSignals: options, cryptoSignals: crypto }
  }, [signals])

  const cryptoUniverse = status?.cryptoUniverse
  const cryptoUniverseProvider = (cryptoUniverse?.provider || 'coinpaprika').toUpperCase()
  const cryptoUniverseTopN = cryptoUniverse?.top_n ?? config?.crypto_universe_top_n ?? 0
  const cryptoUniverseRefreshMs = cryptoUniverse?.refresh_ms ?? config?.crypto_universe_refresh_ms ?? 0
  const cryptoUniverseEnabled = !!(config?.crypto_enabled && cryptoUniverseTopN > 0)
  const cryptoUniverseSize = cryptoUniverse?.size ?? 0
  const cryptoUniverseLastUpdated = cryptoUniverse?.last_updated_at ?? 0
  const cryptoUniverseAge = cryptoUniverseLastUpdated > 0
    ? formatDuration(time.getTime() - cryptoUniverseLastUpdated)
    : '—'

  const gridColumns = viewportWidth >= 1024 ? 12 : viewportWidth >= 768 ? 8 : 4
  const rowHeight = viewportWidth >= 1024 ? 110 : viewportWidth >= 768 ? 100 : 90

  const startingEquity = config?.starting_equity || 100000
  const unrealizedPl = positions.reduce((sum, p) => sum + p.unrealized_pl, 0)
  const totalPl = account ? account.equity - startingEquity : 0
  const realizedPl = totalPl - unrealizedPl
  const totalPlPct = account ? (totalPl / startingEquity) * 100 : 0

  const positionColors = ['cyan', 'purple', 'yellow', 'blue', 'green'] as const

  const positionPriceHistories = useMemo(() => {
    const histories: Record<string, number[]> = {}
    positions.forEach(pos => {
      histories[pos.symbol] = generateMockPriceHistory(pos.current_price, pos.unrealized_pl)
    })
    return histories
  }, [positions.map(p => p.symbol).join(',')])

  const portfolioChartData = useMemo(() => {
    return portfolioHistory.map(s => s.equity)
  }, [portfolioHistory])

  const portfolioChartLabels = useMemo(() => {
    return portfolioHistory.map(s => 
      new Date(s.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false })
    )
  }, [portfolioHistory])

  const normalizedPositionSeries = useMemo(() => {
    return positions.map((pos, idx) => {
      const priceHistory = positionPriceHistories[pos.symbol] || []
      if (priceHistory.length < 2) return null
      const startPrice = priceHistory[0]
      const normalizedData = priceHistory.map(price => ((price - startPrice) / startPrice) * 100)
      return {
        label: pos.symbol,
        data: normalizedData,
        variant: positionColors[idx % positionColors.length],
      }
    }).filter(Boolean) as { label: string; data: number[]; variant: typeof positionColors[number] }[]
  }, [positions, positionPriceHistories])

  const panelNodes: Record<string, JSX.Element> = {
    account: (
      <Panel title="ACCOUNT" className="h-full">
        {account ? (
          <div className="space-y-4">
            <Metric label="EQUITY" value={formatCurrency(account.equity)} size="xl" />
            <div className="grid grid-cols-2 gap-4">
              <Metric label="CASH" value={formatCurrency(account.cash)} size="md" />
              <Metric label="BUYING POWER" value={formatCurrency(account.buying_power)} size="md" />
            </div>
            <div className="pt-2 border-t border-hud-line space-y-2">
              <Metric
                label="TOTAL P&L"
                value={`${formatCurrency(totalPl)} (${formatPercent(totalPlPct)})`}
                size="md"
                color={totalPl >= 0 ? 'success' : 'error'}
              />
              <div className="grid grid-cols-2 gap-2">
                <MetricInline
                  label="REALIZED"
                  value={formatCurrency(realizedPl)}
                  color={realizedPl >= 0 ? 'success' : 'error'}
                />
                <MetricInline
                  label="UNREALIZED"
                  value={formatCurrency(unrealizedPl)}
                  color={unrealizedPl >= 0 ? 'success' : 'error'}
                />
              </div>
            </div>
          </div>
        ) : (
          <div className="text-hud-text-dim text-sm">Loading...</div>
        )}
      </Panel>
    ),
    positions: (
      <Panel title="POSITIONS" titleRight={`${positions.length}/${config?.max_positions || 5}`} className="h-full">
        {positions.length === 0 ? (
          <div className="text-hud-text-dim text-sm py-8 text-center">No open positions</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-hud-line/50">
                  <th className="hud-label text-left py-2 px-2">Symbol</th>
                  <th className="hud-label text-right py-2 px-2 hidden sm:table-cell">Qty</th>
                  <th className="hud-label text-right py-2 px-2 hidden md:table-cell">Value</th>
                  <th className="hud-label text-right py-2 px-2">P&L</th>
                  <th className="hud-label text-center py-2 px-2">Trend</th>
                  <th className="hud-label text-right py-2 px-2">Close</th>
                </tr>
              </thead>
              <tbody>
                {positions.map((pos: Position) => {
                  const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100
                  const priceHistory = positionPriceHistories[pos.symbol] || []
                  const posEntry = status?.positionEntries?.[pos.symbol]
                  const staleness = status?.stalenessAnalysis?.[pos.symbol]
                  const holdTime = posEntry ? Math.floor((Date.now() - posEntry.entry_time) / 3600000) : null
                  const isCryptoPos = isCryptoSymbol(pos.symbol, config?.crypto_symbols)
                  const canClose = isMarketOpen || isCryptoPos
                  
                  return (
                    <motion.tr 
                      key={pos.symbol}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      className="border-b border-hud-line/20 hover:bg-hud-line/10"
                    >
                      <td className="hud-value-sm py-2 px-2">
                        <Tooltip
                          position="right"
                          content={
                            <TooltipContent
                              title={pos.symbol}
                              items={[
                                { label: 'Entry Price', value: posEntry ? formatCurrency(posEntry.entry_price) : 'N/A' },
                                { label: 'Current Price', value: formatCurrency(pos.current_price) },
                                { label: 'Hold Time', value: holdTime !== null ? `${holdTime}h` : 'N/A' },
                                { label: 'Entry Sentiment', value: posEntry ? `${(posEntry.entry_sentiment * 100).toFixed(0)}%` : 'N/A' },
                                ...(staleness ? [{ 
                                  label: 'Staleness', 
                                  value: `${(staleness.score * 100).toFixed(0)}%`,
                                  color: staleness.shouldExit ? 'text-hud-error' : 'text-hud-text'
                                }] : []),
                              ]}
                              description={posEntry?.entry_reason}
                            />
                          }
                        >
                          <span className="cursor-help border-b border-dotted border-hud-text-dim">
                            {isCryptoPos && (
                              <span className="text-hud-warning mr-1">₿</span>
                            )}
                            {pos.symbol}
                          </span>
                        </Tooltip>
                      </td>
                      <td className="hud-value-sm text-right py-2 px-2 hidden sm:table-cell">{pos.qty}</td>
                      <td className="hud-value-sm text-right py-2 px-2 hidden md:table-cell">{formatCurrency(pos.market_value)}</td>
                      <td className={clsx(
                        'hud-value-sm text-right py-2 px-2',
                        pos.unrealized_pl >= 0 ? 'text-hud-success' : 'text-hud-error'
                      )}>
                        <div>{formatCurrency(pos.unrealized_pl)}</div>
                        <div className="text-xs opacity-70">{formatPercent(plPct)}</div>
                      </td>
                      <td className="py-2 px-2">
                        <div className="flex justify-center">
                          <Sparkline data={priceHistory} width={60} height={20} />
                        </div>
                      </td>
                      <td className="py-2 px-2 text-right">
                        <button
                          className="hud-button text-[10px] px-2 py-1"
                          onClick={() => handleClosePosition(pos.symbol)}
                          disabled={!canClose || closingSymbol === pos.symbol}
                          title={!canClose ? 'Market closed for equities' : 'Close position'}
                        >
                          {closingSymbol === pos.symbol ? 'Closing...' : 'Close'}
                        </button>
                      </td>
                    </motion.tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Panel>
    ),
    llm_costs: (
      <Panel title="LLM COSTS" className="h-full">
        <div className="grid grid-cols-2 gap-4">
          <Metric label="TOTAL SPENT" value={`$${costs.total_usd.toFixed(4)}`} size="lg" />
          <Metric label="API CALLS" value={costs.calls.toString()} size="lg" />
          <MetricInline label="TOKENS IN" value={costs.tokens_in.toLocaleString()} />
          <MetricInline label="TOKENS OUT" value={costs.tokens_out.toLocaleString()} />
          <MetricInline 
            label="AVG COST/CALL" 
            value={costs.calls > 0 ? `$${(costs.total_usd / costs.calls).toFixed(6)}` : '$0'} 
          />
          <MetricInline label="MODEL" value={config?.llm_model || 'gpt-4o-mini'} />
        </div>
      </Panel>
    ),
    portfolio: (
      <Panel title="PORTFOLIO PERFORMANCE" titleRight="24H" className="h-full">
        {portfolioChartData.length > 1 ? (
          <div className="h-full w-full">
            <LineChart
              series={[{ label: 'Equity', data: portfolioChartData, variant: totalPl >= 0 ? 'green' : 'red' }]}
              labels={portfolioChartLabels}
              showArea={true}
              showGrid={true}
              showDots={false}
              formatValue={(v) => `$${(v / 1000).toFixed(1)}k`}
            />
          </div>
        ) : (
          <div className="h-full flex items-center justify-center text-hud-text-dim text-sm">
            Collecting performance data...
          </div>
        )}
      </Panel>
    ),
    position_perf: (
      <Panel title="POSITION PERFORMANCE" titleRight="% CHANGE" className="h-full">
        {positions.length === 0 ? (
          <div className="h-full flex items-center justify-center text-hud-text-dim text-sm">
            No positions to display
          </div>
        ) : normalizedPositionSeries.length > 0 ? (
          <div className="h-full flex flex-col">
            <div className="flex flex-wrap gap-3 mb-2 pb-2 border-b border-hud-line/30 shrink-0">
              {positions.slice(0, 5).map((pos: Position, idx: number) => {
                const isPositive = pos.unrealized_pl >= 0
                const plPct = (pos.unrealized_pl / (pos.market_value - pos.unrealized_pl)) * 100
                const color = positionColors[idx % positionColors.length]
                return (
                  <div key={pos.symbol} className="flex items-center gap-1.5">
                    <div 
                      className="w-2 h-2 rounded-full" 
                      style={{ backgroundColor: `var(--color-hud-${color})` }}
                    />
                    <span className="hud-value-sm">{pos.symbol}</span>
                    <span className={clsx('hud-label', isPositive ? 'text-hud-success' : 'text-hud-error')}>
                      {formatPercent(plPct)}
                    </span>
                  </div>
                )
              })}
            </div>
            <div className="flex-1 min-h-0 w-full">
              <LineChart
                series={normalizedPositionSeries.slice(0, 5)}
                showArea={false}
                showGrid={true}
                showDots={false}
                animated={false}
                formatValue={(v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`}
              />
            </div>
          </div>
        ) : (
          <div className="h-full flex items-center justify-center text-hud-text-dim text-sm">
            Loading position data...
          </div>
        )}
      </Panel>
    ),
    active_signals: (
      <Panel title="ACTIVE SIGNALS" titleRight={signals.length.toString()} className="h-full">
        <div className="h-full overflow-y-auto pr-1">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-3 min-h-full">
          <div className="rounded-lg border border-hud-line/30 bg-hud-line/5 p-2 flex flex-col min-h-0">
            <div className="flex items-center justify-between border-b border-hud-line/20 pb-1 mb-2">
              <div className="flex items-center gap-2">
                <span className="hud-label text-hud-primary">STOCKS</span>
                <span className="hud-label text-hud-text-dim">{stockSignals.length}</span>
              </div>
              <span className="hud-label text-hud-text-dim">VOL / SENT</span>
            </div>
            <div className="flex-1 min-h-0 overflow-y-auto space-y-1">
              {stockSignals.length === 0 ? (
                <div className="text-hud-text-dim text-xs py-2 text-center">No stock signals yet</div>
              ) : (
                stockSignals.slice(0, 8).map((sig: Signal, i: number) => (
                  <Tooltip
                    key={`stocks-${sig.symbol}-${sig.source}-${i}`}
                    position="right"
                    content={
                      <TooltipContent
                        title={`${sig.symbol} - ${sig.source.toUpperCase()}`}
                        items={buildSignalTooltipItems(sig)}
                        description={sig.reason}
                      />
                    }
                  >
                    <motion.div
                      initial={{ opacity: 0, x: -6 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.02 }}
                      className="flex items-center justify-between gap-2 rounded-md px-2 py-1 border border-hud-line/10 hover:bg-hud-line/10 cursor-help"
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2 min-w-0">
                          <span className="hud-value-sm">{sig.symbol}</span>
                          <span className="hud-label text-hud-text-dim truncate">{sig.source.toUpperCase()}</span>
                        </div>
                        {sig.reason ? (
                          <div className="text-[10px] text-hud-text-dim truncate">{sig.reason}</div>
                        ) : null}
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="hud-label hidden sm:inline">VOL {sig.volume}</span>
                        <span className={clsx('hud-value-sm', getSentimentColor(sig.sentiment))}>
                          {(sig.sentiment * 100).toFixed(0)}%
                        </span>
                      </div>
                    </motion.div>
                  </Tooltip>
                ))
              )}
            </div>
          </div>

          <div className="rounded-lg border border-hud-line/30 bg-hud-line/5 p-2 flex flex-col min-h-0">
            <div className="flex items-center justify-between border-b border-hud-line/20 pb-1 mb-2">
              <div className="flex items-center gap-2">
                <span className="hud-label text-hud-purple">OPTIONS</span>
                <span className="hud-label text-hud-text-dim">{optionsSignals.length}</span>
              </div>
              <span className="hud-label text-hud-text-dim">VOL / SENT</span>
            </div>
            <div className="flex-1 min-h-0 overflow-y-auto space-y-1">
              {optionsSignals.length === 0 ? (
                <div className="text-hud-text-dim text-xs py-2 text-center">No options signals yet</div>
              ) : (
                optionsSignals.slice(0, 8).map((sig: Signal, i: number) => (
                  <Tooltip
                    key={`options-${sig.symbol}-${sig.source}-${i}`}
                    position="right"
                    content={
                      <TooltipContent
                        title={`${sig.symbol} - ${sig.source.toUpperCase()}`}
                        items={buildSignalTooltipItems(sig)}
                        description={sig.reason}
                      />
                    }
                  >
                    <motion.div
                      initial={{ opacity: 0, x: -6 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.02 }}
                      className="flex items-center justify-between gap-2 rounded-md px-2 py-1 border border-hud-line/10 hover:bg-hud-line/10 cursor-help"
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2 min-w-0">
                          <span className="text-hud-purple text-xs">Δ</span>
                          <span className="hud-value-sm">{sig.symbol}</span>
                          <span className="hud-label text-hud-text-dim truncate">{sig.source.toUpperCase()}</span>
                        </div>
                        {sig.reason ? (
                          <div className="text-[10px] text-hud-text-dim truncate">{sig.reason}</div>
                        ) : null}
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="hud-label hidden sm:inline">VOL {sig.volume}</span>
                        <span className={clsx('hud-value-sm', getSentimentColor(sig.sentiment))}>
                          {(sig.sentiment * 100).toFixed(0)}%
                        </span>
                      </div>
                    </motion.div>
                  </Tooltip>
                ))
              )}
            </div>
          </div>

          <div className="rounded-lg border border-hud-line/30 bg-hud-line/5 p-2 flex flex-col min-h-0">
            <div className="flex items-center justify-between border-b border-hud-line/20 pb-1 mb-2">
              <div className="flex items-center gap-2">
                <span className="hud-label text-hud-warning">CRYPTO</span>
                <span className="hud-label text-hud-text-dim">{cryptoSignals.length}</span>
              </div>
              <span className="hud-label text-hud-text-dim">MOM / SENT</span>
            </div>
            <div className="flex-1 min-h-0 overflow-y-auto space-y-1">
              {cryptoSignals.length === 0 ? (
                <div className="text-hud-text-dim text-xs py-2 text-center">No crypto signals yet</div>
              ) : (
                cryptoSignals.slice(0, 8).map((sig: Signal, i: number) => (
                  <Tooltip
                    key={`crypto-${sig.symbol}-${sig.source}-${i}`}
                    position="right"
                    content={
                      <TooltipContent
                        title={`${sig.symbol} - ${sig.source.toUpperCase()}`}
                        items={buildSignalTooltipItems(sig)}
                        description={sig.reason}
                      />
                    }
                  >
                    <motion.div
                      initial={{ opacity: 0, x: -6 }}
                      animate={{ opacity: 1, x: 0 }}
                      transition={{ delay: i * 0.02 }}
                      className="flex items-center justify-between gap-2 rounded-md px-2 py-1 border border-hud-line/10 hover:bg-hud-line/10 cursor-help"
                    >
                      <div className="min-w-0">
                        <div className="flex items-center gap-2 min-w-0">
                          <span className="text-hud-warning text-xs">₿</span>
                          <span className="hud-value-sm">{sig.symbol}</span>
                          <span className="hud-label text-hud-text-dim truncate">{sig.source.toUpperCase()}</span>
                        </div>
                        {sig.reason ? (
                          <div className="text-[10px] text-hud-text-dim truncate">{sig.reason}</div>
                        ) : null}
                      </div>
                      <div className="flex items-center gap-2">
                        {sig.momentum !== undefined ? (
                          <span className={clsx('hud-label hidden sm:inline', sig.momentum >= 0 ? 'text-hud-success' : 'text-hud-error')}>
                            {sig.momentum >= 0 ? '+' : ''}{sig.momentum.toFixed(1)}%
                          </span>
                        ) : (
                          <span className="hud-label hidden sm:inline">VOL {sig.volume}</span>
                        )}
                        <span className={clsx('hud-value-sm', getSentimentColor(sig.sentiment))}>
                          {(sig.sentiment * 100).toFixed(0)}%
                        </span>
                      </div>
                    </motion.div>
                  </Tooltip>
                ))
              )}
            </div>
          </div>

          <div className="rounded-lg border border-hud-line/30 bg-hud-line/5 p-2 flex flex-col min-h-0">
            <div className="flex items-center justify-between border-b border-hud-line/20 pb-1 mb-2">
              <div className="flex items-center gap-2">
                <span className="hud-label text-hud-cyan">{cryptoUniverseProvider}</span>
                <span
                  className={clsx(
                    'hud-label',
                    cryptoUniverseEnabled ? 'text-hud-success' : (config?.crypto_enabled ? 'text-hud-warning' : 'text-hud-text-dim')
                  )}
                >
                  {cryptoUniverseEnabled ? 'ACTIVE' : (config?.crypto_enabled ? 'MANUAL' : 'OFF')}
                </span>
              </div>
              <span className="hud-label text-hud-text-dim">UNIVERSE</span>
            </div>
            <div className="flex-1 min-h-0 grid grid-cols-2 gap-2 text-xs">
              <div className="rounded-md border border-hud-line/20 p-2">
                <div className="hud-label text-hud-text-dim">Top N</div>
                <div className="hud-value-sm">{cryptoUniverseTopN || 0}</div>
              </div>
              <div className="rounded-md border border-hud-line/20 p-2">
                <div className="hud-label text-hud-text-dim">Matched</div>
                <div className="hud-value-sm">{cryptoUniverseSize || 0}</div>
              </div>
              <div className="rounded-md border border-hud-line/20 p-2">
                <div className="hud-label text-hud-text-dim">Last Refresh</div>
                <div className="hud-value-sm">{cryptoUniverseLastUpdated ? `${cryptoUniverseAge} ago` : '—'}</div>
              </div>
              <div className="rounded-md border border-hud-line/20 p-2">
                <div className="hud-label text-hud-text-dim">Refresh Cadence</div>
                <div className="hud-value-sm">{formatDuration(cryptoUniverseRefreshMs)}</div>
              </div>
            </div>
            <div className="text-[9px] text-hud-text-dim mt-2">
              {cryptoUniverseEnabled
                ? `Pulling top market-cap coins from ${cryptoUniverseProvider} and filtering to Alpaca-tradable symbols.`
                : `Set Top-N > 0 to use ${cryptoUniverseProvider}. Otherwise, manual symbols are used.`}
            </div>
          </div>
          </div>
        </div>
      </Panel>
    ),
    activity_feed: (
      <Panel title="ACTIVITY FEED" titleRight="LIVE" className="h-full">
        <div className="flex flex-col h-full">
          <div className="flex flex-wrap items-center gap-2 pb-2 border-b border-hud-line/20">
            {(['all', 'skips', 'trades', 'errors'] as const).map((filter) => (
              <button
                key={filter}
                type="button"
                onClick={() => setLogFilter(filter)}
                className={clsx(
                  'rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-[0.2em]',
                  logFilter === filter
                    ? 'border-hud-primary/60 text-hud-primary bg-hud-primary/10'
                    : 'border-hud-line/30 text-hud-text-dim hover:text-hud-text'
                )}
              >
                {filter}
              </button>
            ))}
            {logReasonCounts.slice(0, 6).map(([reason, count]) => (
              <button
                key={reason}
                type="button"
                onClick={() => setLogReasonFilter((prev) => (prev === reason ? null : reason))}
                className={clsx(
                  'rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-[0.08em]',
                  logReasonFilter === reason
                    ? 'border-hud-warning/60 text-hud-warning bg-hud-warning/10'
                    : 'border-hud-line/30 text-hud-text-dim hover:text-hud-text'
                )}
              >
                {reason} <span className="text-[9px] text-hud-text-dim">({count})</span>
              </button>
            ))}
            {(logFilter !== 'all' || logReasonFilter) && (
              <button
                type="button"
                onClick={() => {
                  setLogFilter('all')
                  setLogReasonFilter(null)
                }}
                className="rounded-full border border-hud-line/30 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-hud-text-dim hover:text-hud-text"
              >
                Clear
              </button>
            )}
          </div>
          <div className="overflow-y-auto flex-1 font-mono text-xs space-y-1 pt-2">
            {filteredLogs.length === 0 ? (
              <div className="text-hud-text-dim py-4 text-center">
                No activity matches the current filters.
              </div>
            ) : (
              filteredLogs.slice(-50).map((log: LogEntry, i: number) => {
                const reason = typeof log.reason === 'string' ? log.reason : ''
                const confidence = typeof log.confidence === 'number' ? log.confidence : null
                const required = typeof log.required === 'number' ? log.required : null
                return (
                  <motion.div 
                    key={`${log.timestamp}-${i}`}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className="flex items-start gap-2 py-1 border-b border-hud-line/10"
                  >
                    <span className="text-hud-text-dim shrink-0 hidden sm:inline w-[52px]">
                      {new Date(log.timestamp).toLocaleTimeString('en-US', { hour12: false })}
                    </span>
                    <span className={clsx('shrink-0 w-[72px] text-right', getAgentColor(log.agent))}>
                      {log.agent}
                    </span>
                    <div className="text-hud-text flex-1 flex flex-wrap items-center justify-end gap-1">
                      <span className="wrap-break-word">
                        {log.action}
                        {log.symbol && <span className="text-hud-primary ml-1">({log.symbol})</span>}
                      </span>
                      {reason ? (
                        <span className="rounded-full border border-hud-warning/40 bg-hud-warning/10 px-2 py-0.5 text-[10px] uppercase tracking-[0.14em] text-hud-warning">
                          {reason}
                        </span>
                      ) : null}
                      {confidence !== null ? (
                        <span className="rounded-full border border-hud-line/30 px-2 py-0.5 text-[10px] text-hud-text-dim">
                          C {Math.round(confidence * 100)}%
                        </span>
                      ) : null}
                      {required !== null ? (
                        <span className="rounded-full border border-hud-line/30 px-2 py-0.5 text-[10px] text-hud-text-dim">
                          R {Math.round(required * 100)}%
                        </span>
                      ) : null}
                    </div>
                  </motion.div>
                )
              })
            )}
            <div ref={logsEndRef} />
          </div>
        </div>
      </Panel>
    ),
    signal_research: (
      <Panel title="SIGNAL RESEARCH" titleRight={Object.keys(status?.signalResearch || {}).length.toString()} className="h-full">
        <div className="overflow-y-auto h-full space-y-2">
          {Object.entries(status?.signalResearch || {}).length === 0 ? (
            <div className="text-hud-text-dim text-sm py-4 text-center">Researching candidates...</div>
          ) : (
            Object.entries(status?.signalResearch || {}).map(([symbol, research]: [string, SignalResearch]) => (
              <Tooltip
                key={symbol}
                position="left"
                content={
                  <div className="space-y-2 min-w-[200px]">
                    <div className="hud-label text-hud-primary border-b border-hud-line/50 pb-1">
                      {symbol} DETAILS
                    </div>
                    <div className="space-y-1">
                      <div className="flex justify-between">
                        <span className="text-hud-text-dim">Confidence</span>
                        <span className="text-hud-text-bright">{(research.confidence * 100).toFixed(0)}%</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-hud-text-dim">Sentiment</span>
                        <span className={getSentimentColor(research.sentiment)}>
                          {(research.sentiment * 100).toFixed(0)}%
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-hud-text-dim">Analyzed</span>
                        <span className="text-hud-text">
                          {new Date(research.timestamp).toLocaleTimeString('en-US', { hour12: false })}
                        </span>
                      </div>
                    </div>
                    {research.catalysts.length > 0 && (
                      <div className="pt-1 border-t border-hud-line/30">
                        <span className="text-[9px] text-hud-text-dim">CATALYSTS:</span>
                        <ul className="mt-1 space-y-0.5">
                          {research.catalysts.map((c, i) => (
                            <li key={i} className="text-[10px] text-hud-success">+ {c}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                    {research.red_flags.length > 0 && (
                      <div className="pt-1 border-t border-hud-line/30">
                        <span className="text-[9px] text-hud-text-dim">RED FLAGS:</span>
                        <ul className="mt-1 space-y-0.5">
                          {research.red_flags.map((f, i) => (
                            <li key={i} className="text-[10px] text-hud-error">- {f}</li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                }
              >
                <motion.div 
                  initial={{ opacity: 0, x: 10 }}
                  animate={{ opacity: 1, x: 0 }}
                  className="border border-hud-line/30 p-2 hover:bg-hud-line/10 cursor-help"
                >
                  <div className="flex justify-between items-center mb-1">
                    <span className="hud-value-sm">{symbol}</span>
                    <span className={clsx('hud-label', getVerdictColor(research.verdict))}>{research.verdict}</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className={clsx('text-xs', getQualityColor(research.entry_quality))}>{research.entry_quality}</span>
                    <span className="text-xs text-hud-text-dim">{(research.confidence * 100).toFixed(0)}% confidence</span>
                  </div>
                </motion.div>
              </Tooltip>
            ))
          )}
        </div>
      </Panel>
    ),
  }

  // Early returns (after all hooks)
  if (showSetup) {
    return <SetupWizard onComplete={() => setShowSetup(false)} />
  }

  if (error && !status) {
    const isAuthError = error.includes('Unauthorized')
    return (
      <div className="min-h-screen bg-hud-bg flex items-center justify-center p-6">
        <Panel title={isAuthError ? "AUTHENTICATION REQUIRED" : "CONNECTION ERROR"} className="max-w-md w-full">
          <div className="text-center py-8">
            <div className="text-hud-error text-2xl mb-4">{isAuthError ? "NO TOKEN" : "OFFLINE"}</div>
            <p className="text-hud-text-dim text-sm mb-6">{error}</p>
            {isAuthError ? (
              <div className="space-y-4">
                <div className="text-left bg-hud-panel p-4 border border-hud-line">
                  <label className="hud-label block mb-2">API Token</label>
                  <input
                    type="password"
                    className="hud-input w-full mb-2"
                    placeholder="Enter MAHORAGA_API_TOKEN"
                    defaultValue={localStorage.getItem('mahoraga_api_token') || ''}
                    onChange={(e) => localStorage.setItem('mahoraga_api_token', e.target.value)}
                  />
                  <button 
                    onClick={() => window.location.reload()}
                    className="hud-button w-full"
                  >
                    Save & Reload
                  </button>
                </div>
                <p className="text-hud-text-dim text-xs">
                  Find your token in <code className="text-hud-primary">.dev.vars</code> (local) or Cloudflare secrets (deployed)
                </p>
              </div>
            ) : (
              <p className="text-hud-text-dim text-xs">
                Enable the agent: <code className="text-hud-primary">curl -H "Authorization: Bearer $TOKEN" localhost:8787/agent/enable</code>
              </p>
            )}
          </div>
        </Panel>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-hud-bg">
      <div className="max-w-[1920px] mx-auto p-4">
        <header className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3 mb-4 pb-3 border-b border-hud-line">
          <div className="flex items-center gap-4 md:gap-6">
            <div className="flex items-baseline gap-2">
              <span className="text-xl md:text-2xl font-light tracking-tight text-hud-text-bright">
                MAHORAGA
              </span>
              <span className="hud-label">v2</span>
            </div>
            <StatusIndicator 
              status={isMarketOpen ? 'active' : 'inactive'} 
              label={isMarketOpen ? 'MARKET OPEN' : 'MARKET CLOSED'}
              pulse={isMarketOpen}
            />
          </div>
          <div className="flex items-center gap-3 md:gap-6 flex-wrap">
            <StatusBar
              items={[
                { label: 'LLM COST', value: `$${costs.total_usd.toFixed(4)}`, status: costs.total_usd > 1 ? 'warning' : 'active' },
                { label: 'API CALLS', value: costs.calls.toString() },
              ]}
            />
            <NotificationBell 
              overnightActivity={status?.overnightActivity}
              premarketPlan={status?.premarketPlan}
            />
            <button 
              className={clsx(
                "hud-label transition-colors",
                layoutEdit ? "text-hud-primary" : "hover:text-hud-primary"
              )}
              onClick={() => setLayoutEdit((prev) => !prev)}
            >
              {layoutEdit ? '[DONE]' : '[LAYOUT]'}
            </button>
            {layoutEdit && (
              <button 
                className="hud-label hover:text-hud-warning transition-colors"
                onClick={resetLayout}
              >
                [RESET]
              </button>
            )}
            <button 
              className="hud-label hover:text-hud-primary transition-colors"
              onClick={() => setShowSettings(true)}
            >
              [CONFIG]
            </button>
            <span className="hud-value-sm font-mono">
              {time.toLocaleTimeString('en-US', { hour12: false })}
            </span>
          </div>
        </header>

        <div
          className="grid grid-cols-4 md:grid-cols-8 lg:grid-cols-12 gap-4"
          style={{ gridAutoRows: `${rowHeight}px` }}
        >
          {layout.map((item) => {
            const panel = panelNodes[item.id]
            if (!panel) return null
            const colSpan = gridColumns >= 12 ? item.colSpan : gridColumns
            const rowSpan = item.rowSpan
            const isDropTarget = layoutEdit && dropTarget === item.id && draggingPanel && draggingPanel !== item.id

            return (
              <div
                key={item.id}
                className={clsx(
                  'relative min-w-0',
                  layoutEdit && 'ring-1 ring-hud-primary/20',
                  isDropTarget && 'ring-2 ring-hud-primary/60'
                )}
                style={{ gridColumnEnd: `span ${colSpan}`, gridRowEnd: `span ${rowSpan}` }}
                onDragOver={(event) => {
                  if (!layoutEdit) return
                  event.preventDefault()
                  if (draggingPanel && draggingPanel !== item.id) {
                    setDropTarget(item.id)
                  }
                }}
                onDragLeave={() => {
                  if (dropTarget === item.id) setDropTarget(null)
                }}
                onDrop={(event) => {
                  if (!layoutEdit) return
                  event.preventDefault()
                  const sourceId = event.dataTransfer.getData('text/plain')
                  if (sourceId) {
                    movePanel(sourceId, item.id)
                  }
                  setDropTarget(null)
                  setDraggingPanel(null)
                }}
              >
                {layoutEdit && (
                  <>
                    <div className="absolute top-2 left-2 z-20 flex items-center gap-2">
                      <div
                        className={clsx(
                          'hud-label text-[10px] px-2 py-1 rounded bg-hud-line/60 text-hud-text-dim cursor-grab active:cursor-grabbing',
                          draggingPanel === item.id && 'text-hud-primary'
                        )}
                        draggable
                        onDragStart={(event) => {
                          event.dataTransfer.setData('text/plain', item.id)
                          event.dataTransfer.effectAllowed = 'move'
                          setDraggingPanel(item.id)
                        }}
                        onDragEnd={() => {
                          setDraggingPanel(null)
                          setDropTarget(null)
                        }}
                      >
                        MOVE
                      </div>
                      <span className="hud-label text-[9px] text-hud-text-dim">{item.colSpan}x{item.rowSpan}</span>
                    </div>
                    <div className="absolute top-2 right-2 z-20 flex items-center gap-1 bg-hud-line/60 backdrop-blur rounded px-1 py-1">
                      <button
                        className="hud-button text-[9px] px-1.5 py-0.5"
                        onClick={() => resizePanel(item.id, -1, 0)}
                        title="Decrease width"
                      >
                        -W
                      </button>
                      <button
                        className="hud-button text-[9px] px-1.5 py-0.5"
                        onClick={() => resizePanel(item.id, 1, 0)}
                        title="Increase width"
                      >
                        +W
                      </button>
                      <button
                        className="hud-button text-[9px] px-1.5 py-0.5"
                        onClick={() => resizePanel(item.id, 0, -1)}
                        title="Decrease height"
                      >
                        -H
                      </button>
                      <button
                        className="hud-button text-[9px] px-1.5 py-0.5"
                        onClick={() => resizePanel(item.id, 0, 1)}
                        title="Increase height"
                      >
                        +H
                      </button>
                    </div>
                  </>
                )}
                {panel}
              </div>
            )
          })}
        </div>

        <footer className="mt-4 pt-3 border-t border-hud-line flex flex-col sm:flex-row justify-between items-start sm:items-center gap-3">
          <div className="flex flex-wrap gap-4 md:gap-6">
            {config && (
              <>
                <MetricInline label="MAX POS" value={`$${config.max_position_value}`} />
                <MetricInline label="MIN SENT" value={`${(config.min_sentiment_score * 100).toFixed(0)}%`} />
                <MetricInline label="TAKE PROFIT" value={`${config.take_profit_pct}%`} />
                <MetricInline label="STOP LOSS" value={`${config.stop_loss_pct}%`} />
                <span className="hidden lg:inline text-hud-line">|</span>
                <MetricInline 
                  label="OPTIONS" 
                  value={config.options_enabled ? 'ON' : 'OFF'} 
                  valueClassName={config.options_enabled ? 'text-hud-purple' : 'text-hud-text-dim'}
                />
                {config.options_enabled && (
                  <>
                    <MetricInline label="OPT Δ" value={config.options_target_delta?.toFixed(2) || '0.35'} />
                    <MetricInline label="OPT DTE" value={`${config.options_min_dte || 7}-${config.options_max_dte || 45}`} />
                  </>
                )}
                <span className="hidden lg:inline text-hud-line">|</span>
                <MetricInline 
                  label="CRYPTO" 
                  value={config.crypto_enabled ? '24/7' : 'OFF'} 
                  valueClassName={config.crypto_enabled ? 'text-hud-warning' : 'text-hud-text-dim'}
                />
                {config.crypto_enabled && (
                  <MetricInline label="SYMBOLS" value={(config.crypto_symbols || ['BTC', 'ETH', 'SOL']).map(s => s.split('/')[0]).join('/')} />
                )}
              </>
            )}
          </div>
          <div className="flex items-center gap-4">
            <span className="hud-label hidden md:inline">AUTONOMOUS TRADING SYSTEM</span>
            <span className="hud-value-sm">PAPER MODE</span>
          </div>
        </footer>
      </div>

      <AnimatePresence>
        {showSettings && config && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <SettingsModal 
              config={config} 
              onSave={handleSaveConfig} 
              onClose={() => setShowSettings(false)} 
            />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

