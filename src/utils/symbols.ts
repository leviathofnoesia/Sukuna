export function normalizeSymbol(symbol: string): string {
  return symbol.trim().toUpperCase();
}

export function cryptoSymbolKey(symbol: string): string {
  return normalizeSymbol(symbol).replace("/", "");
}

export function buildCryptoSymbolMap(cryptoSymbols?: string[]): Map<string, string> {
  const map = new Map<string, string>();
  for (const symbol of cryptoSymbols ?? []) {
    const normalized = normalizeSymbol(symbol);
    const compact = normalized.replace("/", "");
    map.set(compact, normalized);
    if (normalized.includes("/")) {
      const base = normalized.split("/")[0] ?? normalized;
      map.set(base, normalized);
    } else if (normalized.endsWith("USD") && normalized.length > 3) {
      map.set(normalized.slice(0, -3), normalized);
    } else {
      map.set(normalized, normalized);
    }
  }
  return map;
}

export function normalizeCryptoSymbol(symbol: string, cryptoSymbols?: string[]): string {
  const normalized = normalizeSymbol(symbol);
  if (normalized.includes("/")) return normalized;
  const map = buildCryptoSymbolMap(cryptoSymbols);
  return map.get(normalized) ?? normalized;
}

export function toSlashUsdSymbol(symbol: string): string | null {
  const normalized = normalizeSymbol(symbol);
  if (normalized.includes("/")) return normalized;
  if (normalized.endsWith("USD") && normalized.length > 3) {
    return `${normalized.slice(0, -3)}/USD`;
  }
  return null;
}
