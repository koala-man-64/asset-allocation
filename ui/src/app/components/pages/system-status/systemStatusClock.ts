export function resolveCentralTimeZoneLabel(date: Date): string {
  const tzRaw =
    new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Chicago',
      timeZoneName: 'short'
    })
      .formatToParts(date)
      .find((part) => part.type === 'timeZoneName')?.value ?? '';

  const value = String(tzRaw || '').trim();
  if (!value) return 'CST';
  if (value === 'CST' || value === 'CDT') return value;
  if (/central.*daylight/i.test(value)) return 'CDT';
  if (/central.*standard/i.test(value)) return 'CST';

  const offsetMatch = value.match(/(?:GMT|UTC)([+-]\d{1,2})(?::?(\d{2}))?/i);
  if (!offsetMatch) return 'CST';

  const hours = Number.parseInt(offsetMatch[1] || '0', 10);
  const minutes = Number.parseInt(offsetMatch[2] || '0', 10);
  const total = hours * 60 + (hours < 0 ? -minutes : minutes);
  if (total === -360) return 'CST';
  if (total === -300) return 'CDT';
  return 'CST';
}

export function formatMetadataTimestamp(value?: string | null): string {
  if (!value) return 'N/A';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'N/A';
  const stamp = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Chicago',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  }).format(date);
  return `${stamp} ${resolveCentralTimeZoneLabel(date)}`;
}

export function getCentralClockParts(date: Date): { time: string; tz: string } {
  return {
    time: new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Chicago',
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(date),
    tz: resolveCentralTimeZoneLabel(date)
  };
}
