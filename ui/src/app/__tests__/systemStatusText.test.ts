import { describe, expect, it } from 'vitest';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

describe('formatSystemStatusText', () => {
  it('extracts detail from API error payloads', () => {
    const raw =
      'API Error: 500 Internal Server Error [requestId=req-123] - {"detail":"Metadata unavailable for market"}';

    expect(formatSystemStatusText(raw)).toBe('Metadata unavailable for market');
  });

  it('strips ANSI markers from JSON-wrapped log lines', () => {
    const raw =
      '- {"detail":"Domain metadata unavailable: Kernel error -> \\u001b[31mpermission denied\\u001b[0m for path market-data/AAPL/_delta_log/_last_checkpoint\\nError performing GET"}';

    expect(formatSystemStatusText(raw)).toBe(
      'Domain metadata unavailable: Kernel error -> permission denied for path market-data/AAPL/_delta_log/_last_checkpoint Error performing GET'
    );
  });

  it('normalizes plain text messages', () => {
    expect(formatSystemStatusText('  plain failure  \n')).toBe('plain failure');
  });
});
