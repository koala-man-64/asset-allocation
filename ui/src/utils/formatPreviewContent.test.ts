import { describe, expect, it } from 'vitest';

import { formatPreviewContent } from './formatPreviewContent';

describe('formatPreviewContent', () => {
  it('pretty-prints a standard JSON document', () => {
    const formatted = formatPreviewContent('{"name":"gold","enabled":true}', {
      path: 'market/config.json',
      contentType: 'application/octet-stream'
    });

    expect(formatted).toBe('{\n  "name": "gold",\n  "enabled": true\n}');
  });

  it('pretty-prints newline-delimited JSON entries', () => {
    const formatted = formatPreviewContent('{"id":1,"name":"a"}\n{"id":2,"name":"b"}', {
      path: 'earnings/_delta_log/00000000000000000000.json'
    });

    expect(formatted).toBe('{\n  "id": 1,\n  "name": "a"\n}\n{\n  "id": 2,\n  "name": "b"\n}');
  });

  it('leaves non-JSON plaintext unchanged', () => {
    const source = 'alpha,beta,gamma\n1,2,3';

    expect(
      formatPreviewContent(source, { path: 'market/sample.csv', contentType: 'text/csv' })
    ).toBe(source);
  });

  it('preserves non-parseable lines while formatting valid JSON lines', () => {
    const formatted = formatPreviewContent('{"id":1}\npartial-json', {
      path: 'market/truncated.json'
    });

    expect(formatted).toBe('{\n  "id": 1\n}\npartial-json');
  });
});
