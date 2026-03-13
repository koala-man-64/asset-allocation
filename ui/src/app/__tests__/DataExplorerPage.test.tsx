import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '@/test/utils';

import { DataExplorerPage } from '@/app/components/pages/DataExplorerPage';
import { DataService } from '@/services/DataService';

vi.mock('@/services/DataService', () => ({
  DataService: {
    getAdlsTree: vi.fn(),
    getAdlsFilePreview: vi.fn()
  }
}));

describe('DataExplorerPage', () => {
  beforeAll(() => {
    if (!Element.prototype.scrollIntoView) {
      Element.prototype.scrollIntoView = () => undefined;
    }
    if (!Element.prototype.hasPointerCapture) {
      Element.prototype.hasPointerCapture = () => false;
    }
    if (!Element.prototype.setPointerCapture) {
      Element.prototype.setPointerCapture = () => undefined;
    }
    if (!Element.prototype.releasePointerCapture) {
      Element.prototype.releasePointerCapture = () => undefined;
    }
  });

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(DataService.getAdlsTree).mockResolvedValue({
      layer: 'gold',
      container: 'gold',
      path: 'market/buckets/',
      truncated: false,
      scanLimit: 5000,
      entries: [
        {
          type: 'file',
          name: 'part-00000.snappy.parquet',
          path: 'market/buckets/A/part-00000.snappy.parquet',
          size: 1024,
          lastModified: null,
          contentType: 'application/octet-stream'
        }
      ]
    });
    vi.mocked(DataService.getAdlsFilePreview).mockResolvedValue({
      layer: 'gold',
      container: 'gold',
      path: 'market/buckets/A/part-00000.snappy.parquet',
      isPlainText: false,
      encoding: null,
      truncated: false,
      maxBytes: 262144,
      contentType: 'application/x-delta-table-preview',
      contentPreview: null,
      previewMode: 'delta-table',
      processedDeltaFiles: null,
      maxDeltaFiles: 0,
      deltaLogPath: 'market/buckets/A/_delta_log/',
      tableColumns: ['symbol', 'close'],
      tableRows: [{ symbol: 'AAPL', close: 101.25 }],
      tableRowCount: 1,
      tablePreviewLimit: 100,
      tableTruncated: false,
      resolvedTablePath: 'market/buckets/A',
      tableVersion: null
    });
  });

  it('renders delta-backed previews as a table', async () => {
    renderWithProviders(<DataExplorerPage />);

    const fileButton = await screen.findByRole('button', { name: /part-00000\.snappy\.parquet/i });
    fireEvent.click(fileButton);

    expect(await screen.findByRole('table')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: /symbol/i })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: /close/i })).toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('101.25')).toBeInTheDocument();
    expect(screen.getByText(/delta snapshot/i)).toBeInTheDocument();
  });

  it('reloads the tree when the domain filter changes', async () => {
    const user = userEvent.setup();

    renderWithProviders(<DataExplorerPage />);

    await waitFor(() => {
      expect(DataService.getAdlsTree).toHaveBeenCalledWith({
        layer: 'gold',
        path: 'market/buckets/',
        maxEntries: 5000
      });
    });

    await user.click(screen.getByRole('combobox', { name: /domain/i }));
    await user.click(await screen.findByRole('option', { name: 'Earnings' }));

    await waitFor(() => {
      expect(DataService.getAdlsTree).toHaveBeenLastCalledWith({
        layer: 'gold',
        path: 'earnings/buckets/',
        maxEntries: 5000
      });
    });
  });

  it('uses the finance domain root for gold instead of jumping straight to buckets', async () => {
    const user = userEvent.setup();

    renderWithProviders(<DataExplorerPage />);

    await user.click(screen.getByRole('combobox', { name: /domain/i }));
    await user.click(await screen.findByRole('option', { name: 'Finance' }));

    await waitFor(() => {
      expect(DataService.getAdlsTree).toHaveBeenLastCalledWith({
        layer: 'gold',
        path: 'finance/',
        maxEntries: 5000
      });
    });
  });

  it('refreshes loaded metadata folders and the selected preview on demand', async () => {
    const user = userEvent.setup();

    vi.mocked(DataService.getAdlsTree).mockImplementation(async ({ path }) => {
      if (path === 'market/buckets/') {
        return {
          layer: 'gold',
          container: 'gold',
          path: 'market/buckets/',
          truncated: false,
          scanLimit: 5000,
          entries: [
            {
              type: 'folder',
              name: '_metadata',
              path: 'market/buckets/_metadata/',
              size: null,
              lastModified: null
            }
          ]
        };
      }

      if (path === 'market/buckets/_metadata/') {
        return {
          layer: 'gold',
          container: 'gold',
          path: 'market/buckets/_metadata/',
          truncated: false,
          scanLimit: 5000,
          entries: [
            {
              type: 'file',
              name: 'domain.json',
              path: 'market/buckets/_metadata/domain.json',
              size: 256,
              lastModified: null,
              contentType: 'application/json'
            }
          ]
        };
      }

      return {
        layer: 'gold',
        container: 'gold',
        path: path ?? '',
        truncated: false,
        scanLimit: 5000,
        entries: []
      };
    });

    vi.mocked(DataService.getAdlsFilePreview).mockResolvedValue({
      layer: 'gold',
      container: 'gold',
      path: 'market/buckets/_metadata/domain.json',
      isPlainText: true,
      encoding: 'utf-8',
      truncated: false,
      maxBytes: 262144,
      contentType: 'application/json',
      contentPreview: '{"updatedAt":"2026-03-13T12:00:00Z"}',
      previewMode: 'blob',
      processedDeltaFiles: null,
      maxDeltaFiles: 0,
      deltaLogPath: null,
      tableColumns: [],
      tableRows: [],
      tableRowCount: 0,
      tablePreviewLimit: 0,
      tableTruncated: false,
      resolvedTablePath: null,
      tableVersion: null
    });

    renderWithProviders(<DataExplorerPage />);

    await user.click(await screen.findByRole('button', { name: /_metadata/i }));
    await user.click(await screen.findByRole('button', { name: /domain\.json/i }));

    await waitFor(() => {
      expect(DataService.getAdlsTree).toHaveBeenCalledWith({
        layer: 'gold',
        path: 'market/buckets/_metadata/',
        maxEntries: 5000
      });
    });

    await waitFor(() => {
      expect(DataService.getAdlsFilePreview).toHaveBeenCalledWith({
        layer: 'gold',
        path: 'market/buckets/_metadata/domain.json',
        maxBytes: 262144,
        maxDeltaFiles: 0
      });
    });

    await user.click(screen.getByRole('button', { name: /refresh metadata/i }));

    await waitFor(() => {
      expect(DataService.getAdlsTree).toHaveBeenCalledTimes(4);
    });
    expect(vi.mocked(DataService.getAdlsTree).mock.calls[2]?.[0]).toEqual({
      layer: 'gold',
      path: 'market/buckets/',
      maxEntries: 5000
    });
    expect(vi.mocked(DataService.getAdlsTree).mock.calls[3]?.[0]).toEqual({
      layer: 'gold',
      path: 'market/buckets/_metadata/',
      maxEntries: 5000
    });

    await waitFor(() => {
      expect(DataService.getAdlsFilePreview).toHaveBeenCalledTimes(2);
    });
    expect(vi.mocked(DataService.getAdlsFilePreview).mock.calls[1]?.[0]).toEqual({
      layer: 'gold',
      path: 'market/buckets/_metadata/domain.json',
      maxBytes: 262144,
      maxDeltaFiles: 0
    });
  });
});
