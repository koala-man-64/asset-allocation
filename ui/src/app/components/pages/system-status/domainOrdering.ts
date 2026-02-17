import type { DataLayer } from '@/types/strategy';

import { normalizeDomainKey } from './SystemPurgeControls';

export type DomainOrderEntry = {
  key: string;
  label: string;
};

const getDomainLabel = (value: string): string => {
  const domainName = String(value || '').trim();
  return domainName ? normalizeDomainKey(domainName) : '';
};

type DomainOrderRecord = {
  key: string;
  label: string;
  index: number;
};

const domainLabelCollator = new Intl.Collator(undefined, {
  sensitivity: 'base',
  numeric: true
});

export const getDomainOrderEntries = (dataLayers: DataLayer[]): DomainOrderEntry[] => {
  const orderIndex = new Map<string, DomainOrderRecord>();

  let seen = 0;
  for (const layer of dataLayers || []) {
    for (const domain of layer.domains || []) {
      const label = String(domain?.name || '').trim();
      if (!label) continue;
      const key = getDomainLabel(label);
      if (!key || orderIndex.has(key)) continue;
      orderIndex.set(key, { key, label, index: seen++ });
    }
  }

  return Array.from(orderIndex.values())
    .sort((a, b) => {
      const cmp = domainLabelCollator.compare(a.key, b.key);
      if (cmp !== 0) return cmp;
      return a.index - b.index;
    })
    .map(({ key, label }) => ({ key, label }));
};

export const getDomainOrderIndex = (dataLayers: DataLayer[]): Map<string, number> => {
  const entries = getDomainOrderEntries(dataLayers);
  const index = new Map<string, number>();
  entries.forEach((entry, position) => {
    index.set(entry.key, position);
  });
  return index;
};
