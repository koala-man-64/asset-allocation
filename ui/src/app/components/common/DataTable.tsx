import React, { useMemo } from 'react';

interface DataTableProps<T> {
  data: T[];
  className?: string;
  emptyMessage?: string;
  columns?: { header: string; accessorKey: keyof T | string }[];
  onRowClick?: (item: T) => void;
}

export const DataTable = <T extends Record<string, unknown>>({
  data,
  className = '',
  emptyMessage = 'No data available.',
  columns: propColumns,
  onRowClick
}: DataTableProps<T>) => {
  const tableColumns = useMemo(() => {
    if (propColumns) {
      return propColumns;
    }
    if (!data || data.length === 0) return [];
    return Object.keys(data[0]).map((key) => ({ header: key, accessorKey: key }));
  }, [data, propColumns]);

  if (!data || data.length === 0) {
    return (
      <div className={`mcm-panel p-4 text-xs text-mcm-olive font-body italic ${className}`}>
        {emptyMessage}
      </div>
    );
  }

  return (
    <div
      className={`overflow-x-auto rounded-2xl border-2 border-mcm-walnut bg-mcm-paper p-2 shadow-[8px_8px_0px_0px_rgba(119,63,26,0.08)] ${className}`}
    >
      <table className="min-w-full border-separate border-spacing-y-2 text-xs font-body">
        <thead>
          <tr>
            <th className="px-3 py-2 text-left text-[10px] font-black uppercase tracking-widest text-mcm-walnut/70 w-12">
              #
            </th>
            {tableColumns.map((col) => (
              <th
                key={String(col.accessorKey)}
                className="px-3 py-2 text-left text-[10px] font-black uppercase tracking-widest text-mcm-walnut/70 whitespace-nowrap"
              >
                {col.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, idx) => (
            <tr
              key={idx}
              className={`group transition-colors hover:[&>td]:bg-mcm-cream ${onRowClick ? 'cursor-pointer' : ''}`}
              onClick={() => onRowClick?.(row)}
            >
              <td className="px-3 py-2 text-mcm-olive bg-mcm-cream border-y-2 border-mcm-walnut/40 border-l-2 border-mcm-walnut/40 rounded-l-2xl text-right select-none text-[11px] font-semibold">
                {idx + 1}
              </td>
              {tableColumns.map((col) => {
                const val = row[col.accessorKey as keyof T];
                let displayVal: React.ReactNode = '-';

                if (val !== null && val !== undefined) {
                  if (typeof val === 'object') {
                    displayVal = JSON.stringify(val);
                  } else if (typeof val === 'boolean') {
                    displayVal = val ? 'true' : 'false';
                  } else {
                    displayVal = String(val);
                  }
                }

                return (
                  <td
                    key={String(col.accessorKey)}
                    className="px-3 py-2 text-mcm-walnut border-y-2 border-mcm-walnut/40 bg-mcm-paper whitespace-nowrap lowercase last:border-r-2 last:border-mcm-walnut/40 last:rounded-r-2xl"
                  >
                    {displayVal}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
