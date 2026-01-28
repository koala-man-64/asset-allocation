import React, { useMemo } from 'react';

interface DataTableProps {
    data: Record<string, unknown>[];
    className?: string;
}

export const DataTable: React.FC<DataTableProps> = ({ data, className = '' }) => {
    const columns = useMemo(() => {
        if (!data || data.length === 0) return [];
        return Object.keys(data[0]);
    }, [data]);

    if (!data || data.length === 0) {
        return (
            <div className={`p-4 text-sm text-gray-500 font-mono border border-gray-200 bg-gray-50 ${className}`}>
                No data available.
            </div>
        );
    }

    return (
        <div className={`overflow-x-auto border border-gray-300 ${className}`}>
            <table className="min-w-full divide-y divide-gray-300 font-mono text-xs">
                <thead className="bg-gray-100">
                    <tr className="divide-x divide-gray-300">
                        <th className="px-3 py-2 text-left font-semibold text-gray-700 w-12 bg-gray-200">
                            #
                        </th>
                        {columns.map((col) => (
                            <th
                                key={col}
                                className="px-3 py-2 text-left font-semibold text-gray-700 whitespace-nowrap"
                            >
                                {col}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 bg-white">
                    {data.map((row, idx) => (
                        <tr key={idx} className="divide-x divide-gray-200 hover:bg-yellow-50 transition-colors">
                            <td className="px-3 py-1.5 text-gray-500 bg-gray-50 border-r border-gray-200 text-right select-none">
                                {idx + 1}
                            </td>
                            {columns.map((col) => {
                                const val = row[col];
                                let displayVal: React.ReactNode = '-';

                                if (val !== null && val !== undefined) {
                                    if (typeof val === 'object') {
                                        displayVal = JSON.stringify(val);
                                    } else if (typeof val === 'boolean') {
                                        displayVal = val ? 'TRUE' : 'FALSE';
                                    } else {
                                        displayVal = String(val);
                                    }
                                }

                                return (
                                    <td key={col} className="px-3 py-1.5 text-gray-800 whitespace-nowrap">
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
