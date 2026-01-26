import React, { useState, useMemo } from 'react';
import { useSystemHealthQuery } from '@/hooks/useDataQueries';
import { StatusTypos, StatusColors } from './system-status/StatusTokens';
import { getStatusConfig, formatTimeAgo, formatDuration } from './system-status/SystemStatusHelpers';
import {
    Database,
    Folder,
    FolderOpen,
    FileText,
    Activity,
    Search,
    ChevronRight,
    ChevronDown,
    Server,
    ExternalLink,
    Clock,
    RotateCw,
    AlertCircle,
    CheckCircle2
} from 'lucide-react';
import { DataLayer, DataDomain } from '@/types/strategy';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';

export function DataExplorerPage() {
    const { data, isLoading, error } = useSystemHealthQuery();
    const [selectedLayerName, setSelectedLayerName] = useState<string | null>(null);
    const [selectedDomainName, setSelectedDomainName] = useState<string | null>(null);

    // Default selection strategy: Select first layer if nothing selected
    React.useEffect(() => {
        if (data?.dataLayers?.length && !selectedLayerName) {
            setSelectedLayerName(data.dataLayers[0].name);
        }
    }, [data, selectedLayerName]);

    const selectedLayer = useMemo(() =>
        data?.dataLayers?.find(l => l.name === selectedLayerName),
        [data, selectedLayerName]);

    const selectedDomain = useMemo(() =>
        selectedLayer?.domains?.find(d => d.name === selectedDomainName),
        [selectedLayer, selectedDomainName]);

    if (isLoading) {
        return (
            <div className="flex items-center justify-center h-full min-h-[400px]">
                <div className="flex flex-col items-center gap-4 text-slate-400">
                    <RotateCw className="h-8 w-8 animate-spin" />
                    <span className="font-mono text-sm uppercase tracking-widest">Loading Data Map...</span>
                </div>
            </div>
        );
    }

    if (error || !data) {
        return (
            <div className="p-8 flex flex-col items-center justify-center text-rose-500">
                <AlertCircle className="h-12 w-12 mb-4" />
                <h2 className={StatusTypos.HEADER}>SYSTEM LINK ERROR</h2>
                <p className="font-mono text-sm mt-2">Failed to retrieve data topology.</p>
            </div>
        );
    }

    return (
        <div className="flex h-[calc(100vh-6rem)] border border-slate-200 bg-white shadow-sm overflow-hidden font-sans">
            {/* LEFT SIDEBAR: Tree View */}
            <div className="w-1/3 min-w-[300px] max-w-[400px] flex flex-col border-r border-slate-200 bg-slate-50">
                <div className="p-4 border-b border-slate-200 bg-white">
                    <h2 className={`${StatusTypos.HEADER} mb-1`}>DATA TOPOLOGY</h2>
                    <div className="relative">
                        <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-slate-400" />
                        <input
                            type="text"
                            placeholder="Filter layers or domains..."
                            className="w-full pl-8 pr-3 py-1.5 text-xs bg-slate-100 border-transparent focus:bg-white focus:border-indigo-500 focus:ring-0 rounded-sm font-mono placeholder:text-slate-400 transition-all"
                        />
                    </div>
                </div>

                <div className="flex-1 overflow-y-auto p-2 space-y-1">
                    {data.dataLayers.map(layer => {
                        const isLayerSelected = layer.name === selectedLayerName;
                        const statusConfig = getStatusConfig(layer.status);

                        return (
                            <div key={layer.name} className="space-y-0.5">
                                {/* Layer Item */}
                                <button
                                    onClick={() => {
                                        setSelectedLayerName(layer.name);
                                        setSelectedDomainName(null); // Reset domain selection when switching layers
                                    }}
                                    className={`w-full flex items-center gap-2 px-3 py-2 text-xs font-medium rounded-sm border transition-all group ${isLayerSelected
                                        ? 'bg-white border-slate-200 shadow-sm text-slate-900'
                                        : 'bg-transparent border-transparent text-slate-600 hover:bg-slate-200/50'
                                        }`}
                                >
                                    <div
                                        className={`w-1.5 h-1.5 rounded-sm ${isLayerSelected ? 'scale-110' : ''}`}
                                        style={{ backgroundColor: statusConfig.text }}
                                    />
                                    <Database className={`h-4 w-4 ${isLayerSelected ? 'text-indigo-600' : 'text-slate-400 group-hover:text-slate-500'}`} />
                                    <span className="flex-1 text-left uppercase tracking-wide">{layer.name}</span>
                                    {isLayerSelected && <ChevronRight className="h-3.5 w-3.5 text-slate-400" />}
                                </button>

                                {/* Domain List (Visible if layer is selected or implicitly always expanded in this design) */}
                                {isLayerSelected && (
                                    <div className="pl-6 space-y-0.5 py-1">
                                        {(layer.domains || []).map(domain => {
                                            const isDomainSelected = domain.name === selectedDomainName;
                                            const dStatus = getStatusConfig(domain.status);

                                            return (
                                                <button
                                                    key={domain.name}
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        setSelectedDomainName(domain.name);
                                                    }}
                                                    className={`w-full flex items-center gap-2 px-3 py-1.5 text-xs rounded-sm border-l-2 transition-colors ${isDomainSelected
                                                        ? 'bg-indigo-50 border-indigo-500 text-indigo-700 font-medium'
                                                        : 'bg-transparent border-transparent text-slate-500 hover:text-slate-800 hover:bg-slate-100'
                                                        }`}
                                                >
                                                    <Folder className={`h-3.5 w-3.5 ${isDomainSelected ? 'text-indigo-500' : 'text-slate-300'}`} />
                                                    <span className="truncate">{domain.name}</span>
                                                    <div className="ml-auto w-1 h-1 rounded-full" style={{ backgroundColor: dStatus.text }} />
                                                </button>
                                            )
                                        })}
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>

                <div className="p-3 border-t border-slate-200 bg-slate-100 text-[10px] text-slate-500 font-mono text-center">
                    {(data.dataLayers.length)} LAYERS • {data.dataLayers.reduce((acc, l) => acc + (l.domains?.length || 0), 0)} DOMAINS
                </div>
            </div>

            {/* RIGHT PANEL: Details View */}
            <div className="flex-1 bg-white overflow-y-auto">
                {!selectedLayer ? (
                    <div className="h-full flex flex-col items-center justify-center text-slate-300">
                        <Server className="h-16 w-16 mb-4 stroke-[1]" />
                        <p className="font-mono text-sm uppercase tracking-widest">Select a Data Layer</p>
                    </div>
                ) : (
                    <div className="flex flex-col min-h-full">
                        {/* Header */}
                        <div className="px-6 py-5 border-b border-slate-100">
                            <div className="flex items-center gap-2 mb-2 text-xs font-mono text-slate-400 uppercase tracking-widest">
                                <Database className="h-3 w-3" />
                                <span>{selectedLayer.name}</span>
                                {selectedDomain && (
                                    <>
                                        <ChevronRight className="h-3 w-3" />
                                        <Folder className="h-3 w-3" />
                                        <span className="text-indigo-600 font-bold">{selectedDomain.name}</span>
                                    </>
                                )}
                            </div>

                            <h1 className="text-2xl font-bold text-slate-900 tracking-tight">
                                {selectedDomain ? selectedDomain.name : selectedLayer.name}
                            </h1>
                            <p className="text-slate-500 mt-1 text-sm">
                                {selectedDomain ? selectedDomain.description : selectedLayer.description}
                            </p>
                        </div>

                        {/* Content Area */}
                        <div className="p-6">
                            {selectedDomain ? (
                                <DomainDetails domain={selectedDomain} />
                            ) : (
                                <LayerDetails layer={selectedLayer} />
                            )}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

// Sub-component for Layer Details
function LayerDetails({ layer }: { layer: DataLayer }) {
    const status = getStatusConfig(layer.status);

    return (
        <div className="space-y-8">
            {/* Key Metrics Grid */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <MetricCard
                    label="Status"
                    value={layer.status}
                    icon={<Activity className="h-4 w-4" />}
                    statusColor={status.text}
                />
                <MetricCard
                    label="Last Updated"
                    value={formatTimeAgo(layer.lastUpdated) + ' ago'}
                    subValue={new Date(layer.lastUpdated).toLocaleString()}
                    icon={<Clock className="h-4 w-4" />}
                />
                <MetricCard
                    label="Domains"
                    value={String(layer.domains?.length || 0)}
                    icon={<FolderOpen className="h-4 w-4" />}
                />
            </div>

            {/* Quick Actions / Links */}
            {layer.portalUrl && (
                <div className="p-4 bg-slate-50 border border-slate-200 rounded-sm">
                    <h3 className={`${StatusTypos.HEADER} mb-3`}>INFRASTRUCTURE</h3>
                    <a
                        href={layer.portalUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-2 text-sm font-medium text-indigo-600 hover:text-indigo-800 hover:underline"
                    >
                        <ExternalLink className="h-4 w-4" />
                        Open Azure Container
                    </a>
                </div>
            )}
        </div>
    );
}

// Sub-component for Domain Details
function DomainDetails({ domain }: { domain: DataDomain }) {
    const status = getStatusConfig(domain.status);

    return (
        <div className="space-y-8">
            {/* Key Metrics Grid */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <MetricCard
                    label="Status"
                    value={domain.status}
                    icon={<Activity className="h-4 w-4" />}
                    statusColor={status.text}
                />
                <MetricCard
                    label="Last Updated"
                    value={formatTimeAgo(domain.lastUpdated) + ' ago'}
                    subValue={domain.lastUpdated ? new Date(domain.lastUpdated).toLocaleString() : '-'}
                    icon={<Clock className="h-4 w-4" />}
                />
                <MetricCard
                    label="Type"
                    value={domain.type}
                    icon={<FileText className="h-4 w-4" />}
                />
            </div>

            {/* Path Information */}
            <div className="p-4 bg-slate-50 border border-slate-200 rounded-sm font-mono text-sm max-w-full overflow-hidden">
                <div className="flex items-center gap-2 text-slate-400 mb-2 uppercase text-[10px] tracking-widest font-sans font-bold">
                    <Folder className="h-3 w-3" /> Storage Path
                </div>
                <div className="p-2 bg-white border border-slate-200 rounded text-slate-700 break-all select-all">
                    {domain.path || 'No path configured'}
                </div>
                {domain.portalUrl && (
                    <a
                        href={domain.portalUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-2 mt-3 text-xs font-sans font-medium text-indigo-600 hover:text-indigo-800 hover:underline"
                    >
                        <ExternalLink className="h-3.5 w-3.5" />
                        Open in Azure Storage Explorer
                    </a>
                )}
            </div>

            {/* Job Integration */}
            {domain.jobName && (
                <div className="p-4 bg-slate-50 border border-slate-200 rounded-sm">
                    <h3 className={`${StatusTypos.HEADER} mb-3`}>ASSOCIATED JOB</h3>
                    <div className="flex items-center justify-between p-3 bg-white border border-slate-200 rounded-sm">
                        <div className="flex items-center gap-3">
                            <Server className="h-4 w-4 text-slate-400" />
                            <div>
                                <div className="font-bold text-slate-800 text-sm">{domain.jobName}</div>
                                <div className="text-xs text-slate-500 font-mono">{domain.frequency || 'Manual Trigger'}</div>
                            </div>
                        </div>
                        {domain.jobUrl && (
                            <a
                                href={domain.jobUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="p-2 text-slate-400 hover:text-indigo-600 hover:bg-slate-50 rounded transition-colors"
                            >
                                <ExternalLink className="h-4 w-4" />
                            </a>
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}

function MetricCard({ label, value, subValue, icon, statusColor }: { label: string, value: string, subValue?: string, icon: React.ReactNode, statusColor?: string }) {
    return (
        <div className="p-4 border border-slate-200 rounded-sm bg-white">
            <div className="flex items-center justify-between mb-2">
                <span className="text-[10px] font-bold uppercase tracking-widest text-slate-400">{label}</span>
                <span className="text-slate-300">{icon}</span>
            </div>
            <div className="flex items-center gap-2">
                {statusColor && (
                    <div className="w-2 h-2 rounded-full" style={{ backgroundColor: statusColor }} />
                )}
                <div className="text-lg font-bold text-slate-900 truncate" title={value}>
                    {value}
                </div>
            </div>
            {subValue && (
                <div className="text-xs text-slate-500 font-mono mt-0.5 truncate" title={subValue}>
                    {subValue}
                </div>
            )}
        </div>
    );
}
