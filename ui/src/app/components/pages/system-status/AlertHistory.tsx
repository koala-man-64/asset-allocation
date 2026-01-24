import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { AlertCircle, CheckCircle2 } from 'lucide-react';
import { getSeverityIcon, formatTimestamp } from './SystemStatusHelpers';
import { SystemAlert } from '@/types/strategy';

interface AlertHistoryProps {
    alerts: SystemAlert[];
}

export function AlertHistory({ alerts }: AlertHistoryProps) {
    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <AlertCircle className="h-5 w-5" />
                    Alert History
                </CardTitle>
                <CardDescription>
                    Complete history of system alerts and notifications
                </CardDescription>
            </CardHeader>
            <CardContent>
                <div className="space-y-2">
                    {alerts.map((alert, idx) => (
                        <div
                            key={idx}
                            className={`flex items-start gap-3 p-3 border rounded-lg ${alert.acknowledged ? 'opacity-50' : ''}`}
                        >
                            <div className="mt-0.5">{getSeverityIcon(alert.severity)}</div>
                            <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 mb-1 flex-wrap">
                                    <Badge variant={
                                        alert.severity === 'critical' || alert.severity === 'error'
                                            ? 'destructive'
                                            : alert.severity === 'warning'
                                                ? 'secondary'
                                                : 'outline'
                                    }>
                                        {alert.severity.toUpperCase()}
                                    </Badge>
                                    <span className="text-sm text-muted-foreground">{alert.component}</span>
                                    {alert.acknowledged && (
                                        <Badge variant="outline" className="text-sm">
                                            <CheckCircle2 className="h-3.5 w-3.5 mr-1" />
                                            Acknowledged
                                        </Badge>
                                    )}
                                    <span className="text-sm text-muted-foreground ml-auto">
                                        {formatTimestamp(alert.timestamp)}
                                    </span>
                                </div>
                                <p className="text-base">{alert.message}</p>
                            </div>
                        </div>
                    ))}
                    {alerts.length === 0 && (
                        <div className="text-center text-muted-foreground py-8">
                            No alert history found.
                        </div>
                    )}
                </div>
            </CardContent>
        </Card>
    );
}
