import React from 'react';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { AlertCircle, CheckCircle2 } from 'lucide-react';
import { getSeverityIcon, formatTimestamp } from './SystemStatusHelpers';

import { SystemAlert } from '@/types/strategy';

interface AlertsPanelProps {
  alerts: SystemAlert[];
}

export function AlertsPanel({ alerts }: AlertsPanelProps) {
  const unacknowledgedAlerts = alerts.filter((a) => !a.acknowledged);

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <AlertCircle className="h-5 w-5" />
            Active Alerts
          </CardTitle>
          {unacknowledgedAlerts.length > 0 && (
            <Badge variant="destructive">{unacknowledgedAlerts.length} Active</Badge>
          )}
        </div>
        <CardDescription>System alerts requiring attention</CardDescription>
      </CardHeader>
      <CardContent className="flex-1 overflow-auto">
        {unacknowledgedAlerts.length > 0 ? (
          <div className="space-y-3">
            {unacknowledgedAlerts.map((alert, idx) => (
              <div key={idx} className="flex items-start gap-3 p-3 border rounded-lg">
                <div className="mt-0.5">{getSeverityIcon(alert.severity)}</div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="text-sm font-semibold">{alert.component}</span>
                    <span className="text-xs text-muted-foreground ml-auto">
                      {formatTimestamp(alert.timestamp)}
                    </span>
                  </div>
                  <p className="text-sm text-muted-foreground line-clamp-2">{alert.message}</p>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="flex flex-col items-center justify-center h-32 text-muted-foreground">
            <CheckCircle2 className="h-8 w-8 mb-2 opacity-20" />
            <p className="text-sm">No active alerts</p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
