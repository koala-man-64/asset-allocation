import React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { toast } from 'sonner';
import { AlertCircle, CheckCircle2, Clock, Check, BellOff, XCircle } from 'lucide-react';
import { getSeverityIcon, formatTimestamp } from './SystemStatusHelpers';
import { SystemAlert } from '@/types/strategy';
import { ApiError, backtestApi } from '@/services/backtestApi';

interface AlertHistoryProps {
  alerts: SystemAlert[];
}

export function AlertHistory({ alerts }: AlertHistoryProps) {
  const queryClient = useQueryClient();

  const mutate = async (action: string, fn: () => Promise<unknown>) => {
    try {
      await fn();
      toast.success(action);
      void queryClient.invalidateQueries({ queryKey: ['systemHealth'] });
    } catch (err: unknown) {
      const message =
        err instanceof ApiError
          ? `${err.status}: ${err.message}`
          : err instanceof Error
            ? err.message
            : String(err);
      toast.error(`${action} failed: ${message}`);
    }
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <AlertCircle className="h-5 w-5" />
          Alert History
        </CardTitle>
        <CardDescription>Complete history of system alerts and notifications</CardDescription>
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
                  <Badge
                    variant={
                      alert.severity === 'critical' || alert.severity === 'error'
                        ? 'destructive'
                        : alert.severity === 'warning'
                          ? 'secondary'
                          : 'outline'
                    }
                  >
                    {alert.severity.toUpperCase()}
                  </Badge>
                  <span className="text-sm text-muted-foreground">{alert.component}</span>
                  {alert.acknowledged && (
                    <Badge variant="outline">
                      <CheckCircle2 />
                      Acknowledged
                    </Badge>
                  )}
                  {alert.snoozedUntil && Date.parse(alert.snoozedUntil) > Date.now() && (
                    <Badge variant="outline">
                      <BellOff />
                      Snoozed
                    </Badge>
                  )}
                  {alert.resolvedAt && (
                    <Badge variant="outline">
                      <Check />
                      Resolved
                    </Badge>
                  )}
                  <span className="text-sm text-muted-foreground ml-auto">
                    {formatTimestamp(alert.timestamp)}
                  </span>
                </div>
                <div className="flex items-start justify-between gap-3">
                  <p className="text-base">{alert.message}</p>
                  <div className="flex items-center gap-2 shrink-0">
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8 px-2 gap-1"
                      disabled={!alert.id || alert.acknowledged}
                      onClick={() =>
                        alert.id
                          ? mutate('Acknowledged', () => backtestApi.acknowledgeAlert(alert.id!))
                          : Promise.resolve()
                      }
                    >
                      <CheckCircle2 className="h-4 w-4" />
                      Ack
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8 px-2 gap-1"
                      disabled={!alert.id}
                      onClick={() =>
                        alert.id
                          ? mutate('Snoozed (30m)', () =>
                              backtestApi.snoozeAlert(alert.id!, { minutes: 30 })
                            )
                          : Promise.resolve()
                      }
                    >
                      <Clock className="h-4 w-4" />
                      Snooze
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-8 px-2 gap-1"
                      disabled={!alert.id || Boolean(alert.resolvedAt)}
                      onClick={() =>
                        alert.id
                          ? mutate('Resolved', () => backtestApi.resolveAlert(alert.id!))
                          : Promise.resolve()
                      }
                    >
                      <XCircle className="h-4 w-4" />
                      Resolve
                    </Button>
                  </div>
                </div>
              </div>
            </div>
          ))}
          {alerts.length === 0 && (
            <div className="text-center text-muted-foreground py-8">No alert history found.</div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
