import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Activity } from 'lucide-react';

export function LiveTradingPage() {
  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title">Live Trading</h1>
        <p className="page-subtitle">
          Monitor order flow and execution health when real-time trading is enabled.
        </p>
      </div>
      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="h-5 w-5" />
            Trading Monitor
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          Live trading views are not enabled in this deployment. Use System Status and Signal
          Monitor for operational monitoring.
        </CardContent>
      </Card>
    </div>
  );
}
