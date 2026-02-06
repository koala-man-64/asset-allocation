import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Activity } from 'lucide-react';

export function LiveTradingPage() {
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="h-5 w-5" />
            Live Trading
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
